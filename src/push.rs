//! Render a batch of [`ScannedDoc`]s as a `_bulk` NDJSON body and push it to
//! the target, with the retry/fail-fast policy in [`crate::retry`].
//!
//! The `_bulk` action shape — `index`, `version_type: external`, `version =`
//! the source document's `_seq_no` — is adapted from `wal_tap.rs`'s
//! `render_action`: it is what makes a full rerun after an interruption
//! always safe. The target keeps, for each `_id`, whichever write carries the
//! highest external version; since the source's `_seq_no` only increases, a
//! rerun that reads the same document again sends the same version and either
//! updates nothing (already-current) or is a genuine no-op version conflict —
//! never a duplicate, never a regression. See the README's Attribution
//! section for a link to the original.

use anyhow::Result;
use serde_json::{json, Value};

use crate::http::EsClient;
use crate::retry::{classify_status, with_retry, Outcome, Retryability};
use crate::scan::ScannedDoc;

/// What became of one push.
#[derive(Debug, Default)]
pub struct PushOutcome {
    /// Actions the target accepted (new write or a no-op refresh of the
    /// current version).
    pub shipped: u64,
    /// `version_conflict_engine_exception`: the target already holds this-or-
    /// higher a version for the `_id`. Expected and benign on a rerun.
    pub conflicts: u64,
    /// Rejected for any other reason (mapping error, etc). Not retried —
    /// retrying a deterministic per-document rejection would just repeat it;
    /// it is reported instead so the operator can fix the document or the
    /// target mapping and rerun (safe — see module docs).
    pub other_rejections: u64,
    /// Error `type` of the first non-conflict rejection, for the summary.
    pub last_rejection_kind: Option<String>,
}

fn render_bulk_body(index: &str, docs: &[ScannedDoc]) -> String {
    let mut body = String::new();
    for doc in docs {
        let meta = json!({ "index": {
            "_index": index,
            "_id": doc.id,
            "version": doc.seq_no,
            "version_type": "external",
        }});
        body.push_str(&meta.to_string());
        body.push('\n');
        body.push_str(&doc.source.to_string());
        body.push('\n');
    }
    body
}

/// Push one batch. The whole `_bulk` request is retried on a transient
/// failure and reported immediately on a permanent one (see
/// [`crate::retry`]); once a 2xx response comes back, per-item outcomes are
/// tallied but never retried — see [`PushOutcome::other_rejections`].
pub async fn push_batch(
    target: &EsClient,
    index: &str,
    docs: &[ScannedDoc],
    max_retries: u32,
) -> Result<PushOutcome> {
    if docs.is_empty() {
        return Ok(PushOutcome::default());
    }
    let body = render_bulk_body(index, docs);

    let resp_body = with_retry("push batch", max_retries, || {
        let body = body.clone();
        async move {
            let resp = match target
                .post("_bulk")
                .header("Content-Type", "application/x-ndjson")
                .body(body)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => return Outcome::Retryable(format!("transport: {e}")),
            };
            let status = resp.status();
            match classify_status(status) {
                Retryability::Success => match resp.json::<Value>().await {
                    Ok(v) => Outcome::Done(v),
                    Err(e) => Outcome::Retryable(format!("unparseable _bulk response: {e}")),
                },
                Retryability::Retryable => Outcome::Retryable(format!("HTTP {status}")),
                Retryability::Permanent => {
                    let snippet = resp.text().await.unwrap_or_default();
                    Outcome::Permanent(format!("HTTP {status}: {snippet}"))
                }
            }
        }
    })
    .await?;

    let mut outcome = PushOutcome::default();

    // `errors: false` (or absent) means every action landed — ES-compatible
    // targets omit nothing worth walking in that case.
    if resp_body.get("errors").and_then(Value::as_bool) != Some(true) {
        outcome.shipped = docs.len() as u64;
        return Ok(outcome);
    }

    let items = resp_body
        .get("items")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();

    for item in items {
        let action = item.as_object().and_then(|o| o.values().next());
        let error = action.and_then(|a| a.get("error"));
        let Some(error) = error else {
            outcome.shipped += 1;
            continue;
        };
        let kind = error.get("type").and_then(Value::as_str).unwrap_or("unknown");
        if kind == "version_conflict_engine_exception" {
            outcome.conflicts += 1;
        } else {
            outcome.other_rejections += 1;
            outcome.last_rejection_kind = Some(kind.to_string());
        }
    }

    Ok(outcome)
}
