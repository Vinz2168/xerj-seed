//! Point-in-Time scan of the source index: open a PIT, page through it with
//! `search_after`, and close it when done.
//!
//! **Pagination key vs. version key — these are two different things, and
//! xerj's `_search` only gives us one of them as a sort key.** The plan
//! going in was to sort on `_seq_no` directly, matching `wal_tap.rs`'s use
//! of `_seq_no` as the `_bulk` version. In practice xerj's `_search`
//! endpoint does not resolve `_seq_no` (or any other dotted meta-field) as a
//! *sort* key — sorting on it comes back with every hit's `sort` value as
//! `null`, silently, so `search_after` would never advance past the first
//! page. What xerj *does* support is `sort: [{"_doc": "asc"}]`, which
//! resolves to `_id` order and is a perfectly valid, unique, total order for
//! keyset pagination — correctness of the scan (every document visited
//! exactly once) does not require the pagination order to match write
//! order, only that it be total and stable, which `_id` order is.
//!
//! The *version* pushed to the target still has to be the real `_seq_no`,
//! for the reason `wal_tap.rs` uses it: a rerun after an interruption must
//! converge rather than duplicate or regress. That value **is** available,
//! just not as a sort key — requesting `"seq_no_primary_term": true` (the
//! same ES request flag `GET .../_doc/_id?seq_no_primary_term=true` uses)
//! makes every hit carry its real `_seq_no` as an ordinary response field,
//! resolved server-side from the engine's version map rather than a
//! per-page positional counter. So: sort/search_after on `_doc` (pagination),
//! read `_seq_no` per hit (version). See the README's Attribution section
//! for the wal_tap.rs source this version scheme is adapted from.

use anyhow::{Context, Result};
use serde_json::{json, Value};

use crate::http::EsClient;
use crate::retry::{classify_status, with_retry, Outcome, Retryability};

/// One document read from the source, ready to be rendered as a `_bulk`
/// action. `seq_no` becomes the `version` on the target — see module docs.
pub struct ScannedDoc {
    pub id: String,
    pub seq_no: u64,
    pub source: Value,
}

/// Open a PIT on `index` and return its id.
pub async fn open_pit(
    source: &EsClient,
    index: &str,
    keep_alive: &str,
    max_retries: u32,
) -> Result<String> {
    let path = format!("{index}/_pit?keep_alive={keep_alive}");
    let body = with_retry("open PIT", max_retries, || async {
        let resp = match source.post(&path).send().await {
            Ok(r) => r,
            Err(e) => return Outcome::Retryable(format!("transport: {e}")),
        };
        let status = resp.status();
        match classify_status(status) {
            Retryability::Success => match resp.json::<Value>().await {
                Ok(v) => Outcome::Done(v),
                Err(e) => Outcome::Retryable(format!("unparseable PIT response: {e}")),
            },
            Retryability::Retryable => Outcome::Retryable(format!("HTTP {status}")),
            Retryability::Permanent => {
                let snippet = resp.text().await.unwrap_or_default();
                Outcome::Permanent(format!("HTTP {status}: {snippet}"))
            }
        }
    })
    .await?;

    body.get("id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .with_context(|| format!("PIT response for {index:?} carried no \"id\": {body}"))
}

/// Close a PIT, best-effort. A failure here does not affect data already
/// scanned/pushed, so it is logged and swallowed rather than surfaced as a
/// run failure — matching wal_tap.rs's own cleanup-block posture.
pub async fn close_pit(source: &EsClient, pit_id: &str) {
    let resp = source
        .delete("_pit")
        .json(&json!({ "id": pit_id }))
        .send()
        .await;
    match resp {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => eprintln!(
            "warning: closing the PIT returned HTTP {} (it will expire on its own after \
             keep_alive)",
            r.status()
        ),
        Err(e) => eprintln!(
            "warning: could not close the PIT ({e}) — it will expire on its own after \
             keep_alive"
        ),
    }
}

/// Fetch the next page after `search_after` (`None` for the first page).
///
/// Returns the documents found (empty means the scan is complete) and the
/// `search_after` cursor (the last hit's `_id`) to pass on the next call.
pub async fn next_page(
    source: &EsClient,
    pit_id: &str,
    keep_alive: &str,
    batch_size: usize,
    search_after: Option<&str>,
    max_retries: u32,
) -> Result<(Vec<ScannedDoc>, Option<String>)> {
    let mut body = json!({
        "size": batch_size,
        "seq_no_primary_term": true,
        "query": { "match_all": {} },
        "pit": { "id": pit_id, "keep_alive": keep_alive },
        "sort": [{ "_doc": "asc" }],
    });
    if let Some(after) = search_after {
        body["search_after"] = json!([after]);
    }

    let resp_body = with_retry("scan page", max_retries, || {
        let body = body.clone();
        async move {
            let resp = match source.post("_search").json(&body).send().await {
                Ok(r) => r,
                Err(e) => return Outcome::Retryable(format!("transport: {e}")),
            };
            let status = resp.status();
            match classify_status(status) {
                Retryability::Success => match resp.json::<Value>().await {
                    Ok(v) => Outcome::Done(v),
                    Err(e) => Outcome::Retryable(format!("unparseable _search response: {e}")),
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

    let hits = resp_body
        .get("hits")
        .and_then(|h| h.get("hits"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut docs = Vec::with_capacity(hits.len());
    let mut last_id: Option<String> = search_after.map(str::to_string);
    for hit in hits {
        let id = hit
            .get("_id")
            .and_then(Value::as_str)
            .context("hit carried no _id")?
            .to_string();
        let seq_no = hit
            .get("_seq_no")
            .and_then(Value::as_u64)
            .with_context(|| {
                format!(
                    "hit {id:?} carried no usable _seq_no (expected because \
                     seq_no_primary_term=true was requested)"
                )
            })?;
        let source_doc = hit.get("_source").cloned().unwrap_or_else(|| json!({}));
        last_id = Some(id.clone());
        docs.push(ScannedDoc {
            id,
            seq_no,
            source: source_doc,
        });
    }

    // An empty `docs` here is the normal end-of-scan signal (no more hits
    // past `search_after`) — the caller stops paging on it, not this
    // function.
    Ok((docs, last_id))
}
