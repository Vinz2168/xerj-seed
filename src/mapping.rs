//! Copy the source index's mapping/settings to the target before any
//! documents are written, so the target ends up with the source's real
//! schema instead of whatever dynamic-mapping inference guesses from the
//! first batch of `_bulk` documents.
//!
//! Idempotent by construction, same as the rest of this tool: if the target
//! index already exists, this step is a no-op — it never touches an
//! existing index's mapping, whatever that mapping currently is. A rerun
//! after the target index has been created is safe and skips straight to
//! the document push.

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use crate::http::EsClient;
use crate::retry::{classify_status, with_retry, Outcome, Retryability};

/// `index.*` settings keys an ES-compatible engine assigns at creation time
/// and does not expect back on the way in — carrying them over from a `GET`
/// response into a `PUT` body would be asking the target to take on the
/// source's own identity, not a description of desired settings.
const NON_PORTABLE_SETTINGS_KEYS: &[&str] = &[
    "uuid",
    "version",
    "provided_name",
    "creation_date",
    "creation_date_string",
    "history",
];

/// `GET /{index}` on the source: its mappings and settings, with the
/// non-portable settings keys stripped. `Ok(None)` means the source index
/// does not exist.
async fn fetch_source_schema(
    source: &EsClient,
    index: &str,
    max_retries: u32,
) -> Result<Option<(Value, Value)>> {
    let resp_body = with_retry("fetch source index mapping/settings", max_retries, || async {
        let resp = match source.get(index).send().await {
            Ok(r) => r,
            Err(e) => return Outcome::Retryable(format!("transport: {e}")),
        };
        if resp.status().as_u16() == 404 {
            return Outcome::Done(None);
        }
        let status = resp.status();
        match classify_status(status) {
            Retryability::Success => match resp.json::<Value>().await {
                Ok(v) => Outcome::Done(Some(v)),
                Err(e) => Outcome::Retryable(format!("unparseable GET /{index} response: {e}")),
            },
            Retryability::Retryable => Outcome::Retryable(format!("HTTP {status}")),
            Retryability::Permanent => {
                let snippet = resp.text().await.unwrap_or_default();
                Outcome::Permanent(format!("HTTP {status}: {snippet}"))
            }
        }
    })
    .await?;

    let Some(body) = resp_body else {
        return Ok(None);
    };

    let index_body = body
        .get(index)
        .with_context(|| format!("GET /{index} response on the source carried no {index:?} key: {body}"))?;

    let mappings = index_body
        .get("mappings")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let mut settings = index_body
        .get("settings")
        .and_then(|s| s.get("index"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    if let Some(obj) = settings.as_object_mut() {
        for key in NON_PORTABLE_SETTINGS_KEYS {
            obj.remove(*key);
        }
    }

    Ok(Some((mappings, json!({ "index": settings }))))
}

/// Does the target already have this index?
async fn target_index_exists(target: &EsClient, index: &str, max_retries: u32) -> Result<bool> {
    with_retry("check target index existence", max_retries, || async {
        let resp = match target.get(index).send().await {
            Ok(r) => r,
            Err(e) => return Outcome::Retryable(format!("transport: {e}")),
        };
        if resp.status().as_u16() == 404 {
            return Outcome::Done(false);
        }
        let status = resp.status();
        match classify_status(status) {
            Retryability::Success => Outcome::Done(true),
            Retryability::Retryable => Outcome::Retryable(format!("HTTP {status}")),
            Retryability::Permanent => {
                let snippet = resp.text().await.unwrap_or_default();
                Outcome::Permanent(format!("HTTP {status}: {snippet}"))
            }
        }
    })
    .await
}

/// `PUT /{index}` on the target with `mappings`/`settings`. Caller is
/// responsible for having already confirmed the index does not exist.
async fn create_target_index(
    target: &EsClient,
    index: &str,
    mappings: Value,
    settings: Value,
    max_retries: u32,
) -> Result<()> {
    let body = json!({ "mappings": mappings, "settings": settings });
    with_retry("create target index", max_retries, || {
        let body = body.clone();
        async move {
            let resp = match target.put(index).json(&body).send().await {
                Ok(r) => r,
                Err(e) => return Outcome::Retryable(format!("transport: {e}")),
            };
            let status = resp.status();
            match classify_status(status) {
                Retryability::Success => Outcome::Done(()),
                Retryability::Retryable => Outcome::Retryable(format!("HTTP {status}")),
                Retryability::Permanent => {
                    let snippet = resp.text().await.unwrap_or_default();
                    Outcome::Permanent(format!("HTTP {status}: {snippet}"))
                }
            }
        }
    })
    .await
}

/// Fetch the source index's mapping/settings and, if the target doesn't
/// already have this index, create it there first. A no-op when the target
/// index already exists — see the module docs.
///
/// Errors out (stopping the whole run) when the source index doesn't exist
/// at all: that's a configuration mistake worth catching before an empty
/// scroll scan makes it look like the index was just empty.
pub async fn import_mapping_if_needed(
    source: &EsClient,
    target: &EsClient,
    index: &str,
    max_retries: u32,
) -> Result<()> {
    let Some((mappings, settings)) = fetch_source_schema(source, index, max_retries).await? else {
        bail!(
            "source index {index:?} does not exist on {} — nothing to seed",
            source.redacted_base_url()
        );
    };

    if target_index_exists(target, index, max_retries).await? {
        eprintln!(
            "xerj-seed: target index {index:?} already exists — leaving its mapping/settings \
             untouched"
        );
        return Ok(());
    }

    eprintln!("xerj-seed: creating {index:?} on the target with the source's mapping/settings...");
    create_target_index(target, index, mappings, settings, max_retries).await?;
    eprintln!("xerj-seed: target index {index:?} created.");
    Ok(())
}
