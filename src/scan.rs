//! Scroll-based scan of the source index: open a scroll, page through it,
//! and clear it when done.
//!
//! **Why scroll and not Point-in-Time.** The first design here used PIT +
//! `search_after`, which works against xerj and (presumably) real
//! Elasticsearch, but not real OpenSearch: OpenSearch's actual PIT API is
//! shaped differently (`POST /{index}/_search/point_in_time`, not
//! `POST /{index}/_pit`; a different close body). `_scroll`, by contrast,
//! predates that fork — `POST /{index}/_search?scroll=1m`, `POST
//! /_search/scroll`, `DELETE /_search/scroll` — and is live-verified
//! identical on xerj and on a real OpenSearch 3.7.0 node, including
//! `seq_no_primary_term: true` surfacing a real `_seq_no` per hit under
//! scroll exactly as it does under a plain search. Scroll is the more
//! restrictive primitive (no `search_after`-style resumable cursor, no
//! concurrent scroll batches), but this tool only ever runs one linear scan
//! at a time, so none of that is missed — and portability across engines
//! matters more here than the features scroll doesn't have.
//!
//! The *version* pushed to the target is still the real `_seq_no` — see
//! `wal_tap.rs`'s reasoning in the README's Attribution section — read the
//! same way as before: `seq_no_primary_term: true` in the request, `_seq_no`
//! as an ordinary field on each hit.

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

/// Pull `hits.hits` out of a `_search`/scroll response into [`ScannedDoc`]s.
fn extract_docs(resp_body: &Value) -> Result<Vec<ScannedDoc>> {
    let hits = resp_body
        .get("hits")
        .and_then(|h| h.get("hits"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut docs = Vec::with_capacity(hits.len());
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
        docs.push(ScannedDoc {
            id,
            seq_no,
            source: source_doc,
        });
    }
    Ok(docs)
}

/// The `_scroll_id` a request carried, if any — ES/OpenSearch both include
/// it on every scroll response, and recommend always using the latest one
/// for the next continuation call rather than assuming it never changes.
fn extract_scroll_id(resp_body: &Value) -> Result<String> {
    resp_body
        .get("_scroll_id")
        .and_then(Value::as_str)
        .map(str::to_string)
        .with_context(|| format!("response carried no _scroll_id: {resp_body}"))
}

/// `POST /{index}/_search?scroll={keep_alive}` — opens the scroll and
/// returns its id plus the first page of documents in one call (unlike PIT,
/// scroll doesn't separate "open" from "first read").
pub async fn open_scroll(
    source: &EsClient,
    index: &str,
    keep_alive: &str,
    batch_size: usize,
    max_retries: u32,
) -> Result<(String, Vec<ScannedDoc>)> {
    let path = format!("{index}/_search?scroll={keep_alive}");
    let body = json!({
        "size": batch_size,
        "seq_no_primary_term": true,
        "query": { "match_all": {} },
    });

    let resp_body = with_retry("open scroll", max_retries, || {
        let body = body.clone();
        let path = path.clone();
        async move {
            let resp = match source.post(&path).json(&body).send().await {
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

    let scroll_id = extract_scroll_id(&resp_body)?;
    let docs = extract_docs(&resp_body)?;
    Ok((scroll_id, docs))
}

/// `POST /_search/scroll` — the next page. Returns the (possibly updated)
/// scroll id and the documents found; an empty result is the normal
/// end-of-scan signal, same as the rest of this tool's paging.
pub async fn next_page(
    source: &EsClient,
    scroll_id: &str,
    keep_alive: &str,
    max_retries: u32,
) -> Result<(String, Vec<ScannedDoc>)> {
    let body = json!({ "scroll": keep_alive, "scroll_id": scroll_id });

    let resp_body = with_retry("scroll next page", max_retries, || {
        let body = body.clone();
        async move {
            let resp = match source.post("_search/scroll").json(&body).send().await {
                Ok(r) => r,
                Err(e) => return Outcome::Retryable(format!("transport: {e}")),
            };
            let status = resp.status();
            match classify_status(status) {
                Retryability::Success => match resp.json::<Value>().await {
                    Ok(v) => Outcome::Done(v),
                    Err(e) => Outcome::Retryable(format!("unparseable scroll response: {e}")),
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

    let next_scroll_id = extract_scroll_id(&resp_body)?;
    let docs = extract_docs(&resp_body)?;
    Ok((next_scroll_id, docs))
}

/// Clear a scroll, best-effort. A failure here does not affect data already
/// scanned/pushed, so it is logged and swallowed rather than surfaced as a
/// run failure — matching wal_tap.rs's own cleanup-block posture.
pub async fn clear_scroll(source: &EsClient, scroll_id: &str) {
    let resp = source
        .delete("_search/scroll")
        .json(&json!({ "scroll_id": [scroll_id] }))
        .send()
        .await;
    match resp {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => eprintln!(
            "warning: clearing the scroll returned HTTP {} (it will expire on its own after \
             keep_alive)",
            r.status()
        ),
        Err(e) => eprintln!(
            "warning: could not clear the scroll ({e}) — it will expire on its own after \
             keep_alive"
        ),
    }
}
