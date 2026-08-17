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
//! scroll exactly as it does under a plain search.
//!
//! Scroll is a snapshot taken at the moment it opens: a write made on the
//! source *after* the scroll opens is not guaranteed to appear in the
//! results, on any engine — the same consistency model PIT would have
//! given, and the reason the mapping-import step and this scan both run
//! before `--enable-sync-after` arms `wal_tap`, not after.
//!
//! **A real xerj bug this module works around.** `seq_no_primary_term:
//! true` on the scroll-opening `_search?scroll=...` request is honored for
//! that first page, but xerj's `_search/scroll` continuation handler drops
//! it — every page after the first comes back with no `_seq_no` on a xerj
//! source, live-verified up to 5 pages. Real Elasticsearch and OpenSearch
//! both persist the flag for the scroll's lifetime, as documented (checked
//! live against OpenSearch 3.7.0, 4 pages, `_seq_no` present throughout);
//! only xerj drops it. [`backfill_missing_seq_no`] papers over this with
//! one `_mget?seq_no_primary_term=true` call per page that needs it — not
//! per document — rather than failing the whole scan. Filed as a bug
//! against xerj-org/xerj; see the README's Testing section for the issue
//! link. This workaround is harmless overhead everywhere else, since it
//! only fires when `_seq_no` is actually missing from a page.
//!
//! **Why the backfill also replaces `_source`, not only `_seq_no`.** An
//! upstream attempt at the same bug (xerj-org/xerj#431, not merged — see
//! the README) revealed a sharper failure mode than a missing field:
//! resolving `_seq_no` from the live version map while serving `_source`
//! from a point-in-time snapshot can pair a document's *old* body with its
//! *new* sequence number, if a write lands in between. Fed back as
//! `version_type: external` on the target — exactly what this tool does —
//! that stale body is accepted as the highest-versioned write, silently
//! discarding whatever the real, newer document was. [`backfill_missing_seq_no`]
//! avoids constructing that pairing itself: when a page is missing
//! `_seq_no`, it takes `_source` **from the same `_mget` response** the
//! `_seq_no` backfill comes from, rather than keeping the scroll page's
//! (potentially now-stale) `_source` next to a freshly-fetched `_seq_no`.
//! A single-document `_mget` lookup is a direct read, not a
//! gather-then-resolve-later pass over a list, so the two values it
//! returns come from the same read — the same property real Elasticsearch
//! gets from a pinned segment reader, applied here per-document at the
//! client instead of per-scroll at the engine.
//!
//! The *version* pushed to the target is still the real `_seq_no` — see
//! `wal_tap.rs`'s reasoning in the README's Attribution section.

use std::collections::HashMap;

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

/// A hit as read straight off a `_search`/scroll response, before the
/// `_seq_no` fallback. `seq_no` is `None` when the page didn't carry it —
/// see the module docs for why that's a real, expected case on a xerj
/// source, not necessarily a malformed response.
struct RawHit {
    id: String,
    seq_no: Option<u64>,
    source: Value,
}

/// Pull `hits.hits` out of a `_search`/scroll response into [`RawHit`]s.
fn extract_hits(resp_body: &Value) -> Result<Vec<RawHit>> {
    let hits = resp_body
        .get("hits")
        .and_then(|h| h.get("hits"))
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut out = Vec::with_capacity(hits.len());
    for hit in hits {
        let id = hit
            .get("_id")
            .and_then(Value::as_str)
            .context("hit carried no _id")?
            .to_string();
        let seq_no = hit.get("_seq_no").and_then(Value::as_u64);
        let source_doc = hit.get("_source").cloned().unwrap_or_else(|| json!({}));
        out.push(RawHit {
            id,
            seq_no,
            source: source_doc,
        });
    }
    Ok(out)
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

/// A backfilled doc's `_seq_no` and `_source`, read together from the same
/// `_mget` response entry — see the module docs on why both, not just the
/// sequence number, come from there.
struct Backfilled {
    seq_no: u64,
    source: Value,
}

/// Resolve every [`RawHit`] to a [`ScannedDoc`], backfilling `_seq_no`
/// (**and** `_source`, together) via `_mget?seq_no_primary_term=true` for
/// any hit that came back without a `_seq_no` — see the module docs' "real
/// xerj bug" section. One `_mget` call per page that needs it, carrying
/// only the ids that are actually missing, not the whole page.
async fn backfill_missing_seq_no(
    source: &EsClient,
    index: &str,
    hits: Vec<RawHit>,
    max_retries: u32,
) -> Result<Vec<ScannedDoc>> {
    let missing_ids: Vec<&str> = hits
        .iter()
        .filter(|h| h.seq_no.is_none())
        .map(|h| h.id.as_str())
        .collect();

    let mut backfilled: HashMap<String, Backfilled> = HashMap::new();
    if !missing_ids.is_empty() {
        let path = format!("{index}/_mget?seq_no_primary_term=true");
        let body = json!({ "ids": missing_ids });
        let resp_body = with_retry("backfill _seq_no via _mget", max_retries, || {
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
                        Err(e) => Outcome::Retryable(format!("unparseable _mget response: {e}")),
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

        for doc in resp_body
            .get("docs")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            let (Some(id), Some(seq_no)) = (
                doc.get("_id").and_then(Value::as_str),
                doc.get("_seq_no").and_then(Value::as_u64),
            ) else {
                continue;
            };
            // The whole point: this _source is read in the same _mget
            // response as seq_no, not carried over from the scroll page —
            // see the module docs.
            let doc_source = doc.get("_source").cloned().unwrap_or_else(|| json!({}));
            backfilled.insert(
                id.to_string(),
                Backfilled {
                    seq_no,
                    source: doc_source,
                },
            );
        }
    }

    hits.into_iter()
        .map(|h| match h.seq_no {
            Some(seq_no) => Ok(ScannedDoc {
                id: h.id,
                seq_no,
                source: h.source,
            }),
            None => {
                let b = backfilled.get(&h.id).with_context(|| {
                    format!(
                        "hit {:?} carried no _seq_no and the _mget backfill didn't resolve one \
                         either",
                        h.id
                    )
                })?;
                Ok(ScannedDoc {
                    id: h.id,
                    seq_no: b.seq_no,
                    source: b.source.clone(),
                })
            }
        })
        .collect()
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
    let hits = extract_hits(&resp_body)?;
    let docs = backfill_missing_seq_no(source, index, hits, max_retries).await?;
    Ok((scroll_id, docs))
}

/// `POST /_search/scroll` — the next page. Returns the (possibly updated)
/// scroll id and the documents found; an empty result is the normal
/// end-of-scan signal, same as the rest of this tool's paging.
pub async fn next_page(
    source: &EsClient,
    index: &str,
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
    let hits = extract_hits(&resp_body)?;
    let docs = backfill_missing_seq_no(source, index, hits, max_retries).await?;
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
