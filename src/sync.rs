//! Arm the source's `wal_tap` for incremental sync once the seed is done.
//!
//! `PUT /_xerj/wal_tap` is superuser-only on xerj (see `wal_tap_api.rs`'s
//! module docs — it names an arbitrary `target_url` and attaches an
//! arbitrary `Authorization` header, so it is treated the same as
//! snapshot/restore). `--source-auth` is reused as that admin credential,
//! exactly as the task this tool was built for specifies: the same
//! credential that read the source index is the one allowed to reconfigure
//! it.

use anyhow::Result;
use serde_json::json;

use crate::http::EsClient;
use crate::retry::{classify_status, with_retry, Outcome, Retryability};

pub async fn enable_wal_tap(
    source: &EsClient,
    index: &str,
    target_url: &str,
    target_auth: &str,
    max_retries: u32,
) -> Result<()> {
    let body = json!({
        "enabled": true,
        "indices": [index],
        "target_url": target_url,
        "target_auth": target_auth,
    });

    with_retry("enable wal_tap", max_retries, || {
        let body = body.clone();
        async move {
            let resp = match source.put("_xerj/wal_tap").json(&body).send().await {
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
