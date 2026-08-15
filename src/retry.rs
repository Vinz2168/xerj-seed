//! Retry/fail-fast policy for outbound HTTP calls.
//!
//! Adapted from the classification `xerj-org/xerj`'s
//! `engine/crates/xerj-engine/src/wal_tap.rs` applies to `_bulk` responses:
//! a transport error or a 5xx/429/408 status is transient, so it is retried
//! with capped exponential backoff and jitter; any other 4xx is the target
//! telling us the *request itself* is wrong (bad auth, bad body, unknown
//! index) and will fail identically forever, so it is reported immediately
//! instead of being retried. wal_tap is a long-running daemon and can afford
//! to hold a cursor and keep backing off indefinitely; this is a one-shot
//! CLI, so [`with_retry`] additionally gives up after `max_retries`
//! consecutive transient failures and returns an error — a rerun from
//! scratch is always safe (see the README), so there is nothing gained by
//! blocking forever on a target that may never come back.

use std::time::Duration;

use anyhow::{anyhow, Result};

/// What a completed HTTP attempt tells the caller to do next.
pub enum Outcome<T> {
    /// The attempt succeeded; here is the value.
    Done(T),
    /// Transient: worth retrying (transport error, 5xx, 429, 408).
    Retryable(String),
    /// Permanent: retrying would fail identically (any other 4xx).
    Permanent(String),
}

/// Classify an HTTP status the way wal_tap.rs classifies a `_bulk` response's
/// whole-request failure: 429 and 408 are transient despite being 4xx (rate
/// limiting and request-timeout are the target asking to be retried, not
/// rejecting the request), every other 4xx is permanent, and 5xx is
/// transient.
pub fn classify_status(status: reqwest::StatusCode) -> Retryability {
    if status.is_success() {
        Retryability::Success
    } else if status.as_u16() == 429 || status.as_u16() == 408 || status.is_server_error() {
        Retryability::Retryable
    } else {
        Retryability::Permanent
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Retryability {
    Success,
    Retryable,
    Permanent,
}

/// Capped exponential backoff with jitter, same shape as
/// `WalTap::arm_backoff`: base delay doubles per failure up to a cap, then a
/// deterministic-free jitter in [50%, 100%] spreads out retries against a
/// recovering target.
fn backoff_delay(attempt: u32, base: Duration, cap: Duration) -> Duration {
    let shift = attempt.saturating_sub(1).min(20);
    let uncapped = base.saturating_mul(1u32 << shift);
    let delay = uncapped.min(cap);
    // Jitter without pulling in an RNG dependency: mix in wall-clock nanos.
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let jitter_pct = 50 + (nanos % 50);
    delay.mul_f64(jitter_pct as f64 / 100.0)
}

/// Run `attempt` up to `max_retries` times, backing off between transient
/// failures and returning immediately on a permanent one or on success.
///
/// `attempt` returns an [`Outcome`] rather than a `Result` so the caller can
/// distinguish "worth retrying" from "will never work" — see the module docs.
pub async fn with_retry<T, F, Fut>(
    label: &str,
    max_retries: u32,
    mut attempt: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Outcome<T>>,
{
    let base = Duration::from_millis(250);
    let cap = Duration::from_secs(30);
    let mut tries = 0u32;
    loop {
        tries += 1;
        match attempt().await {
            Outcome::Done(v) => return Ok(v),
            Outcome::Permanent(msg) => {
                return Err(anyhow!("{label}: permanent failure, not retrying: {msg}"));
            }
            Outcome::Retryable(msg) => {
                if tries > max_retries {
                    return Err(anyhow!(
                        "{label}: giving up after {tries} attempts (last error: {msg}). \
                         A rerun from scratch is safe once the target is reachable again."
                    ));
                }
                let delay = backoff_delay(tries, base, cap);
                eprintln!(
                    "  [retry] {label}: {msg} — retrying in {:.1}s (attempt {tries}/{max_retries})",
                    delay.as_secs_f64()
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}
