//! Safety guards shared by every phase of the tool: URL validation, credential
//! redaction for anything printed to the terminal, and the system-index
//! exclusion.
//!
//! Adapted (not copied) from the equivalent checks in
//! `xerj-org/xerj`'s `engine/crates/xerj-common/src/config.rs`
//! (`WalTapConfig::check_target_url` / `redacted_target_url`) and
//! `engine/crates/xerj-engine/src/wal_tap.rs` (system-index exclusion). See
//! the README's Attribution section for links to the originals.

use anyhow::{bail, Result};

/// Reject a base URL this tool must not accept.
///
/// Two rules:
///
/// 1. It must be an absolute `http://` / `https://` URL — every phase of this
///    tool joins a path onto it (`/{index}/_pit`, `/_bulk`, `/_xerj/wal_tap`),
///    and a relative URL would silently target nothing.
/// 2. **No userinfo.** `https://user:pass@host` is a URL `reqwest` accepts,
///    and it turns the userinfo into a `Basic` `Authorization` header —
///    meaning a credential typed into `--source-url` / `--target-url` would
///    be sent as one regardless, while also being the exact string this tool
///    echoes in its progress output and error messages. Credentials belong in
///    `--source-auth` / `--target-auth`, which are never echoed (see
///    [`redact_url`] for the belt-and-braces case: a URL that carries
///    userinfo despite this check having run, e.g. because a caller
///    constructs one programmatically without going through the CLI parser).
pub fn check_base_url(flag: &str, url: &str) -> Result<()> {
    let trimmed = url.trim();
    let Some(rest) = trimmed
        .strip_prefix("http://")
        .or_else(|| trimmed.strip_prefix("https://"))
    else {
        bail!("{flag} must be an absolute http:// or https:// URL, e.g. \"https://localhost:9200\" (got {trimmed:?})");
    };
    // Userinfo is everything before the first `@` of the authority, which
    // ends at the first `/`, `?` or `#`.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    if authority.contains('@') {
        bail!(
            "{flag} must not carry credentials in the URL (user:password@host): this tool \
             prints the URL in its progress output and error messages. Put the credential in \
             --source-auth / --target-auth instead, e.g. --source-auth \"Basic dXNlcjpwdw==\" \
             or --source-auth \"ApiKey abc123\"."
        );
    }
    Ok(())
}

/// `scheme://user:pass@host/…` → `scheme://***@host/…`.
///
/// Belt and braces on top of [`check_base_url`]: the check refuses userinfo
/// on the way in, so this only fires for a URL that was never validated (a
/// future call site, a bug). A credential must never reach stderr or an error
/// message even if the guard upstream of it is ever weakened.
pub fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    match authority.rsplit_once('@') {
        Some((_userinfo, host)) => format!("{scheme}://***@{host}{tail}"),
        None => url.to_string(),
    }
}

/// Is `index` one xerj-seed must never touch, as source or target?
///
/// Mirrors `wal_tap.rs`'s system-index exclusion, generalised: any
/// `.`-prefixed name is a system/hidden index on every ES-compatible engine
/// this tool talks to (xerj's own `.xerj*` namespace, but also `.security`,
/// `.kibana`, `.opendistro*`, `.ds-*` hidden backing indices, and so on), and
/// the same reasoning applies — nothing here is index-scoped, so there is no
/// per-index judgement call to make; the whole class is refused.
pub fn is_system_index(index: &str) -> bool {
    index.starts_with('.')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_plain_http_and_https() {
        assert!(check_base_url("--source-url", "http://localhost:9200").is_ok());
        assert!(check_base_url("--source-url", "https://es.example.com:9200").is_ok());
    }

    #[test]
    fn rejects_relative_or_schemeless_urls() {
        assert!(check_base_url("--source-url", "localhost:9200").is_err());
        assert!(check_base_url("--source-url", "//localhost:9200").is_err());
    }

    #[test]
    fn rejects_userinfo() {
        let err = check_base_url("--target-url", "https://user:pass@host:9200")
            .unwrap_err()
            .to_string();
        assert!(err.contains("--target-auth"));
    }

    #[test]
    fn redacts_userinfo_but_leaves_plain_urls_alone() {
        assert_eq!(
            redact_url("https://user:pass@host:9200/idx"),
            "https://***@host:9200/idx"
        );
        assert_eq!(
            redact_url("https://host:9200/idx"),
            "https://host:9200/idx"
        );
    }

    #[test]
    fn system_index_guard_matches_any_dot_prefix() {
        assert!(is_system_index(".xerj_users"));
        assert!(is_system_index(".kibana"));
        assert!(is_system_index(".security-7"));
        assert!(!is_system_index("edge-logs"));
    }
}
