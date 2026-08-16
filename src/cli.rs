use clap::Parser;

/// xerj-seed — one-shot index migration between ES-compatible clusters.
///
/// Fully scans a source index (Elasticsearch, OpenSearch, or xerj) and
/// writes every document to an ES-compatible target, and — optionally, and
/// only when the source is xerj — arms `/_xerj/wal_tap` on the source when
/// done, so the target keeps receiving writes incrementally without further
/// intervention.
///
/// No state is kept between runs. If the process is interrupted, rerun it
/// from scratch: the push is idempotent (index action, version_type
/// external, version = source seq_no), so a rerun always converges without
/// duplicating documents.
#[derive(Debug, Parser)]
#[command(name = "xerj-seed", version, about, long_about = None)]
pub struct Args {
    /// Base URL of the source cluster, e.g. https://source:9200
    #[arg(long)]
    pub source_url: String,

    /// Index to scan on the source. Must not start with '.' (system indices
    /// are never read or written by this tool).
    #[arg(long)]
    pub source_index: String,

    /// Authorization header value for the source, e.g. "ApiKey abc123" or
    /// "Basic dXNlcjpwdw==". Superuser credential — also used for the
    /// wal_tap PUT when --enable-sync-after is passed.
    #[arg(long)]
    pub source_auth: Option<String>,

    /// Base URL of the target ES-compatible cluster, e.g. https://target:9200
    #[arg(long)]
    pub target_url: String,

    /// Authorization header value for the target.
    #[arg(long)]
    pub target_auth: Option<String>,

    /// Documents per _search page and per _bulk request.
    #[arg(long, default_value_t = 1000)]
    pub batch_size: usize,

    /// After a successful scan+push, PUT /_xerj/wal_tap on the source with
    /// enabled=true, indices=[--source-index], target_url and target_auth,
    /// so the source keeps streaming subsequent writes to the target.
    #[arg(long, default_value_t = false)]
    pub enable_sync_after: bool,

    /// Scroll keep_alive, e.g. "5m". Renewed on every page.
    #[arg(long, default_value = "5m")]
    pub scroll_keep_alive: String,

    /// Per-HTTP-request timeout in seconds.
    #[arg(long, default_value_t = 30)]
    pub request_timeout_secs: u64,

    /// Give up on a batch after this many retryable failures in a row
    /// (5xx / 429 / 408 / transport errors). A rerun from scratch is always
    /// safe, so this bounds how long a one-shot run waits on a target that
    /// may never recover, rather than retrying forever.
    #[arg(long, default_value_t = 10)]
    pub max_retries: u32,

    /// Do not copy the source index's mapping/settings to the target before
    /// pushing documents. By default xerj-seed fetches GET
    /// /{source-index} from the source and, if the target index does not
    /// already exist, creates it there with the same mappings and settings
    /// — so the target ends up with the source's real schema instead of
    /// whatever the target's dynamic-mapping inference guesses from the
    /// first batch of documents. If the target index already exists, this
    /// step is skipped either way (never touches an existing index's
    /// mapping — safe to rerun, same as everything else in this tool).
    #[arg(long, default_value_t = false)]
    pub skip_mapping_import: bool,
}

impl Args {
    /// `Authorization` header value for the source, empty string meaning
    /// "send none" — matches wal_tap's convention for `target_auth`.
    pub fn source_auth(&self) -> &str {
        self.source_auth.as_deref().unwrap_or("")
    }

    pub fn target_auth(&self) -> &str {
        self.target_auth.as_deref().unwrap_or("")
    }
}
