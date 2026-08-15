mod cli;
mod http;
mod mapping;
mod push;
mod retry;
mod scan;
mod security;
mod sync;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use clap::Parser;

use cli::Args;
use http::EsClient;

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(e) = run(args).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<()> {
    // ── Guards ──────────────────────────────────────────────────────────
    // Same reasoning as wal_tap.rs's system-index exclusion: nothing here is
    // scoped tightly enough to make a per-index judgement call safe, so the
    // whole class of dotted names is refused outright, as source AND target.
    if security::is_system_index(&args.source_index) {
        bail!(
            "--source-index {:?} starts with '.': system/hidden indices are never read or \
             written by this tool.",
            args.source_index
        );
    }
    security::check_base_url("--source-url", &args.source_url)?;
    security::check_base_url("--target-url", &args.target_url)?;

    let timeout = Duration::from_secs(args.request_timeout_secs);
    let source = EsClient::new(&args.source_url, args.source_auth(), timeout)?;
    let target = EsClient::new(&args.target_url, args.target_auth(), timeout)?;

    eprintln!(
        "xerj-seed: {:?} @ {} -> {} (batch_size={})",
        args.source_index,
        source.redacted_base_url(),
        target.redacted_base_url(),
        args.batch_size
    );

    // ── Mapping/settings import ─────────────────────────────────────────
    // Before any document moves: give the target the source's real schema
    // instead of leaving it to dynamic-mapping inference from the first
    // _bulk batch. No-op (and safe to rerun) once the target index exists.
    if !args.skip_mapping_import {
        mapping::import_mapping_if_needed(
            &source,
            &target,
            &args.source_index,
            args.max_retries,
        )
        .await?;
    }

    // A background task flips this the moment Ctrl-C is seen; the scan loop
    // checks it between pages so a PIT that is open always gets closed
    // instead of leaking until its keep_alive expires.
    let interrupted = Arc::new(AtomicBool::new(false));
    {
        let interrupted = interrupted.clone();
        tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                interrupted.store(true, Ordering::SeqCst);
                eprintln!("\nxerj-seed: interrupted — finishing the current batch and closing the PIT...");
            }
        });
    }

    // ── Scan + push ─────────────────────────────────────────────────────
    let pit_id = scan::open_pit(
        &source,
        &args.source_index,
        &args.pit_keep_alive,
        args.max_retries,
    )
    .await?;

    let outcome = scan_and_push(&args, &source, &target, &pit_id, &interrupted).await;

    // Cleanup block: close the PIT whatever happened above (success, error,
    // or Ctrl-C), matching wal_tap.rs's own posture that a best-effort
    // cleanup must not depend on the happy path.
    scan::close_pit(&source, &pit_id).await;

    let stats = outcome?;

    if interrupted.load(Ordering::SeqCst) {
        bail!(
            "interrupted after scanning {} / pushing {} documents. Rerun from scratch when \
             ready — the push is idempotent, so this is always safe.",
            stats.scanned,
            stats.shipped
        );
    }

    eprintln!(
        "xerj-seed: done. scanned={} shipped={} conflicts={} rejected={}{}",
        stats.scanned,
        stats.shipped,
        stats.conflicts,
        stats.rejected,
        stats
            .last_rejection_kind
            .as_deref()
            .map(|k| format!(" (last: {k})"))
            .unwrap_or_default()
    );

    if stats.rejected > 0 {
        bail!(
            "{} document(s) were permanently rejected by the target (see the warnings above). \
             Not arming wal_tap; fix the rejected documents or the target mapping and rerun.",
            stats.rejected
        );
    }

    // ── Arm incremental sync (XERJ-only — see README's "Beyond XERJ") ──
    // Everything above this point (mapping import, scan, push) is generic
    // ES-compat wire protocol and has already fully succeeded by the time
    // we get here. This step alone assumes --source-url is a XERJ node: it
    // calls PUT /_xerj/wal_tap, a native XERJ endpoint no other engine
    // exposes. A failure here — most commonly a 404 because --source-url is
    // a real Elasticsearch/OpenSearch cluster — does not undo, invalidate,
    // or need to roll back the migration that already happened; it just
    // means the operator has to arrange their own incremental sync for that
    // source engine.
    if args.enable_sync_after {
        eprintln!("xerj-seed: arming wal_tap on the source for incremental sync...");
        if let Err(e) = sync::enable_wal_tap(
            &source,
            &args.source_index,
            &args.target_url,
            args.target_auth(),
            args.max_retries,
        )
        .await
        {
            bail!(
                "document migration succeeded ({} document(s) shipped to the target) — but \
                 --enable-sync-after failed: {e:#}\n\n\
                 --enable-sync-after is XERJ-specific: it calls PUT /_xerj/wal_tap on \
                 --source-url, a native endpoint that only a XERJ node exposes. If \
                 --source-url points to a real Elasticsearch or OpenSearch cluster, this \
                 step was never going to succeed there — the migrated data on the target is \
                 unaffected either way. Rerun without --enable-sync-after, or set up your \
                 own incremental sync for that source engine.",
                stats.shipped
            );
        }
        eprintln!("xerj-seed: wal_tap enabled on the source. Seed complete.");
    } else {
        eprintln!("xerj-seed: seed complete (--enable-sync-after not passed; wal_tap left untouched).");
    }

    Ok(())
}

struct RunStats {
    scanned: u64,
    shipped: u64,
    conflicts: u64,
    rejected: u64,
    last_rejection_kind: Option<String>,
}

async fn scan_and_push(
    args: &Args,
    source: &EsClient,
    target: &EsClient,
    pit_id: &str,
    interrupted: &Arc<AtomicBool>,
) -> Result<RunStats> {
    let mut search_after: Option<String> = None;
    let mut scanned = 0u64;
    let mut shipped = 0u64;
    let mut conflicts = 0u64;
    let mut rejected = 0u64;
    let mut last_rejection_kind: Option<String> = None;
    let mut batch_no = 0u64;
    let started = Instant::now();

    loop {
        if interrupted.load(Ordering::SeqCst) {
            break;
        }

        let (docs, next_after) = scan::next_page(
            source,
            pit_id,
            &args.pit_keep_alive,
            args.batch_size,
            search_after.as_deref(),
            args.max_retries,
        )
        .await?;

        if docs.is_empty() {
            break;
        }
        batch_no += 1;
        scanned += docs.len() as u64;
        search_after = next_after;

        let result = push::push_batch(target, &args.source_index, &docs, args.max_retries).await?;
        shipped += result.shipped;
        conflicts += result.conflicts;
        rejected += result.other_rejections;
        if result.last_rejection_kind.is_some() {
            last_rejection_kind = result.last_rejection_kind;
        }
        if result.other_rejections > 0 {
            eprintln!(
                "  [batch {batch_no}] target rejected {} document(s), most recently with {}",
                result.other_rejections,
                last_rejection_kind.as_deref().unwrap_or("unknown")
            );
        }

        eprintln!(
            "  [batch {batch_no}] scanned={scanned} shipped={shipped} conflicts={conflicts} \
             rejected={rejected} ({:.1}s elapsed)",
            started.elapsed().as_secs_f64()
        );
    }

    Ok(RunStats {
        scanned,
        shipped,
        conflicts,
        rejected,
        last_rejection_kind,
    })
}
