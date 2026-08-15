# xerj-seed

A one-shot CLI that seeds an ES-compatible target index from a
[xerj](https://github.com/xerj-org/xerj) source index, then arms xerj's
`wal_tap` on the source so the target keeps receiving writes incrementally,
without any further intervention.

```
┌──────────────┐   1. full scan (PIT + search_after on _seq_no)   ┌──────────────────┐
│  xerj source │ ───────────────────────────────────────────────▶ │ ES-compat target │
│              │   2. _bulk push (index, version_type: external,  │ (xerj / OpenSearch│
│              │      version = source _seq_no)                   │  / Elasticsearch) │
│              │                                                   └──────────────────┘
│              │   3. PUT /_xerj/wal_tap (enabled=true) ─── incremental sync from here on
└──────────────┘
```

## Why this exists

`wal_tap` only ships writes that happen *after* it is turned on — it is
explicitly not a backfill mechanism (see its own docs: "There is no
backfill. Seed the target with snapshot/restore if it needs the existing
documents."). `xerj-seed` is that seed step: it does the one-time bulk copy
of everything already in the source index, and then flips `wal_tap` on at
the exact point where the copy ends, so nothing written in between is lost
and nothing has to be copied twice.

## Usage

```sh
xerj-seed \
  --source-url    https://source.internal:9200 \
  --source-index  orders \
  --source-auth   "ApiKey $SOURCE_API_KEY" \
  --target-url    https://target.internal:9200 \
  --target-auth   "Basic $(printf '%s' 'user:pass' | base64)" \
  --batch-size    1000 \
  --enable-sync-after
```

- `--source-url` / `--target-url` — base URL of each cluster. Must be
  absolute `http://`/`https://` and must not carry credentials in the URL
  itself (`user:pass@host`) — put those in `--source-auth` / `--target-auth`
  instead. See [Security](#security).
- `--source-auth` / `--target-auth` — optional, sent verbatim as the
  `Authorization` header (e.g. `"ApiKey abc123"`, `"Basic dXNlcjpwdw=="`).
  Omit for an unauthenticated cluster.
- `--source-index` — the index to copy. Must not start with `.` (system/
  hidden indices are refused as both source and target — see
  [Security](#security)).
- `--batch-size` — documents per `_search` page and per `_bulk` request
  (default `1000`).
- `--enable-sync-after` — after a successful scan+push, `PUT
  /_xerj/wal_tap` on the source with `enabled=true`,
  `indices=[--source-index]`, and the same `target_url`/`target_auth` this
  run used, so the source starts streaming every subsequent write to the
  target. `--source-auth` is reused as the admin credential for this call —
  `/_xerj/wal_tap` is superuser-only on xerj.
- `--max-retries` — how many consecutive transient failures (5xx / 429 /
  408 / transport error) to absorb with backoff before giving up (default
  `10`). See [Resumability](#resumability--no-checkpoint-file).
- `--pit-keep-alive` — Point-in-Time TTL, renewed on every page (default
  `5m`).
- `--request-timeout-secs` — per-HTTP-request timeout (default `30`).

Progress is printed to stderr per batch (documents scanned, shipped,
conflicts, rejections); there is no structured log file — this is a
one-shot CLI, not a service.

## Resumability — no checkpoint file

`xerj-seed` keeps no state between runs, by design. If the process is
interrupted (Ctrl-C, crash, network partition), just run it again from the
beginning: the push is idempotent by construction — every document is sent
as `action: index, version_type: external, version: <source _seq_no>`, so
the target keeps whichever write carries the highest version for each
`_id`. Re-sending a document the target already has at the same-or-newer
version is a no-op (a benign `version_conflict_engine_exception`, counted
and reported, not an error). This trades resume efficiency (a rerun rescans
everything) for simplicity (no checkpoint format to get wrong, no
partially-applied state to reason about) — deliberately.

## Security

- **No credentials in URLs.** `--source-url` / `--target-url` are rejected
  if they carry userinfo (`https://user:pass@host`). Most HTTP clients,
  including the one this tool uses, turn that into a `Basic Authorization`
  header transparently — so it would be `--source-auth`/`--target-auth`
  wearing a disguise, sent regardless of the check, and also the exact
  string this tool prints to stderr on every run. Put credentials in
  `--source-auth` / `--target-auth`, which are never echoed.
- **System indices are never touched**, as source or target: any index
  name starting with `.` is refused outright before any network call is
  made. There is no per-index exception — nothing in this tool is scoped
  finely enough to make that judgement call safely (the same reasoning
  `wal_tap` applies to its own allowlist).
- **No secrets committed.** This repo ships no test credentials, no
  `.env`, nothing under `data-release-test/` or similar — see
  `.gitignore`.

## Building

```sh
cargo build --release
# binary at target/release/xerj-seed
```

## Testing

`cargo test` runs the unit tests (URL validation, redaction, the system-
index guard).

The tool was also exercised end-to-end against a real xerj source (built
from `xerj-org/xerj` `main`, which has `wal_tap`) and a real **OpenSearch
3.7.0** target container, twice independently (including a rerun after an
unrelated local Docker restart, to also exercise the "rerun from scratch"
path this tool's idempotency is built around). Both runs confirmed: every
document (115–120, varied per run) lands on the target, `GET
/_xerj/wal_tap` on the source reports `enabled: true` with the right
`indices`/`target_url` after the run, and a write made on the source
*after* the run propagates to the target on its own (`wal_tap`'s own
`_stats` reporting `healthy: true`, `lag_seq: 0`) with no further action.
`scripts/e2e-smoke-test.sh` captures this same sequence in reusable form.

A third leg against a real **Elasticsearch 8.x** target was attempted but
not completed — the local Docker daemon used to test this could not pull
the image (a registry/network issue in that environment, unrelated to
this tool). Nothing in `xerj-seed` is OpenSearch-specific: it speaks
plain `_bulk`/`_search`, the same wire format Elasticsearch serves, so
there's no reason to expect different behavior there — but it hasn't been
verified against a live ES cluster yet. Contributions running that leg
are welcome.

## Attribution

The `_bulk` push semantics (`index` action, `version_type: external`,
`version` = the source document's `_seq_no` — what makes a full rerun after
an interruption always converge instead of duplicating writes) and the
retry/fail-fast classification of target responses are adapted from
xerj's single-node WAL tap, Apache-2.0:
<https://github.com/xerj-org/xerj/blob/main/engine/crates/xerj-engine/src/wal_tap.rs>

The `target_url` validation (rejecting embedded userinfo) and credential
redaction are adapted from xerj's WAL tap REST surface, Apache-2.0:
<https://github.com/xerj-org/xerj/blob/main/engine/crates/xerj-api/src/wal_tap_api.rs>

No code was copied verbatim — the patterns above were reimplemented in this
project's own Rust source, adapted to a one-shot CLI rather than a
long-running daemon (notably: this tool gives up after `--max-retries`
consecutive transient failures rather than backing off forever, since a
rerun from scratch is always safe here — see
[Resumability](#resumability--no-checkpoint-file)). See `NOTICE` for the
full attribution text.

## License

Apache-2.0. See `LICENSE`.
