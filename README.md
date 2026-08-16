# xerj-seed

A one-shot CLI that migrates one index between any two ES-compatible
clusters — Elasticsearch, OpenSearch, or [xerj](https://github.com/xerj-org/xerj),
in any combination — over the standard `_search`/`_bulk` wire protocol.
Optionally arms xerj's `wal_tap` on the source afterward for incremental
sync; that one step is the only part of this tool that assumes xerj is
involved at all. See [Beyond XERJ](#beyond-xerj-generic-es-compatible-migration)
for the general case.

```
┌──────────────┐   1. mapping/settings import (GET/PUT the index, once)  ┌──────────────┐
│ ES-compatible│   2. full scan (_search?scroll=… + _search/scroll)      │ ES-compatible│
│    source    │ ───────────────────────────────────────────────────────▶│    target    │
│ (xerj / ES / │   3. _bulk push (index, version_type: external,         │ (xerj / ES / │
│  OpenSearch) │      version = source _seq_no)                          │  OpenSearch) │
└──────────────┘                                                         └──────────────┘
       │
       └── 4. (optional, XERJ source only) PUT /_xerj/wal_tap ─── incremental sync from here on
```

## Why this exists

The original motivating case: xerj's `wal_tap` only ships writes that happen
*after* it is turned on — it is explicitly not a backfill mechanism (see its
own docs: "There is no backfill. Seed the target with snapshot/restore if it
needs the existing documents."). `xerj-seed` is that seed step: it does the
one-time bulk copy of everything already in the source index, and then flips
`wal_tap` on at the exact point where the copy ends, so nothing written in
between is lost and nothing has to be copied twice.

Steps 1–3 (mapping import, scan, push) have no idea what kind of cluster is
on either end — they're plain ES-compat wire calls. Step 4 is the one xerj
-specific extra, and it's entirely optional.

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
- `--enable-sync-after` — **XERJ-only**, see
  [Beyond XERJ](#beyond-xerj-generic-es-compatible-migration). After a
  successful scan+push, `PUT /_xerj/wal_tap` on the source with
  `enabled=true`, `indices=[--source-index]`, and the same
  `target_url`/`target_auth` this run used, so the source starts streaming
  every subsequent write to the target. `--source-auth` is reused as the
  admin credential for this call — `/_xerj/wal_tap` is superuser-only on
  xerj.
- `--skip-mapping-import` — don't copy the source index's mapping/settings
  to the target first (see [Mapping/settings import](#mappingsettings-import)
  below).
- `--max-retries` — how many consecutive transient failures (5xx / 429 /
  408 / transport error) to absorb with backoff before giving up (default
  `10`). See [Resumability](#resumability--no-checkpoint-file).
- `--scroll-keep-alive` — scroll TTL, renewed on every page (default
  `5m`).
- `--request-timeout-secs` — per-HTTP-request timeout (default `30`).

Progress is printed to stderr per batch (documents scanned, shipped,
conflicts, rejections); there is no structured log file — this is a
one-shot CLI, not a service.

## Mapping/settings import

Before any document moves, xerj-seed does `GET /{source-index}` on the
source and, **only if the target doesn't already have that index**,
creates it there with the same mappings and settings (`PUT
/{index}` with a handful of non-portable, engine-assigned keys stripped —
`uuid`, `version`, `provided_name`, `creation_date`, `history`). This means
the target ends up with the source's real field types from the start,
instead of whatever dynamic-mapping inference would have guessed from the
first `_bulk` batch — which matters most exactly when source and target are
different engines with different inference defaults (a `keyword` on one
side vs. a `text` on the other for the same field is a common one).

If the target index already exists, this step does nothing — it never
edits an existing index's mapping. That makes it safe to rerun for the same
reason everything else in this tool is: idempotent by construction, no
state to reconcile. Pass `--skip-mapping-import` to disable it entirely and
let the target's own dynamic mapping handle every field instead.

## Beyond XERJ: generic ES-compatible migration

Only one thing this tool does assumes xerj is on either end:
`--enable-sync-after`, which calls `PUT /_xerj/wal_tap` — a native xerj
endpoint. Everything else — the mapping/settings import, the scroll scan,
the `_bulk` push — is the standard Elasticsearch wire protocol, and source
and target genuinely don't need to be the same kind of cluster, or even
related.

That wasn't true of the first design, worth being upfront about: it used
Point-in-Time (`POST /{index}/_pit` + `search_after`), which real
Elasticsearch and xerj both speak but real OpenSearch does not — verified
live, OpenSearch 3.7.0's actual PIT API is a differently-shaped
`POST /{index}/_search/point_in_time`. The scan now uses `_scroll`
instead (`POST /{index}/_search?scroll=…`, `POST /_search/scroll`,
`DELETE /_search/scroll`) specifically because it predates that fork and
is live-verified byte-for-byte identical on xerj and on a real OpenSearch
3.7.0 node, `_seq_no` included. See `scan.rs`'s module docs for the detail.

Concretely, today:

```sh
# Elasticsearch → OpenSearch (e.g. moving off an Elastic-licensed cluster)
xerj-seed \
  --source-url  https://es-cluster.internal:9200 \
  --source-index orders \
  --source-auth "ApiKey $ES_API_KEY" \
  --target-url  https://os-cluster.internal:9200 \
  --target-auth "Basic $(printf '%s' 'admin:pass' | base64)"

# OpenSearch → xerj (or Elasticsearch — same call, different --target-url)
xerj-seed \
  --source-url  https://os-cluster.internal:9200 \
  --source-index products \
  --source-auth "Basic $(printf '%s' 'admin:pass' | base64)" \
  --target-url  https://es-cluster.internal:9200 \
  --target-auth "ApiKey $ES_API_KEY"

# Same-engine migration (cluster resize, region move, version upgrade
# via reindex-to-new-cluster) — no --enable-sync-after either way
xerj-seed \
  --source-url  https://es-old.internal:9200 \
  --source-index events \
  --target-url  https://es-new.internal:9200
```

All three source/target roles are live-verified — real Elasticsearch
7.10.2 and real OpenSearch 3.7.0, each as both source and target, into and
out of xerj (see [Testing](#testing)). Elasticsearch ↔ OpenSearch directly
wasn't run as its own leg, but is the same call with each end individually
already covered.

None of these pass `--enable-sync-after` — there's nothing xerj-specific
to arm. If you *do* pass it with a source that isn't xerj, `PUT
/_xerj/wal_tap` has nowhere to land there, and this is where the migration
already having fully succeeded by that point matters: the failure is
reported plainly, not left to bubble up as a bare HTTP error. Captured
live, start to finish, source = real OpenSearch 3.7.0:

```
xerj-seed: done. scanned=50 shipped=50 conflicts=0 rejected=0
xerj-seed: arming wal_tap on the source for incremental sync...
error: document migration succeeded (50 document(s) shipped to the target) — but
--enable-sync-after failed: enable wal_tap: permanent failure, not retrying: HTTP 400
Bad Request: {"error":"no handler found for uri [/_xerj/wal_tap] and method [PUT]"}

--enable-sync-after is XERJ-specific: it calls PUT /_xerj/wal_tap on --source-url, a
native endpoint that only a XERJ node exposes. If --source-url points to a real
Elasticsearch or OpenSearch cluster, this step was never going to succeed there — the
migrated data on the target is unaffected either way. Rerun without --enable-sync-after,
or set up your own incremental sync for that source engine.
```

The exact response varies by engine — worth knowing rather than assuming
one shape, since this tool deliberately doesn't try to match a specific
code. Both captured live, `PUT /_xerj/wal_tap` against a real source:

| Source engine | Status | Body |
|---|---|---|
| OpenSearch 3.7.0 | `400` | `{"error":"no handler found for uri [/_xerj/wal_tap] and method [PUT]"}` |
| Elasticsearch 7.10.2 | `405` | `{"error":"Incorrect HTTP method for uri [/_xerj/wal_tap] and method [PUT], allowed: [POST]"}` |

Either way, it's a non-retryable status, so xerj-seed's retry/fail-fast
policy (see [Attribution](#attribution)) reports it immediately rather
than backing off and retrying a request that will never succeed.

## Resumability — no checkpoint file

`xerj-seed` keeps no state between runs, by design. If the process is
interrupted (Ctrl-C, crash, network partition), just run it again from the
beginning: the push is idempotent by construction — every document is sent
as `action: index, version_type: external, version: <source _seq_no>`, so
the target never ends up with a duplicate; a full rerun always converges to
the same content. Live-verified, twice: a full rerun after a successful run
reports the same document count on the target either way it can resolve —
against a real OpenSearch target as a genuine `version_conflict_engine_exception`
per re-sent document (counted, reported, not treated as an error), and
against xerj-as-target as an accepted same-version overwrite instead (no
conflict counted, but the content — and the fact that nothing duplicated —
is identical either way). Which of the two happens is the target engine's
own external-versioning comparison, not something this tool controls or
needs to; both are safe. This trades resume efficiency (a rerun rescans
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

The tool was exercised end-to-end against real clusters throughout —
xerj (built from `xerj-org/xerj` `main`, which has `wal_tap`), a real
**OpenSearch 3.7.0**, and a real **Elasticsearch 7.10.2** — in every
source/target combination among the three:

- **xerj → OpenSearch**, twice independently (including a rerun after an
  unrelated local Docker restart, to also exercise the "rerun from
  scratch" path this tool's idempotency is built around): every document
  lands on the target, `GET /_xerj/wal_tap` on the source reports
  `enabled: true` with the right `indices`/`target_url` after the run,
  and a write made on the source *after* the run propagates to the
  target on its own (`wal_tap`'s own `_stats` reporting `healthy: true`,
  `lag_seq: 0`) with no further action. `scripts/e2e-smoke-test.sh`
  captures this sequence in reusable form.
- **OpenSearch → xerj** (the direction PIT couldn't reach — see
  [Beyond XERJ](#beyond-xerj-generic-es-compatible-migration)): 50
  documents scanned and shipped via `_scroll`, a rerun against the
  already-created target index correctly skipped the mapping import and
  reported the same 50/50 the second time (no duplication — see
  [Resumability](#resumability--no-checkpoint-file) for what "reports
  the same count" resolves to on each target engine), and
  `--enable-sync-after` against this real non-xerj source produced the
  exact failure message quoted in that section, captured from one single
  live run start to finish.
- **Elasticsearch → xerj**: 40 documents scanned and shipped, target
  mapping confirmed to match the source's explicit mapping exactly
  (`keyword` field, plain `text` field, no dynamic sub-field added), and
  `--enable-sync-after` against this real Elasticsearch source produced
  the `405` response quoted in the table above — a third distinct status
  code from a third engine, on top of OpenSearch's `400`.
- **xerj → Elasticsearch**: one document, confirmed present on the target
  (`_version: 0`, matching the source's `_seq_no`, after allowing for
  Elasticsearch's ~1s default `refresh_interval` — `_count` right after
  the write undercounts until the next refresh, which is Elasticsearch's
  own behavior, not this tool's).

Mapping/settings import was verified against both non-xerj engines: an
index created on the source with an explicit mapping (a `keyword` field
and a plain `text` field with no dynamic `.keyword` sub-field) came back
on the target with that exact mapping, not what the target's own
dynamic-mapping defaults would have inferred (OpenSearch in particular
adds a `.keyword` sub-field to a `text`-looking string automatically —
this is exactly the divergence the feature exists to prevent).

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
