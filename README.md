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
│ ES-compatible│   2. full scan (PIT + search_after)                     │ ES-compatible│
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
- `--pit-keep-alive` — Point-in-Time TTL, renewed on every page (default
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
endpoint. Everything else — the mapping/settings import, the PIT +
`search_after` scan, the `_bulk` push — is the standard Elasticsearch wire
protocol. `_bulk` (the target side) is genuinely universal — verified
end-to-end against a real OpenSearch 3.7.0 target (see
[Testing](#testing)) — and there's no reason to expect Elasticsearch to
differ, since it's the same protocol OpenSearch forked from.

**The source side's PIT is not universal, though**, and this is worth being
precise about rather than optimistic: xerj-seed's scan uses Elasticsearch's
PIT shape (`POST /{index}/_pit`, `DELETE /_pit` with `{"id": ...}`).
Real Elasticsearch and xerj both speak that shape. **Real OpenSearch does
not** — verified live against OpenSearch 3.7.0: `POST /{index}/_pit`
returns `400`. OpenSearch's actual PIT API is `POST
/{index}/_search/point_in_time`, closed via `DELETE /_search/point_in_time`
with `{"pit_id": [...]}` (an array, under a different key) — a distinct
enough shape that dialect-detection is a real follow-up, not a one-line
fix. **So: a real Elasticsearch or xerj source works today; a real
OpenSearch source does not yet** — only OpenSearch-as-*target* has been
built and verified.

Concretely, today:

```sh
# Elasticsearch → OpenSearch (e.g. moving off an Elastic-licensed cluster) — works
xerj-seed \
  --source-url  https://es-cluster.internal:9200 \
  --source-index orders \
  --source-auth "ApiKey $ES_API_KEY" \
  --target-url  https://os-cluster.internal:9200 \
  --target-auth "Basic $(printf '%s' 'admin:pass' | base64)"

# Same-engine migration (cluster resize, region move, version upgrade
# via reindex-to-new-cluster) — no --enable-sync-after either way
xerj-seed \
  --source-url  https://es-old.internal:9200 \
  --source-index events \
  --target-url  https://es-new.internal:9200
```

A real OpenSearch cluster as `--source-url` is not supported yet (see
above) — that's a real gap, tracked as a follow-up, not a "should work but
untested" claim.

None of these pass `--enable-sync-after` — there's nothing xerj-specific to
arm. If you *do* pass it with a source that isn't xerj, `PUT
/_xerj/wal_tap` has nowhere to land there. Live-verified against a real
OpenSearch 3.7.0 node: `400`, `{"error":"no handler found for uri
[/_xerj/wal_tap] and method [PUT]"}` — its generic unknown-route response
(not a `404`, worth knowing if you're expecting Elasticsearch's
convention; a real Elasticsearch source, which unlike OpenSearch does get
as far as this step, may well answer differently). Either way, this is
the response the same retry/fail-fast policy the rest of this tool uses
(see [Attribution](#attribution)) treats as a non-retryable failure, and
`main.rs` wraps it before propagating — naming the document count already
shipped and stating plainly that the migration is unaffected, rather than
surfacing a bare HTTP error or leaving you to guess whether anything needs
cleaning up:

```
xerj-seed: done. scanned=110 shipped=110 conflicts=0 rejected=0
xerj-seed: arming wal_tap on the source for incremental sync...
error: document migration succeeded (110 document(s) shipped to the target) — but
--enable-sync-after failed: enable wal_tap: permanent failure, not retrying: HTTP 400: ...

--enable-sync-after is XERJ-specific: it calls PUT /_xerj/wal_tap on --source-url, a
native endpoint that only a XERJ node exposes. If --source-url points to a real
Elasticsearch or OpenSearch cluster, this step was never going to succeed there — the
migrated data on the target is unaffected either way. Rerun without --enable-sync-after,
or set up your own incremental sync for that source engine.
```

The `HTTP 400: ...` line above was captured live (`PUT /_xerj/wal_tap`
against real OpenSearch); the two lines around it are `main.rs`'s own
wrapping, exercised by unit-equivalent reasoning rather than one single
live run reaching this exact point — [today's PIT gap](#beyond-xerj-generic-es-compatible-migration)
means a real OpenSearch source never gets this far (it fails earlier, at
the scan), and no Elasticsearch cluster was reachable while writing this.

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

Mapping/settings import was verified the same way: an index created on the
xerj source with an explicit mapping (a `keyword` field and a plain `text`
field with no dynamic `.keyword` sub-field), migrated to the OpenSearch
target, came back with that exact mapping — not what OpenSearch's own
dynamic-mapping defaults would have inferred (which adds a `.keyword`
sub-field to a `text`-looking string automatically). A second run against
the same already-created target index confirmed the skip path: no mapping
call made, the one document re-sent resolved as a version conflict rather
than a duplicate.

The [Beyond XERJ](#beyond-xerj-generic-es-compatible-migration) section's
claims were checked directly rather than assumed: `_bulk` against real
OpenSearch works (above); a real OpenSearch node as `--source-url` was
tried and fails cleanly at the PIT-open step (`HTTP 400`, the response
body quoted verbatim, not a panic or a hang) because OpenSearch's real PIT
API has a different shape than Elasticsearch's — a genuine, now-documented
gap rather than an assumption.

A leg against a real **Elasticsearch** cluster (as either source or
target) was attempted but not completed — the local Docker daemon used to
test this could not pull the image (a registry/network issue in that
environment, unrelated to this tool). `_bulk`/`_search` as the target side
needs is the same wire format Elasticsearch serves, so there's no reason
to expect different behavior there — but, per the section above, that is
now explicitly flagged as unverified rather than implied by omission.
Contributions running that leg are welcome.

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
