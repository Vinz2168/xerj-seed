#!/usr/bin/env bash
# End-to-end smoke test: a real xerj source node, seeded with ~100+
# documents, pushed by xerj-seed to a real target (xerj / OpenSearch /
# Elasticsearch), with --enable-sync-after — then a write made on the
# source AFTER the run is confirmed to reach the target on its own.
#
# This is the same sequence used to validate this tool during development
# (see README.md's Testing section). It expects:
#   - a `xerj` binary reachable via $XERJ_BIN (a build from
#     https://github.com/xerj-org/xerj with wal_tap support)
#   - a target ES-compatible cluster already running and reachable at
#     $TARGET_URL (start one yourself — e.g. `docker run ... elasticsearch`
#     or `docker run ... opensearch`; this script does not manage
#     containers, since the target is meant to be whatever you're actually
#     testing against)
#   - `xerj-seed` built at ../target/release/xerj-seed (cargo build --release)
#
# Usage:
#   XERJ_BIN=/path/to/xerj TARGET_URL=http://127.0.0.1:9201 \
#     [TARGET_AUTH="Basic ..."] ./scripts/e2e-smoke-test.sh

set -euo pipefail

XERJ_BIN="${XERJ_BIN:?set XERJ_BIN to a xerj binary with wal_tap support}"
TARGET_URL="${TARGET_URL:?set TARGET_URL to a running ES-compatible target}"
TARGET_AUTH="${TARGET_AUTH:-}"
SOURCE_PORT="${SOURCE_PORT:-9250}"
SOURCE_URL="http://127.0.0.1:${SOURCE_PORT}"
INDEX="${INDEX:-smoke-test}"
DATA_DIR="$(mktemp -d)"
SEED_JSON="$(mktemp)"
XERJ_SEED_BIN="$(cd "$(dirname "$0")/.." && pwd)/target/release/xerj-seed"

cleanup() {
  [ -n "${XERJ_PID:-}" ] && kill "$XERJ_PID" 2>/dev/null || true
  rm -rf "$DATA_DIR" "$SEED_JSON"
}
trap cleanup EXIT

echo "== starting source xerj node on :$SOURCE_PORT =="
"$XERJ_BIN" --insecure --port "$SOURCE_PORT" -d "$DATA_DIR" --disable-feedback \
  >"$DATA_DIR/xerj.log" 2>&1 &
XERJ_PID=$!
for _ in $(seq 1 30); do
  curl -sf "$SOURCE_URL/" >/dev/null 2>&1 && break
  sleep 1
done
curl -sf "$SOURCE_URL/" >/dev/null || { echo "source node did not come up"; cat "$DATA_DIR/xerj.log"; exit 1; }

echo "== seeding $INDEX with 110 documents =="
python3 - "$INDEX" > "$SEED_JSON" <<'EOF'
import json, sys
index = sys.argv[1]
for i in range(110):
    print(json.dumps({"index": {"_index": index, "_id": f"doc-{i}"}}))
    print(json.dumps({"n": i, "msg": f"hello {i}"}))
EOF
curl -sf -X POST "$SOURCE_URL/_bulk" -H 'Content-Type: application/x-ndjson' \
  --data-binary @"$SEED_JSON" | python3 -c "import json,sys; d=json.load(sys.stdin); assert not d['errors']; print('seeded ok')"

echo "== running xerj-seed =="
AUTH_ARGS=()
[ -n "$TARGET_AUTH" ] && AUTH_ARGS=(--target-auth "$TARGET_AUTH")
"$XERJ_SEED_BIN" \
  --source-url "$SOURCE_URL" \
  --source-index "$INDEX" \
  --target-url "$TARGET_URL" \
  "${AUTH_ARGS[@]}" \
  --batch-size 40 \
  --enable-sync-after

echo "== (a) verifying document count on target =="
TARGET_COUNT=$(curl -sf ${TARGET_AUTH:+-H "Authorization: $TARGET_AUTH"} "$TARGET_URL/$INDEX/_count" | python3 -c "import json,sys; print(json.load(sys.stdin)['count'])")
[ "$TARGET_COUNT" = "110" ] || { echo "FAIL: target has $TARGET_COUNT docs, expected 110"; exit 1; }
echo "OK: target has 110 documents"

echo "== (b) verifying wal_tap is enabled on the source =="
ENABLED=$(curl -sf "$SOURCE_URL/_xerj/wal_tap" | python3 -c "import json,sys; print(json.load(sys.stdin)['enabled'])")
[ "$ENABLED" = "True" ] || { echo "FAIL: wal_tap.enabled=$ENABLED"; exit 1; }
echo "OK: wal_tap enabled on source"

echo "== (c) verifying a post-run write propagates =="
curl -sf -X POST "$SOURCE_URL/$INDEX/_doc/post-run-doc" -H 'Content-Type: application/json' \
  -d '{"n": 9999, "msg": "written after wal_tap was armed"}' >/dev/null
sleep 2
FOUND=$(curl -s -o /dev/null -w "%{http_code}" ${TARGET_AUTH:+-H "Authorization: $TARGET_AUTH"} "$TARGET_URL/$INDEX/_doc/post-run-doc")
[ "$FOUND" = "200" ] || { echo "FAIL: post-run doc not found on target (HTTP $FOUND)"; exit 1; }
echo "OK: post-run write propagated via wal_tap"

echo "== all checks passed =="
