#!/usr/bin/env bash
# Redacted preflight verification for the stream consumer deployment.
set -euo pipefail
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
[[ -r "$ENV_FILE" ]] || { echo "Missing $ENV_FILE" >&2; exit 1; }
set -a; . "$ENV_FILE"; set +a
for value in COMPARTMENT_ID REGION SUBNET_ID DB_SECRET_OCID CONSUMER_IMAGE_TAG CONSUMER_SHAPE CONSUMER_OCPUS CONSUMER_MEMORY_GBS; do
  [[ -n "${!value:-}" ]] || { echo "FAIL: $value is required" >&2; exit 1; }
done
[[ "$(stat -c '%a' "$ENV_FILE" 2>/dev/null || stat -f '%Lp' "$ENV_FILE")" == 600 ]] || { echo "FAIL: env.sh must be 0600" >&2; exit 1; }
python3 -m py_compile "$ROOT_DIR/consumer/stream_consumer.py" "$ROOT_DIR/consumer/message_store.py"
echo "PASS: redacted stream consumer deployment preflight"
