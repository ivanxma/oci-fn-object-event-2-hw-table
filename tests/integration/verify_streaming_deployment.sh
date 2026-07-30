#!/usr/bin/env bash
# Redacted preflight verification for the stream processor deployment.
set -euo pipefail
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
[[ -r "$ENV_FILE" ]] || { echo "Missing $ENV_FILE" >&2; exit 1; }
set -a; . "$ENV_FILE"; set +a
source "$ROOT_DIR/deploy/oci_context.sh"
oci_context_resolve
PROCESSOR_SHAPE="${PROCESSOR_SHAPE:-CI.Standard.E4.Flex}"
PROCESSOR_OCPUS="${PROCESSOR_OCPUS:-1}"
PROCESSOR_MEMORY_GBS="${PROCESSOR_MEMORY_GBS:-16}"
for value in COMPARTMENT_ID REGION SUBNET_ID DB_SECRET_OCID PROCESSOR_IMAGE_TAG PROCESSOR_SHAPE PROCESSOR_OCPUS PROCESSOR_MEMORY_GBS; do
  [[ -n "${!value:-}" ]] || { echo "FAIL: $value is required" >&2; exit 1; }
done
[[ "$(stat -c '%a' "$ENV_FILE" 2>/dev/null || stat -f '%Lp' "$ENV_FILE")" == 600 ]] || { echo "FAIL: env.sh must be 0600" >&2; exit 1; }

# Oracle Linux 9 may retain Python 3.9 as `python3` while the supported
# Connector/Python RPM is installed for a newer interpreter.  Select the
# interpreter that can import the driver; callers may override PYTHON_BIN.
if [[ -n "${PYTHON_BIN:-}" ]]; then
  command -v "$PYTHON_BIN" >/dev/null || { echo "FAIL: PYTHON_BIN is not executable: $PYTHON_BIN" >&2; exit 1; }
elif [[ -x "$ROOT_DIR/.venv-verification-py312/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv-verification-py312/bin/python"
else
  for candidate in python3.13 python3.12 python3; do
    if command -v "$candidate" >/dev/null && "$candidate" -c 'import mysql.connector' >/dev/null 2>&1; then
      PYTHON_BIN="$candidate"
      break
    fi
  done
fi
[[ -n "${PYTHON_BIN:-}" ]] || { echo "FAIL: no Python interpreter with mysql.connector is available" >&2; exit 1; }
"$PYTHON_BIN" -m py_compile "$ROOT_DIR/processor/stream_processor.py" "$ROOT_DIR/processor/message_store.py"
echo "PASS: verification Python: $($PYTHON_BIN --version 2>&1)"
echo "PASS: redacted stream processor deployment preflight"
