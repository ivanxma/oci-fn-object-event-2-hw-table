#!/usr/bin/env bash
# Run the disposable FIFO flow using the OL9 verifier environment.
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv-verification-py312/bin/python}"

[[ -r "$ENV_FILE" ]] || { echo "Missing $ENV_FILE" >&2; exit 1; }
[[ -x "$PYTHON_BIN" ]] || { echo "Missing verification Python: $PYTHON_BIN" >&2; exit 1; }
set -a; . "$ENV_FILE"; set +a
if [[ -n "${VALIDATION_ENV_FILE:-}" ]]; then
  [[ -r "$VALIDATION_ENV_FILE" ]] || { echo "Missing validation overlay: $VALIDATION_ENV_FILE" >&2; exit 1; }
  set -a; . "$VALIDATION_ENV_FILE"; set +a
fi
export OCI_AUTH_MODE=instance_principal
export FLOW_MODE=FIFO
export FLOW_TARGET_DATABASE="${FLOW_TARGET_DATABASE:-${FIFO_TARGET_DATABASE:-}}"
exec "$PYTHON_BIN" "$ROOT_DIR/tests/integration/verify_flow.py"
