#!/usr/bin/env bash
# Run the disposable two-partition OCI flow using the OL9 verifier environment.
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv-verification-py312/bin/python}"

[[ -r "$ENV_FILE" ]] || { echo "Missing $ENV_FILE" >&2; exit 1; }
[[ -x "$PYTHON_BIN" ]] || { echo "Missing verification Python: $PYTHON_BIN" >&2; exit 1; }
set -a; . "$ENV_FILE"; set +a
export OCI_AUTH_MODE=instance_principal
exec "$PYTHON_BIN" "$ROOT_DIR/tests/integration/verify_parallel_flow.py"
