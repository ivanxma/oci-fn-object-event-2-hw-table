#!/usr/bin/env bash
# Runs the bounded durable-capture integration check using the deployment env.
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv-verification-py312/bin/python}"

[[ -r "$ENV_FILE" ]] || { echo "Missing $ENV_FILE" >&2; exit 1; }
[[ -x "$PYTHON_BIN" ]] || {
  echo "FAIL: verification Python not found: $PYTHON_BIN" >&2
  echo "Create a project-local Python 3.12 environment with the official Connector/Python 9.7 OL9 RPM and OCI SDK." >&2
  exit 1
}
set -a; . "$ENV_FILE"; set +a
[[ -n "${DB_SECRET_OCID:-}" ]] || { echo "FAIL: DB_SECRET_OCID is required" >&2; exit 1; }
"$PYTHON_BIN" -c 'import mysql.connector, oci' || {
  echo "FAIL: verification Python must provide mysql.connector and oci" >&2
  exit 1
}
cd "$ROOT_DIR/consumer"
OCI_AUTH_MODE="${OCI_AUTH_MODE:-instance_principal}" exec "$PYTHON_BIN" verify_capture_store.py
