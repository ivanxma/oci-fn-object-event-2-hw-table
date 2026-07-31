#!/usr/bin/env bash
# Launch the complete FIFO performance campaign without interactive prompts.
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv-verification-py312/bin/python}"
RUN_ID="${PERF_RUN_ID:-$(date -u +%Y%m%d-%H%M%S)}"

[[ -r "$ENV_FILE" ]] || { echo "Missing $ENV_FILE" >&2; exit 1; }
[[ -x "$PYTHON_BIN" ]] || { echo "Missing verification Python: $PYTHON_BIN" >&2; exit 1; }

set -a
. "$ENV_FILE"
set +a
source "$ROOT_DIR/deploy/oci_context.sh"
oci_context_resolve

export OCI_AUTH_MODE=instance_principal
export PROCESSOR_SHAPE="${PROCESSOR_SHAPE:-CI.Standard.E4.Flex}"
export PROCESSOR_OCPUS="${PROCESSOR_OCPUS:-1}"
export PROCESSOR_MEMORY_GBS="${PROCESSOR_MEMORY_GBS:-16}"
export WRITER_WORKERS="${WRITER_WORKERS:-4}"

WORK_DIR="${PERF_WORK_DIR:-/tmp/oci-stream-fifo-performance-$RUN_ID}"
METRICS="${PERF_METRICS_JSON:-$ROOT_DIR/report/fifo-performance-$RUN_ID.json}"
REPORT="${PERF_REPORT_HTML:-$ROOT_DIR/report/fifo-performance-$RUN_ID.html}"

mkdir -p "$WORK_DIR" "$ROOT_DIR/report"
exec "$PYTHON_BIN" "$ROOT_DIR/tests/performance/run_fifo_campaign.py" \
  --run-id "$RUN_ID" \
  --work-dir "$WORK_DIR" \
  --metrics "$METRICS" \
  --report "$REPORT"
