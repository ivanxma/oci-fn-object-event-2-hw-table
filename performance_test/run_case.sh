#!/usr/bin/env bash
# Generate, upload, await, delete, and record one measured ingestion case.
set -euo pipefail
umask 077
export PATH="$HOME/.fn/bin:$HOME/bin:$HOME/.local/bin:$PATH"
[[ $# -eq 4 ]] || { echo "Usage: $0 LABEL TARGET_BYTES SYNC|DETACHED RESULTS_JSONL" >&2; exit 2; }
CASE_LABEL=$1
TARGET_BYTES=$2
INVOCATION_MODE=${3^^}
RESULTS_JSONL=$4
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
[[ -r "$ENV_FILE" ]] || { echo "Deployment environment is not readable: $ENV_FILE" >&2; exit 1; }
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a
[[ "$TARGET_BYTES" =~ ^[0-9]+$ ]] && (( TARGET_BYTES > 0 )) || { echo "TARGET_BYTES must be a positive integer." >&2; exit 2; }
[[ "$INVOCATION_MODE" == SYNC || "$INVOCATION_MODE" == DETACHED ]] || { echo "Mode must be SYNC or DETACHED." >&2; exit 2; }
for value in DB_HOST DB_USER DB_PASSWORD CONTROL_DATABASE OBJECT_STORAGE_BUCKET_NAME UI_SERVICE_NAME; do
  [[ -n "${!value:-}" ]] || { echo "Missing $value in $ENV_FILE" >&2; exit 1; }
done
for command in oci jq podman sudo awk; do
  command -v "$command" >/dev/null 2>&1 || { echo "Missing required command: $command" >&2; exit 1; }
done
sudo podman image exists "$UI_SERVICE_NAME:latest" || {
  echo "UI image $UI_SERVICE_NAME:latest is missing. Run deploy/deploy_ui.sh first." >&2
  exit 1
}
TARGET_DATABASE="${PERF_TARGET_DATABASE:-fntestdb}"
TARGET_TABLE="${PERF_TARGET_TABLE:-perf_t_001}"
COMPARTMENT_NAME="${PERF_COMPARTMENT_NAME:-HWDemo}"
BUCKET_NAME="$OBJECT_STORAGE_BUCKET_NAME"
if [[ "$FUNCTION_NAME" =~ ([0-9]+)$ ]]; then DEPLOY_SUFFIX="${PERF_DEPLOY_SUFFIX:-${BASH_REMATCH[1]}}"; else DEPLOY_SUFFIX="${PERF_DEPLOY_SUFFIX:-local}"; fi
TEST_PREFIX="${PERF_TEST_PREFIX:-performance/vm${DEPLOY_SUFFIX}/$TARGET_TABLE}"
RESOURCE_NAME_PATTERN="$TEST_PREFIX/*.csv"
RUN_ID="${PERF_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OBJECT_NAME="$TEST_PREFIX/${RUN_ID}-${CASE_LABEL,,}-${INVOCATION_MODE,,}.csv"
DATA_DIR="${PERF_DATA_DIR:-/home/opc/performance-test-data}"
DATA_FILE="$DATA_DIR/current.csv"
META_FILE="$DATA_DIR/current.json"
PAYLOAD_BYTES="${PERF_PAYLOAD_BYTES:-480}"
mkdir -p "$DATA_DIR" "$(dirname "$RESULTS_JSONL")"
cleanup() { rm -f "$DATA_FILE" "$META_FILE"; }
trap cleanup EXIT
export TARGET_DATABASE TARGET_TABLE COMPARTMENT_NAME BUCKET_NAME TEST_PREFIX RESOURCE_NAME_PATTERN INVOCATION_MODE OBJECT_NAME CASE_LABEL PAYLOAD_BYTES

container_python() {
  local script=$1
  shift
  sudo --preserve-env=DB_HOST,DB_PORT,DB_USER,DB_PASSWORD,CONTROL_DATABASE,TARGET_DATABASE,TARGET_TABLE,COMPARTMENT_NAME,BUCKET_NAME,RESOURCE_NAME_PATTERN,INVOCATION_MODE,WRITER_WORKERS,OBJECT_NAME,EXPECTED_ACTION,EXPECTED_ROWS,EVENT_WAIT_SECONDS,FILE_BYTES,CASE_LABEL,PAYLOAD_BYTES,GENERATION_SECONDS,UPLOAD_SECONDS \
    podman run --security-opt label=disable --rm --user 0 --network host \
    -e DB_HOST -e DB_PORT -e DB_USER -e DB_PASSWORD -e CONTROL_DATABASE \
    -e TARGET_DATABASE -e TARGET_TABLE -e COMPARTMENT_NAME -e BUCKET_NAME \
    -e RESOURCE_NAME_PATTERN -e INVOCATION_MODE -e WRITER_WORKERS -e OBJECT_NAME -e EXPECTED_ACTION \
    -e EXPECTED_ROWS -e EVENT_WAIT_SECONDS -e FILE_BYTES -e CASE_LABEL \
    -e PAYLOAD_BYTES -e GENERATION_SECONDS -e UPLOAD_SECONDS \
    -v "$ROOT_DIR/performance_test/$script:/opt/$script:ro" \
    "$UI_SERVICE_NAME:latest" python "/opt/$script" "$@"
}

container_python set_mode.py
rm -f "$DATA_FILE" "$META_FILE"
generation_start=$(date +%s.%N)
sudo podman run --security-opt label=disable --rm --user 0 \
  -v "$ROOT_DIR/performance_test/generate_csv.py:/opt/generate_csv.py:ro" \
  -v "$DATA_DIR:/data" \
  "$UI_SERVICE_NAME:latest" python /opt/generate_csv.py --target-bytes "$TARGET_BYTES" --payload-bytes "$PAYLOAD_BYTES" --output /data/current.csv --metadata /data/current.json
generation_end=$(date +%s.%N)
GENERATION_SECONDS=$(awk -v s="$generation_start" -v e="$generation_end" 'BEGIN {printf "%.6f", e-s}')
EXPECTED_ROWS=$(jq -r .rows "$META_FILE")
FILE_BYTES=$(jq -r .bytes "$META_FILE")
export GENERATION_SECONDS EXPECTED_ROWS FILE_BYTES
echo "CASE=$CASE_LABEL MODE=$INVOCATION_MODE BYTES=$FILE_BYTES ROWS=$EXPECTED_ROWS"

upload_start=$(date +%s.%N)
oci --auth instance_principal os object put --bucket-name "$BUCKET_NAME" --name "$OBJECT_NAME" --file "$DATA_FILE" --force >/dev/null
upload_end=$(date +%s.%N)
UPLOAD_SECONDS=$(awk -v s="$upload_start" -v e="$upload_end" 'BEGIN {printf "%.6f", e-s}')
export UPLOAD_SECONDS EXPECTED_ACTION=CREATE
EVENT_WAIT_SECONDS="${EVENT_WAIT_SECONDS:-$([[ "$INVOCATION_MODE" == DETACHED ]] && echo 3700 || echo 420)}"
export EVENT_WAIT_SECONDS
echo "UPLOAD_SECONDS=$UPLOAD_SECONDS OBJECT=$OBJECT_NAME"
container_python wait_event.py

oci --auth instance_principal os object delete --bucket-name "$BUCKET_NAME" --name "$OBJECT_NAME" --force
export EXPECTED_ACTION=DELETE EXPECTED_ROWS=0 EVENT_WAIT_SECONDS=420
container_python wait_event.py
export EXPECTED_ROWS
EXPECTED_ROWS=$(jq -r .rows "$META_FILE")
export EXPECTED_ROWS
container_python collect_result.py | tee -a "$RESULTS_JSONL"
echo "CASE_COMPLETE=$CASE_LABEL"
