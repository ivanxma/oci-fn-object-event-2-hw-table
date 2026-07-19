#!/usr/bin/env bash
# Idempotently prepare an empty database/UI environment for performance tests.
set -euo pipefail
umask 077

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
RESET=false
SMOKE_TEST=false
while (($#)); do
  case "$1" in
    --reset) RESET=true ;;
    --smoke-test) SMOKE_TEST=true ;;
    *) echo "Usage: $0 [--reset] [--smoke-test]" >&2; exit 2 ;;
  esac
  shift
done
[[ -r "$ENV_FILE" ]] || { echo "Deployment environment is not readable: $ENV_FILE" >&2; exit 1; }
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

for value in COMPARTMENT_ID APP_NAME FUNCTION_NAME RULE_NAME REGION DB_HOST DB_USER DB_PASSWORD CONTROL_DATABASE OBJECT_STORAGE_BUCKET_NAME UI_SERVICE_NAME UI_CONTAINER_NAME; do
  [[ -n "${!value:-}" ]] || { echo "Missing $value in $ENV_FILE" >&2; exit 1; }
done
for command in oci jq podman sudo; do
  command -v "$command" >/dev/null || { echo "Missing required command: $command" >&2; exit 1; }
done

TARGET_DATABASE="${PERF_TARGET_DATABASE:-fntestdb}"
TARGET_TABLE="${PERF_TARGET_TABLE:-perf_t_001}"
if [[ "$FUNCTION_NAME" =~ ([0-9]+)$ ]]; then
  DEPLOY_SUFFIX="${PERF_DEPLOY_SUFFIX:-${BASH_REMATCH[1]}}"
else
  DEPLOY_SUFFIX="${PERF_DEPLOY_SUFFIX:-local}"
fi
TEST_PREFIX="${PERF_TEST_PREFIX:-performance/vm${DEPLOY_SUFFIX}/$TARGET_TABLE}"
RESOURCE_NAME_PATTERN="$TEST_PREFIX/*.csv"
COMPARTMENT_NAME="${PERF_COMPARTMENT_NAME:-HWDemo}"
INVOCATION_MODE="${PERF_INVOCATION_MODE:-SYNC}"
PROFILE_NAME="${PERF_PROFILE_NAME:-HWDemo ${DEPLOY_SUFFIX}}"
export TARGET_DATABASE TARGET_TABLE TEST_PREFIX RESOURCE_NAME_PATTERN COMPARTMENT_NAME INVOCATION_MODE PROFILE_NAME

OCI=(oci --auth instance_principal)
APP_ID=$("${OCI[@]}" fn application list --compartment-id "$COMPARTMENT_ID" --all \
  --query "data[?\"display-name\"=='$APP_NAME'].id | [0]" --raw-output)
[[ -n "$APP_ID" && "$APP_ID" != null ]] || { echo "Function application not found: $APP_NAME" >&2; exit 1; }
FUNCTION_ID=$("${OCI[@]}" fn function list --application-id "$APP_ID" --all \
  --query "data[?\"display-name\"=='$FUNCTION_NAME'].id | [0]" --raw-output)
[[ -n "$FUNCTION_ID" && "$FUNCTION_ID" != null ]] || { echo "Function not found: $FUNCTION_NAME" >&2; exit 1; }
RULE_JSON=$(mktemp)
RULE_DETAIL=$(mktemp)
trap 'rm -f "$RULE_JSON" "$RULE_DETAIL" "${SMOKE_FILE:-}"' EXIT
"${OCI[@]}" events rule list --compartment-id "$COMPARTMENT_ID" --all > "$RULE_JSON"
EVENT_RULE_ID=$(jq -r --arg rule "$RULE_NAME" '.data[] | select(."display-name" == $rule) | .id' "$RULE_JSON" | head -n 1)
[[ -n "$EVENT_RULE_ID" && "$EVENT_RULE_ID" != null ]] || { echo "OCI Events rule not found: $RULE_NAME" >&2; exit 1; }
"${OCI[@]}" events rule get --rule-id "$EVENT_RULE_ID" > "$RULE_DETAIL"
RULE_FUNCTION_ID=$(jq -r '.data.actions.actions[] | ."function-id" // .functionId' "$RULE_DETAIL" | head -n 1)
[[ "$RULE_FUNCTION_ID" == "$FUNCTION_ID" ]] || { echo "Rule $RULE_NAME does not target Function $FUNCTION_NAME." >&2; exit 1; }
RULE_CONDITION=$(jq -r '.data.condition' "$RULE_DETAIL")
jq -e --arg bucket "$OBJECT_STORAGE_BUCKET_NAME" --arg pattern "$TEST_PREFIX/*" \
  '.data.additionalDetails.bucketName == $bucket and .data.resourceName == $pattern' <<<"$RULE_CONDITION" >/dev/null || {
  echo "Rule $RULE_NAME does not match bucket $OBJECT_STORAGE_BUCKET_NAME and prefix $TEST_PREFIX/*." >&2
  exit 1
}
export EVENT_RULE_ID BUCKET_NAME="$OBJECT_STORAGE_BUCKET_NAME"

sudo podman exec --user 10001 \
  -e PROFILE_NAME="$PROFILE_NAME" -e PROFILE_HOST="$DB_HOST" -e PROFILE_PORT="${DB_PORT:-3306}" -e PROFILE_DATABASE="$CONTROL_DATABASE" \
  "$UI_CONTAINER_NAME" python -c 'import os; from pathlib import Path; from myapp.services.profile_store import ProfileStore; store=ProfileStore(Path("/app/instance/profiles.json"),Path("/app/instance/keys")); name=os.environ["PROFILE_NAME"]; store.save({"name":name,"mode":"direct","host":os.environ["PROFILE_HOST"],"port":os.environ["PROFILE_PORT"],"database":os.environ["PROFILE_DATABASE"]},original_name=name)'
echo "UI profile ready: $PROFILE_NAME (no password stored)"

DB_SETUP_ARGS=()
[[ "$RESET" == true ]] && DB_SETUP_ARGS+=(--reset)
sudo --preserve-env=DB_HOST,DB_PORT,DB_USER,DB_PASSWORD,CONTROL_DATABASE,TARGET_DATABASE,TARGET_TABLE,RESOURCE_NAME_PATTERN,COMPARTMENT_NAME,BUCKET_NAME,INVOCATION_MODE,WRITER_WORKERS,EVENT_RULE_ID \
  podman run --rm --user 0 --network host \
  -e DB_HOST -e DB_PORT -e DB_USER -e DB_PASSWORD -e CONTROL_DATABASE \
  -e TARGET_DATABASE -e TARGET_TABLE -e RESOURCE_NAME_PATTERN -e COMPARTMENT_NAME \
  -e BUCKET_NAME -e INVOCATION_MODE -e WRITER_WORKERS -e EVENT_RULE_ID \
  -v "$ROOT_DIR/performance_test/setup_db.py:/opt/setup_db.py:ro,Z" \
  "$UI_SERVICE_NAME:latest" python /opt/setup_db.py "${DB_SETUP_ARGS[@]}"

if [[ "$SMOKE_TEST" == true ]]; then
  SMOKE_FILE=$(mktemp --suffix=.csv)
  sudo podman run --rm --user 0 \
    -v "$ROOT_DIR/performance_test/generate_csv.py:/opt/generate_csv.py:ro,Z" \
    -v "$SMOKE_FILE:/output.csv:Z" \
    "$UI_SERVICE_NAME:latest" python /opt/generate_csv.py --rows 100 --output /output.csv
  OBJECT_NAME="$TEST_PREFIX/setup-smoke-$(date -u +%Y%m%dT%H%M%SZ).csv"
  "${OCI[@]}" os object put --bucket-name "$OBJECT_STORAGE_BUCKET_NAME" --name "$OBJECT_NAME" --file "$SMOKE_FILE" --force >/dev/null
  echo "Smoke object uploaded: $OBJECT_NAME"
  export OBJECT_NAME EXPECTED_ACTION=CREATE EXPECTED_ROWS=100 EVENT_WAIT_SECONDS="${EVENT_WAIT_SECONDS:-360}"
  sudo --preserve-env=DB_HOST,DB_PORT,DB_USER,DB_PASSWORD,CONTROL_DATABASE,TARGET_DATABASE,TARGET_TABLE,OBJECT_NAME,EXPECTED_ACTION,EXPECTED_ROWS,EVENT_WAIT_SECONDS \
    podman run --rm --user 0 --network host \
    -e DB_HOST -e DB_PORT -e DB_USER -e DB_PASSWORD -e CONTROL_DATABASE \
    -e TARGET_DATABASE -e TARGET_TABLE -e OBJECT_NAME -e EXPECTED_ACTION -e EXPECTED_ROWS -e EVENT_WAIT_SECONDS \
    -v "$ROOT_DIR/performance_test/wait_event.py:/opt/wait_event.py:ro,Z" \
    "$UI_SERVICE_NAME:latest" python /opt/wait_event.py
  "${OCI[@]}" os object delete --bucket-name "$OBJECT_STORAGE_BUCKET_NAME" --name "$OBJECT_NAME" --force
  export EXPECTED_ACTION=DELETE EXPECTED_ROWS=0
  sudo --preserve-env=DB_HOST,DB_PORT,DB_USER,DB_PASSWORD,CONTROL_DATABASE,TARGET_DATABASE,TARGET_TABLE,OBJECT_NAME,EXPECTED_ACTION,EXPECTED_ROWS,EVENT_WAIT_SECONDS \
    podman run --rm --user 0 --network host \
    -e DB_HOST -e DB_PORT -e DB_USER -e DB_PASSWORD -e CONTROL_DATABASE \
    -e TARGET_DATABASE -e TARGET_TABLE -e OBJECT_NAME -e EXPECTED_ACTION -e EXPECTED_ROWS -e EVENT_WAIT_SECONDS \
    -v "$ROOT_DIR/performance_test/wait_event.py:/opt/wait_event.py:ro,Z" \
    "$UI_SERVICE_NAME:latest" python /opt/wait_event.py
fi

echo "Performance setup complete: $OBJECT_STORAGE_BUCKET_NAME/$RESOURCE_NAME_PATTERN -> $TARGET_DATABASE.$TARGET_TABLE ($INVOCATION_MODE)"
