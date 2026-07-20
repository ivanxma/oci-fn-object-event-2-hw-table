#!/usr/bin/env bash
# Idempotently deploy the OCI Function and its Object Storage Events rule.
set -euo pipefail
umask 077

# OCI CLI and Fn installers use user-local paths on a fresh VM. Set PATH before
# any prerequisite checks so deployment works in non-interactive SSH/systemd
# shells without requiring the operator to source .bashrc.
export PATH="$HOME/.fn/bin:$HOME/bin:$HOME/.local/bin:$PATH"

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
[[ -r "$ENV_FILE" ]] || { echo "Copy deploy/env.sh.example to deploy/env.sh and set deployment values." >&2; exit 1; }
chmod 600 "$ENV_FILE"
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a
for value in COMPARTMENT_ID SUBNET_ID APP_NAME FUNCTION_NAME REGION REGION_KEY REPOSITORY_PREFIX OCIR_USERNAME OCIR_AUTH_TOKEN DB_HOST DB_USER DB_PASSWORD; do
  [[ -n "${!value:-}" ]] || { echo "Missing $value in $ENV_FILE" >&2; exit 1; }
done
# OCIR repository paths are strictly lowercase.  Normalize the configurable
# prefix up front so deployments copied from environments such as "HWDemo"
# do not fail with OCI NameInvalid; preserve the function/app names as given.
NORMALIZED_REPOSITORY_PREFIX=$(printf '%s' "$REPOSITORY_PREFIX" | tr '[:upper:]' '[:lower:]')
if [[ "$NORMALIZED_REPOSITORY_PREFIX" != "$REPOSITORY_PREFIX" ]]; then
  echo "Normalizing REPOSITORY_PREFIX to lowercase: $REPOSITORY_PREFIX -> $NORMALIZED_REPOSITORY_PREFIX" >&2
  REPOSITORY_PREFIX="$NORMALIZED_REPOSITORY_PREFIX"
fi
[[ "$REPOSITORY_PREFIX" =~ ^[a-z0-9][a-z0-9._-]*$ ]] || {
  echo "REPOSITORY_PREFIX must contain only lowercase letters, digits, '.', '_' or '-' and start with a letter/digit." >&2
  exit 1
}
CONTROL_DATABASE="${CONTROL_DATABASE:-${DB_NAME:-fndb}}"
DB_PORT="${DB_PORT:-3306}"
for command in oci fn podman jq; do command -v "$command" >/dev/null || { echo "Missing $command; run deploy/bootstrap.sh first." >&2; exit 1; }; done

require_integer() {
  local name=$1 minimum=$2 maximum=$3 value=${!1:-}
  [[ "$value" =~ ^[0-9]+$ ]] || { echo "$name must be numeric." >&2; exit 1; }
  (( value >= minimum && value <= maximum )) || { echo "$name must be from $minimum to $maximum." >&2; exit 1; }
}

require_decimal() {
  local name=$1 minimum=$2 maximum=$3 value=${!1:-}
  jq -en --arg value "$value" --argjson minimum "$minimum" --argjson maximum "$maximum" \
    '($value | tonumber) as $number | ($number >= $minimum and $number <= $maximum)' >/dev/null 2>&1 || {
    echo "$name must be a number from $minimum to $maximum." >&2
    exit 1
  }
}

require_integer DB_PORT 1 65535

OCI=(oci --auth instance_principal)
"${OCI[@]}" fn function update --help | grep -- '--detached-mode-timeout-in-seconds' >/dev/null || {
  echo "OCI CLI is too old to deploy detached Function timeouts; update OCI CLI and retry." >&2
  exit 1
}
NAMESPACE=$("${OCI[@]}" os ns get --query data --raw-output)
if [[ -n "${OBJECT_STORAGE_BUCKET_NAME:-}" && "${ENSURE_BUCKET_OBJECT_EVENTS:-true}" == "true" ]]; then
  BUCKET_EVENTS=$("${OCI[@]}" os bucket get --namespace-name "$NAMESPACE" --name "$OBJECT_STORAGE_BUCKET_NAME" \
    --query 'data."object-events-enabled"' --raw-output)
  if [[ "$BUCKET_EVENTS" != "true" ]]; then
    echo "Enabling Object Storage events on bucket $OBJECT_STORAGE_BUCKET_NAME."
    "${OCI[@]}" os bucket update --namespace-name "$NAMESPACE" --name "$OBJECT_STORAGE_BUCKET_NAME" \
      --object-events-enabled true --force >/dev/null
  fi
fi
REPOSITORY_NAME="$REPOSITORY_PREFIX/$FUNCTION_NAME"
REPOSITORY_ID=$("${OCI[@]}" artifacts container repository list --compartment-id "$COMPARTMENT_ID" --all --query "data.items[?\"display-name\"=='$REPOSITORY_NAME'].id | [0]" --raw-output)
if [[ -z "$REPOSITORY_ID" || "$REPOSITORY_ID" == null ]]; then
  "${OCI[@]}" artifacts container repository create --compartment-id "$COMPARTMENT_ID" --display-name "$REPOSITORY_NAME" --is-public false >/dev/null
fi
APP_ID=$("${OCI[@]}" fn application list --compartment-id "$COMPARTMENT_ID" --all --query "data[?\"display-name\"=='$APP_NAME'].id | [0]" --raw-output)
if [[ -z "$APP_ID" || "$APP_ID" == null ]]; then
  APP_ID=$("${OCI[@]}" fn application create --compartment-id "$COMPARTMENT_ID" --display-name "$APP_NAME" --subnet-ids "[\"$SUBNET_ID\"]" --query 'data.id' --raw-output)
fi

fn create context "$REGION" --provider oracle-ip 2>/dev/null || true
fn use context "$REGION" 2>/dev/null || true
fn update context oracle.compartment-id "$COMPARTMENT_ID"
fn update context api-url "https://functions.$REGION.oci.oraclecloud.com"
fn update context registry "$REGION_KEY.ocir.io/$NAMESPACE/$REPOSITORY_PREFIX"
printf '%s' "$OCIR_AUTH_TOKEN" | podman login "$REGION_KEY.ocir.io" --username "$OCIR_USERNAME" --password-stdin

BUILD_DIR=$(mktemp -d)
CONFIG_FILE=$(mktemp)
LOG_CONFIG=""
cleanup() { rm -rf "$BUILD_DIR" "$CONFIG_FILE" "${LOG_CONFIG:-}"; }
trap cleanup EXIT
FUNCTION_MEMORY="${FUNCTION_MEMORY:-1024}"
FUNCTION_TIMEOUT="${FUNCTION_TIMEOUT:-300}"
DETACHED_TIMEOUT_SECONDS="${DETACHED_TIMEOUT_SECONDS:-3600}"
BATCH_ROWS="${BATCH_ROWS:-10000}"
WRITER_WORKERS="${WRITER_WORKERS:-4}"
LOAD_LEASE_SECONDS="${LOAD_LEASE_SECONDS:-120}"
OBJECT_STORAGE_READ_TIMEOUT_SECONDS="${OBJECT_STORAGE_READ_TIMEOUT_SECONDS:-300}"
OBJECT_STORAGE_RANGE_BYTES="${OBJECT_STORAGE_RANGE_BYTES:-33554432}"
QUEUE_LEASE_SECONDS="${QUEUE_LEASE_SECONDS:-90}"
QUEUE_REORDER_GRACE_SECONDS="${QUEUE_REORDER_GRACE_SECONDS:-30}"
QUEUE_SYNC_RESERVE_SECONDS="${QUEUE_SYNC_RESERVE_SECONDS:-15}"
QUEUE_SYNC_MINIMUM_START_SECONDS="${QUEUE_SYNC_MINIMUM_START_SECONDS:-15}"
QUEUE_SHUTDOWN_RESERVE_SECONDS="${QUEUE_SHUTDOWN_RESERVE_SECONDS:-120}"
QUEUE_MINIMUM_START_SECONDS="${QUEUE_MINIMUM_START_SECONDS:-180}"
QUEUE_UNKNOWN_JOB_SECONDS="${QUEUE_UNKNOWN_JOB_SECONDS:-60}"
QUEUE_EXPECTED_BYTES_PER_SECOND="${QUEUE_EXPECTED_BYTES_PER_SECOND:-4194304}"
QUEUE_PREDICTION_SAFETY_FACTOR="${QUEUE_PREDICTION_SAFETY_FACTOR:-1.35}"
require_integer FUNCTION_MEMORY 128 3072
(( FUNCTION_MEMORY % 64 == 0 )) || { echo "FUNCTION_MEMORY must use a 64 MB increment." >&2; exit 1; }
require_integer FUNCTION_TIMEOUT 1 300
require_integer DETACHED_TIMEOUT_SECONDS 5 3600
require_integer BATCH_ROWS 1 1000000
require_integer WRITER_WORKERS 1 64
require_integer LOAD_LEASE_SECONDS 30 3600
require_integer OBJECT_STORAGE_READ_TIMEOUT_SECONDS 1 300
require_integer OBJECT_STORAGE_RANGE_BYTES 1048576 268435456
require_integer QUEUE_LEASE_SECONDS 30 3600
require_integer QUEUE_REORDER_GRACE_SECONDS 0 3600
require_integer QUEUE_SYNC_RESERVE_SECONDS 0 299
require_integer QUEUE_SYNC_MINIMUM_START_SECONDS 1 299
require_integer QUEUE_SHUTDOWN_RESERVE_SECONDS 0 1800
require_integer QUEUE_MINIMUM_START_SECONDS 1 1800
require_integer QUEUE_UNKNOWN_JOB_SECONDS 1 3600
require_integer QUEUE_EXPECTED_BYTES_PER_SECOND 1 1073741824
require_decimal QUEUE_PREDICTION_SAFETY_FACTOR 1 10
[[ "${DETACHED_ENABLED:-false}" == "true" || "${DETACHED_ENABLED:-false}" == "false" ]] || { echo "DETACHED_ENABLED must be true or false." >&2; exit 1; }
(( QUEUE_SYNC_RESERVE_SECONDS + QUEUE_SYNC_MINIMUM_START_SECONDS < FUNCTION_TIMEOUT )) || { echo "Sync queue reserve plus minimum start budget must be less than FUNCTION_TIMEOUT." >&2; exit 1; }
(( QUEUE_SHUTDOWN_RESERVE_SECONDS + QUEUE_MINIMUM_START_SECONDS < DETACHED_TIMEOUT_SECONDS )) || { echo "Detached queue reserve plus minimum start budget must be less than DETACHED_TIMEOUT_SECONDS." >&2; exit 1; }
cp "$ROOT_DIR/function/Dockerfile" "$ROOT_DIR/function/func.py" "$ROOT_DIR/function/partition_loader.py" "$ROOT_DIR/function/work_queue.py" "$ROOT_DIR/function/requirements.txt" "$BUILD_DIR/"
sed -e "s/^name:.*/name: $FUNCTION_NAME/" -e "s/^memory:.*/memory: $FUNCTION_MEMORY/" -e "s/^timeout:.*/timeout: $FUNCTION_TIMEOUT/" "$ROOT_DIR/function/func.yaml" > "$BUILD_DIR/func.yaml"
(cd "$BUILD_DIR" && fn deploy --app "$APP_NAME")
FUNCTION_ID=$("${OCI[@]}" fn function list --application-id "$APP_ID" --all --query "data[?\"display-name\"=='$FUNCTION_NAME'].id | [0]" --raw-output)
[[ -n "$FUNCTION_ID" && "$FUNCTION_ID" != null ]] || { echo "Function $FUNCTION_NAME was not found after deployment." >&2; exit 1; }
FUNCTION_INVOKE_ENDPOINT=$("${OCI[@]}" fn function get --function-id "$FUNCTION_ID" --query 'data."invoke-endpoint"' --raw-output)
[[ -n "$FUNCTION_INVOKE_ENDPOINT" && "$FUNCTION_INVOKE_ENDPOINT" != null ]] || { echo "Function invoke endpoint was not resolved." >&2; exit 1; }
jq -n --arg host "$DB_HOST" --arg port "${DB_PORT:-3306}" --arg user "$DB_USER" --arg password "$DB_PASSWORD" --arg ssl "${DB_SSL_DISABLED:-false}" --arg namespace "${OBJECT_STORAGE_NAMESPACE:-}" --arg batch "$BATCH_ROWS" --arg workers "$WRITER_WORKERS" --arg load_lease "$LOAD_LEASE_SECONDS" --arg read_timeout "$OBJECT_STORAGE_READ_TIMEOUT_SECONDS" --arg range_bytes "$OBJECT_STORAGE_RANGE_BYTES" --arg control "$CONTROL_DATABASE" --arg function_id "$FUNCTION_ID" --arg invoke_endpoint "$FUNCTION_INVOKE_ENDPOINT" --arg region "$REGION" --arg detached "${DETACHED_ENABLED:-false}" --arg detached_timeout "$DETACHED_TIMEOUT_SECONDS" --arg sync_timeout "$FUNCTION_TIMEOUT" --arg lease_seconds "$QUEUE_LEASE_SECONDS" --arg reorder_grace "$QUEUE_REORDER_GRACE_SECONDS" --arg sync_reserve "$QUEUE_SYNC_RESERVE_SECONDS" --arg sync_minimum "$QUEUE_SYNC_MINIMUM_START_SECONDS" --arg shutdown_reserve "$QUEUE_SHUTDOWN_RESERVE_SECONDS" --arg minimum_start "$QUEUE_MINIMUM_START_SECONDS" --arg unknown_job "$QUEUE_UNKNOWN_JOB_SECONDS" --arg expected_bps "$QUEUE_EXPECTED_BYTES_PER_SECOND" --arg safety_factor "$QUEUE_PREDICTION_SAFETY_FACTOR" '{DB_HOST:$host,DB_PORT:$port,DB_USER:$user,DB_PASSWORD:$password,DB_SSL_DISABLED:$ssl,OBJECT_STORAGE_NAMESPACE:$namespace,BATCH_ROWS:$batch,WRITER_WORKERS:$workers,LOAD_LEASE_SECONDS:$load_lease,OBJECT_STORAGE_READ_TIMEOUT_SECONDS:$read_timeout,OBJECT_STORAGE_RANGE_BYTES:$range_bytes,CONTROL_DATABASE:$control,FUNCTION_ID:$function_id,FUNCTION_INVOKE_ENDPOINT:$invoke_endpoint,OCI_REGION:$region,DETACHED_ENABLED:$detached,DETACHED_TIMEOUT_SECONDS:$detached_timeout,SYNC_TIMEOUT_SECONDS:$sync_timeout,QUEUE_LEASE_SECONDS:$lease_seconds,QUEUE_REORDER_GRACE_SECONDS:$reorder_grace,QUEUE_SYNC_RESERVE_SECONDS:$sync_reserve,QUEUE_SYNC_MINIMUM_START_SECONDS:$sync_minimum,QUEUE_SHUTDOWN_RESERVE_SECONDS:$shutdown_reserve,QUEUE_MINIMUM_START_SECONDS:$minimum_start,QUEUE_UNKNOWN_JOB_SECONDS:$unknown_job,QUEUE_EXPECTED_BYTES_PER_SECOND:$expected_bps,QUEUE_PREDICTION_SAFETY_FACTOR:$safety_factor}' > "$CONFIG_FILE"
"${OCI[@]}" fn function update --function-id "$FUNCTION_ID" \
  --timeout-in-seconds "$FUNCTION_TIMEOUT" \
  --detached-mode-timeout-in-seconds "$DETACHED_TIMEOUT_SECONDS" \
  --config "file://$CONFIG_FILE" --force >/dev/null

# OCI Functions invocation logging is enabled by default when a log group is
# configured. The log is bound to the application, so this also covers every
# function in a newly recreated application.
if [[ "${ENABLE_FUNCTION_LOG:-true}" == "true" && -n "${FUNCTION_LOG_GROUP_ID:-}" ]]; then
  FUNCTION_LOG_NAME="${FUNCTION_LOG_NAME:-${APP_NAME}_invoke}"
  FUNCTION_LOG_ID=$("${OCI[@]}" logging log list --log-group-id "$FUNCTION_LOG_GROUP_ID" --all \
    --query "data[?configuration.source.resource=='$APP_ID' && configuration.source.service=='functions' && configuration.source.category=='invoke'].id | [0]" --raw-output)
  if [[ -z "$FUNCTION_LOG_ID" || "$FUNCTION_LOG_ID" == null ]]; then
    LOG_CONFIG=$(mktemp)
    jq -n --arg compartment "$COMPARTMENT_ID" --arg app "$APP_ID" \
      '{"compartment-id":$compartment,source:{resource:$app,service:"functions","source-type":"OCISERVICE",category:"invoke"}}' > "$LOG_CONFIG"
    "${OCI[@]}" logging log create --log-group-id "$FUNCTION_LOG_GROUP_ID" --display-name "$FUNCTION_LOG_NAME" \
      --log-type SERVICE --is-enabled true --configuration "file://$LOG_CONFIG" >/dev/null
    FUNCTION_LOG_ID=$("${OCI[@]}" logging log list --log-group-id "$FUNCTION_LOG_GROUP_ID" --all \
      --query "data[?configuration.source.resource=='$APP_ID' && configuration.source.service=='functions' && configuration.source.category=='invoke'].id | [0]" --raw-output)
  fi
  [[ -n "$FUNCTION_LOG_ID" && "$FUNCTION_LOG_ID" != null ]] || { echo "Function invocation log was not created." >&2; exit 1; }
  if grep -q '^export FUNCTION_LOG_ID=' "$ENV_FILE"; then
    sed -i "s|^export FUNCTION_LOG_ID=.*|export FUNCTION_LOG_ID='$FUNCTION_LOG_ID'|" "$ENV_FILE"
  else
    printf "\nexport FUNCTION_LOG_ID='%s'\n" "$FUNCTION_LOG_ID" >> "$ENV_FILE"
  fi
  chmod 600 "$ENV_FILE"
fi

RULE_NAME="${RULE_NAME:-${FUNCTION_NAME}-events}"
EVENT_TYPES='["com.oraclecloud.objectstorage.createobject","com.oraclecloud.objectstorage.updateobject","com.oraclecloud.objectstorage.deleteobject"]'
# OCI Object Storage object events carry the full object path in
# `data.resourceName`. OCI Events uses `*` wildcard matching in values, so
# `myfolder/*.csv` limits this rule to CSV files directly under that folder.
CONDITION=$(jq -nc \
  --arg compartment "$COMPARTMENT_ID" \
  --arg bucket "${OBJECT_STORAGE_BUCKET_NAME:-}" \
  --arg object_pattern "${OBJECT_STORAGE_OBJECT_NAME_PATTERN:-}" \
  --argjson events "$EVENT_TYPES" \
  '{eventType:$events,data:{compartmentId:$compartment}}
   | if $object_pattern == "" then . else .data.resourceName = $object_pattern end
   | .data.additionalDetails = ({}
       | if $bucket == "" then . else .bucketName = $bucket end)
   | if .data.additionalDetails == {} then del(.data.additionalDetails) else . end')
ACTIONS=$(jq -nc --arg function_id "$FUNCTION_ID" '{actions:[{actionType:"FAAS",isEnabled:true,description:"Object Storage partition loader",functionId:$function_id}]}')
RULE_ID=$("${OCI[@]}" events rule list --compartment-id "$COMPARTMENT_ID" --all --query "data[?\"display-name\"=='$RULE_NAME'].id | [0]" --raw-output)
if [[ -z "$RULE_ID" || "$RULE_ID" == null ]]; then
  "${OCI[@]}" events rule create --compartment-id "$COMPARTMENT_ID" --display-name "$RULE_NAME" --condition "$CONDITION" --actions "$ACTIONS" --is-enabled true >/dev/null
else
  "${OCI[@]}" events rule update --rule-id "$RULE_ID" --condition "$CONDITION" --actions "$ACTIONS" --force >/dev/null
fi
RULE_ID=$("${OCI[@]}" events rule list --compartment-id "$COMPARTMENT_ID" --all --query "data[?\"display-name\"=='$RULE_NAME'].id | [0]" --raw-output)
[[ -n "$RULE_ID" && "$RULE_ID" != null ]] || { echo "Events rule $RULE_NAME was not found after deployment." >&2; exit 1; }
RULE_FUNCTION_ID=$("${OCI[@]}" events rule get --rule-id "$RULE_ID" --query 'data.actions.actions[0]."function-id"' --raw-output)
[[ "$RULE_FUNCTION_ID" == "$FUNCTION_ID" ]] || { echo "Events rule $RULE_NAME does not target the deployed Function." >&2; exit 1; }
FUNCTION_STATE=$("${OCI[@]}" fn function get --function-id "$FUNCTION_ID" --query 'data."lifecycle-state"' --raw-output)
[[ "$FUNCTION_STATE" == ACTIVE ]] || { echo "Function $FUNCTION_NAME is not ACTIVE after deployment: $FUNCTION_STATE" >&2; exit 1; }
echo "Deployment complete: $APP_NAME/$FUNCTION_NAME ($FUNCTION_STATE); rule $RULE_NAME targets the current Function."
