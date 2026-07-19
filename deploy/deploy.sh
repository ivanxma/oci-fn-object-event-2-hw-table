#!/usr/bin/env bash
# Idempotently deploy the OCI Function and its Object Storage Events rule.
set -euo pipefail
umask 077

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
[[ -r "$ENV_FILE" ]] || { echo "Copy deploy/env.sh.example to deploy/env.sh and set deployment values." >&2; exit 1; }
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
for command in oci fn podman jq; do command -v "$command" >/dev/null || { echo "Missing $command; run deploy/bootstrap.sh first." >&2; exit 1; }; done

OCI=(oci --auth instance_principal)
NAMESPACE=$("${OCI[@]}" os ns get --query data --raw-output)
REPOSITORY_NAME="$REPOSITORY_PREFIX/$FUNCTION_NAME"
REPOSITORY_ID=$("${OCI[@]}" artifacts container repository list --compartment-id "$COMPARTMENT_ID" --all --query "data.items[?\"display-name\"=='$REPOSITORY_NAME'].id | [0]" --raw-output)
if [[ -z "$REPOSITORY_ID" || "$REPOSITORY_ID" == null ]]; then
  "${OCI[@]}" artifacts container repository create --compartment-id "$COMPARTMENT_ID" --display-name "$REPOSITORY_NAME" --is-public false >/dev/null
fi
APP_ID=$("${OCI[@]}" fn application list --compartment-id "$COMPARTMENT_ID" --all --query "data[?\"display-name\"=='$APP_NAME'].id | [0]" --raw-output)
if [[ -z "$APP_ID" || "$APP_ID" == null ]]; then
  APP_ID=$("${OCI[@]}" fn application create --compartment-id "$COMPARTMENT_ID" --display-name "$APP_NAME" --subnet-ids "[\"$SUBNET_ID\"]" --query 'data.id' --raw-output)
fi

export PATH="$HOME/.fn/bin:$HOME/bin:$PATH"
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
[[ "$FUNCTION_MEMORY" =~ ^[0-9]+$ ]] || { echo "FUNCTION_MEMORY must be numeric." >&2; exit 1; }
[[ "$FUNCTION_TIMEOUT" =~ ^[0-9]+$ ]] || { echo "FUNCTION_TIMEOUT must be numeric." >&2; exit 1; }
cp "$ROOT_DIR/function/Dockerfile" "$ROOT_DIR/function/func.py" "$ROOT_DIR/function/partition_loader.py" "$ROOT_DIR/function/requirements.txt" "$BUILD_DIR/"
sed -e "s/^name:.*/name: $FUNCTION_NAME/" -e "s/^memory:.*/memory: $FUNCTION_MEMORY/" -e "s/^timeout:.*/timeout: $FUNCTION_TIMEOUT/" "$ROOT_DIR/function/func.yaml" > "$BUILD_DIR/func.yaml"
(cd "$BUILD_DIR" && fn deploy --app "$APP_NAME")
FUNCTION_ID=$("${OCI[@]}" fn function list --application-id "$APP_ID" --all --query "data[?\"display-name\"=='$FUNCTION_NAME'].id | [0]" --raw-output)
FUNCTION_INVOKE_ENDPOINT=$("${OCI[@]}" fn function get --function-id "$FUNCTION_ID" --query 'data."invoke-endpoint"' --raw-output)
[[ -n "$FUNCTION_INVOKE_ENDPOINT" && "$FUNCTION_INVOKE_ENDPOINT" != null ]] || { echo "Function invoke endpoint was not resolved." >&2; exit 1; }
jq -n --arg host "$DB_HOST" --arg port "${DB_PORT:-3306}" --arg user "$DB_USER" --arg password "$DB_PASSWORD" --arg ssl "${DB_SSL_DISABLED:-false}" --arg namespace "${OBJECT_STORAGE_NAMESPACE:-}" --arg batch "${BATCH_ROWS:-10000}" --arg workers "${WRITER_WORKERS:-4}" --arg read_timeout "${OBJECT_STORAGE_READ_TIMEOUT_SECONDS:-300}" --arg range_bytes "${OBJECT_STORAGE_RANGE_BYTES:-33554432}" --arg control "$CONTROL_DATABASE" --arg function_id "$FUNCTION_ID" --arg invoke_endpoint "$FUNCTION_INVOKE_ENDPOINT" --arg region "$REGION" --arg detached "${DETACHED_ENABLED:-false}" --arg detached_timeout "${DETACHED_TIMEOUT_SECONDS:-3600}" '{DB_HOST:$host,DB_PORT:$port,DB_USER:$user,DB_PASSWORD:$password,DB_SSL_DISABLED:$ssl,OBJECT_STORAGE_NAMESPACE:$namespace,BATCH_ROWS:$batch,WRITER_WORKERS:$workers,OBJECT_STORAGE_READ_TIMEOUT_SECONDS:$read_timeout,OBJECT_STORAGE_RANGE_BYTES:$range_bytes,CONTROL_DATABASE:$control,FUNCTION_ID:$function_id,FUNCTION_INVOKE_ENDPOINT:$invoke_endpoint,OCI_REGION:$region,DETACHED_ENABLED:$detached,DETACHED_TIMEOUT_SECONDS:$detached_timeout}' > "$CONFIG_FILE"
"${OCI[@]}" fn function update --function-id "$FUNCTION_ID" --config "file://$CONFIG_FILE" --force >/dev/null

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
echo "Deployment complete: $APP_NAME/$FUNCTION_NAME; rule $RULE_NAME."
