#!/usr/bin/env bash
# Read-only post-deployment validation for OCI resources, services, and runtime.
set -euo pipefail
umask 077

export PATH="$HOME/.fn/bin:$HOME/bin:$HOME/.local/bin:$PATH"
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
[[ -r "$ENV_FILE" ]] || { echo "Deployment environment is not readable: $ENV_FILE" >&2; exit 1; }
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

for value in COMPARTMENT_ID APP_NAME FUNCTION_NAME RULE_NAME REGION CONTROL_DATABASE DB_HOST UI_SERVICE_NAME UI_CONTAINER_NAME; do
  [[ -n "${!value:-}" ]] || { echo "Missing $value in $ENV_FILE" >&2; exit 1; }
done
for command in oci jq podman sudo systemctl curl stat; do
  command -v "$command" >/dev/null 2>&1 || { echo "Missing required command: $command" >&2; exit 1; }
done

ENV_MODE=$(stat -c '%a' "$ENV_FILE")
[[ "$ENV_MODE" == 600 ]] || { echo "$ENV_FILE must have mode 600; found $ENV_MODE." >&2; exit 1; }

OCI=(oci --auth instance_principal)
APP_ID=$("${OCI[@]}" fn application list --compartment-id "$COMPARTMENT_ID" --all \
  --query "data[?\"display-name\"=='$APP_NAME'].id | [0]" --raw-output)
[[ -n "$APP_ID" && "$APP_ID" != null ]] || { echo "Function application not found: $APP_NAME" >&2; exit 1; }
FUNCTION_ID=$("${OCI[@]}" fn function list --application-id "$APP_ID" --all \
  --query "data[?\"display-name\"=='$FUNCTION_NAME'].id | [0]" --raw-output)
[[ -n "$FUNCTION_ID" && "$FUNCTION_ID" != null ]] || { echo "Function not found: $FUNCTION_NAME" >&2; exit 1; }
FUNCTION_JSON=$("${OCI[@]}" fn function get --function-id "$FUNCTION_ID")
[[ "$(jq -r '.data."lifecycle-state"' <<<"$FUNCTION_JSON")" == ACTIVE ]] || { echo "Function is not ACTIVE." >&2; exit 1; }
[[ "$(jq -r '.data.config.CONTROL_DATABASE' <<<"$FUNCTION_JSON")" == "$CONTROL_DATABASE" ]] || {
  echo "Function CONTROL_DATABASE does not match $CONTROL_DATABASE." >&2
  exit 1
}

RULE_ID=$("${OCI[@]}" events rule list --compartment-id "$COMPARTMENT_ID" --all \
  --query "data[?\"display-name\"=='$RULE_NAME'].id | [0]" --raw-output)
[[ -n "$RULE_ID" && "$RULE_ID" != null ]] || { echo "Events rule not found: $RULE_NAME" >&2; exit 1; }
RULE_JSON=$("${OCI[@]}" events rule get --rule-id "$RULE_ID")
[[ "$(jq -r '.data."is-enabled"' <<<"$RULE_JSON")" == true ]] || { echo "Events rule is disabled: $RULE_NAME" >&2; exit 1; }
[[ "$(jq -r '.data.actions.actions[0]."function-id"' <<<"$RULE_JSON")" == "$FUNCTION_ID" ]] || {
  echo "Events rule does not target Function $FUNCTION_NAME." >&2
  exit 1
}
RULE_CONDITION=$(jq -r '.data.condition' <<<"$RULE_JSON")
if [[ -n "${OBJECT_STORAGE_BUCKET_NAME:-}" ]]; then
  jq -e --arg bucket "$OBJECT_STORAGE_BUCKET_NAME" '.data.additionalDetails.bucketName == $bucket' \
    <<<"$RULE_CONDITION" >/dev/null || { echo "Events rule bucket filter does not match $OBJECT_STORAGE_BUCKET_NAME." >&2; exit 1; }
  NAMESPACE=$("${OCI[@]}" os ns get --query data --raw-output)
  [[ "$("${OCI[@]}" os bucket get --namespace-name "$NAMESPACE" --name "$OBJECT_STORAGE_BUCKET_NAME" --query 'data."object-events-enabled"' --raw-output)" == true ]] || {
    echo "Object events are disabled for bucket $OBJECT_STORAGE_BUCKET_NAME." >&2
    exit 1
  }
fi
if [[ -n "${OBJECT_STORAGE_OBJECT_NAME_PATTERN:-}" ]]; then
  jq -e --arg pattern "$OBJECT_STORAGE_OBJECT_NAME_PATTERN" '.data.resourceName == $pattern' \
    <<<"$RULE_CONDITION" >/dev/null || { echo "Events rule object-name filter does not match $OBJECT_STORAGE_OBJECT_NAME_PATTERN." >&2; exit 1; }
fi

sudo systemctl is-active --quiet "$UI_SERVICE_NAME" || { echo "UI service is not active: $UI_SERVICE_NAME" >&2; exit 1; }
sudo systemctl is-active --quiet nginx || { echo "nginx is not active." >&2; exit 1; }
sudo podman container exists "$UI_CONTAINER_NAME" || { echo "UI container is not running: $UI_CONTAINER_NAME" >&2; exit 1; }
sudo podman image exists "$UI_SERVICE_NAME:latest" || { echo "UI image is missing: $UI_SERVICE_NAME:latest" >&2; exit 1; }
curl --fail --silent --show-error --insecure https://127.0.0.1/login >/dev/null
sudo nginx -t >/dev/null

INSTANCE_DIR="$ROOT_DIR/ui/instance"
[[ "$(stat -c '%a' "$INSTANCE_DIR")" == 700 ]] || { echo "$INSTANCE_DIR must have mode 700." >&2; exit 1; }
[[ "$(stat -c '%a' "$ROOT_DIR/ui/.ui-runtime.env")" == 600 ]] || { echo "UI runtime environment must have mode 600." >&2; exit 1; }

sudo podman run --rm --network host --entrypoint python \
  -e VALIDATE_DB_HOST="$DB_HOST" -e VALIDATE_DB_PORT="${DB_PORT:-3306}" \
  "$UI_SERVICE_NAME:latest" -c \
  'import os,socket,sys,mysql.connector; assert sys.version_info >= (3,13); assert tuple(map(int,mysql.connector.__version__.split(".")[:2])) >= (9,7); socket.create_connection((os.environ["VALIDATE_DB_HOST"],int(os.environ["VALIDATE_DB_PORT"])),5).close()'

echo "Validation complete: Function ACTIVE, Events rule enabled/current, bucket events enabled, UI/nginx healthy, DB endpoint reachable."
echo "Runtime policy: Python 3.13+, Connector/Python 9.7+, protected environment modes verified."
