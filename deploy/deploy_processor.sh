#!/usr/bin/env bash
# Deploy the processor for one mapping. Mapping-specific values are explicit.
set -euo pipefail
umask 077
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
[[ -r "$ENV_FILE" ]] || { echo "Missing $ENV_FILE" >&2; exit 1; }
set -a; . "$ENV_FILE"; set +a
source "$ROOT_DIR/deploy/oci_context.sh"
oci_context_resolve
# A mapping-specific replacement may deliberately select a different Vault
# database bundle without rewriting the UI/deployment host's default env.sh.
DB_SECRET_OCID="${PROCESSOR_DB_SECRET_OCID:-${DB_SECRET_OCID:-}}"
PROCESSOR_SHAPE="${PROCESSOR_SHAPE:-CI.Standard.E4.Flex}"
PROCESSOR_OCPUS="${PROCESSOR_OCPUS:-1}"
PROCESSOR_MEMORY_GBS="${PROCESSOR_MEMORY_GBS:-16}"
WRITER_WORKERS="${WRITER_WORKERS:-4}"
for value in OCI_STREAM_ID PROCESSING_MODE EXPECTED_PARTITION_COUNT PROCESSOR_REPLICA_COUNT PROCESSOR_PARTITIONS PROCESSOR_MAPPING_ID COMPARTMENT_ID REGION REGION_KEY SUBNET_ID CONTAINER_AVAILABILITY_DOMAIN PROCESSOR_SHAPE PROCESSOR_OCPUS PROCESSOR_MEMORY_GBS DB_SECRET_OCID WRITER_WORKERS PROCESSOR_IMAGE_TAG REPOSITORY_PREFIX PROCESSOR_IMAGE_NAME; do
  [[ -n "${!value:-}" ]] || { echo "$value is required" >&2; exit 1; }
done
case "$PROCESSING_MODE" in
  FIFO) [[ "$EXPECTED_PARTITION_COUNT" == 1 && "$PROCESSOR_REPLICA_COUNT" == 1 ]] || { echo 'FIFO requires 1 partition and 1 replica.' >&2; exit 1; } ;;
  PARALLEL) (( EXPECTED_PARTITION_COUNT >= 2 && PROCESSOR_REPLICA_COUNT >= 1 && PROCESSOR_REPLICA_COUNT <= EXPECTED_PARTITION_COUNT )) || { echo 'Invalid parallel replica count.' >&2; exit 1; } ;;
  *) echo 'PROCESSING_MODE must be FIFO or PARALLEL.' >&2; exit 1 ;;
esac
IFS=',' read -r -a PARTITION_LIST <<< "$PROCESSOR_PARTITIONS"
[[ ${#PARTITION_LIST[@]} -ge 1 ]] || { echo 'PROCESSOR_PARTITIONS is required.' >&2; exit 1; }
declare -A SEEN_PARTITIONS=()
for partition in "${PARTITION_LIST[@]}"; do
  [[ "$partition" =~ ^[0-9]+$ ]] && (( partition < EXPECTED_PARTITION_COUNT )) || { echo 'PROCESSOR_PARTITIONS contains an invalid partition.' >&2; exit 1; }
  [[ -z "${SEEN_PARTITIONS[$partition]:-}" ]] || { echo 'PROCESSOR_PARTITIONS contains a duplicate partition.' >&2; exit 1; }
  SEEN_PARTITIONS[$partition]=1
done
[[ "$PROCESSING_MODE" != FIFO || "$PROCESSOR_PARTITIONS" == 0 ]] || { echo 'FIFO processor must be assigned partition 0.' >&2; exit 1; }
[[ "$WRITER_WORKERS" =~ ^[0-9]+$ ]] && (( WRITER_WORKERS >= 1 && WRITER_WORKERS <= 32 )) || { echo 'WRITER_WORKERS must be from 1 to 32.' >&2; exit 1; }
command -v oci >/dev/null || { echo 'Missing OCI CLI.' >&2; exit 1; }
NAMESPACE=$(oci --auth instance_principal --region "$REGION" os ns get --query data --raw-output)
IMAGE="$REGION_KEY.ocir.io/$NAMESPACE/${REPOSITORY_PREFIX,,}/$PROCESSOR_IMAGE_NAME:$PROCESSOR_IMAGE_TAG"
PARTITION_SUFFIX=${PROCESSOR_PARTITIONS//,/-}
CONTAINER_NAME="${PROCESSOR_CONTAINER_NAME_PREFIX}-${PROCESSING_MODE,,}-p${PARTITION_SUFFIX}"
CONFIG=$(mktemp); trap 'rm -f "$CONFIG"' EXIT
jq -n --arg name "$CONTAINER_NAME" --arg image "$IMAGE" --arg stream "$OCI_STREAM_ID" --arg mode "$PROCESSING_MODE" --arg partitions "$EXPECTED_PARTITION_COUNT" --arg replicas "$PROCESSOR_REPLICA_COUNT" --arg assignment "$PROCESSOR_PARTITIONS" --arg secret "$DB_SECRET_OCID" --arg workers "$WRITER_WORKERS" \
  '[{displayName:$name,imageUrl:$image,isResourcePrincipalDisabled:false,environmentVariables:{OCI_STREAM_ID:$stream,PROCESSING_MODE:$mode,EXPECTED_PARTITION_COUNT:$partitions,PROCESSOR_REPLICA_COUNT:$replicas,PROCESSOR_PARTITIONS:$assignment,DB_SECRET_OCID:$secret,WRITER_WORKERS:$workers}}]' > "$CONFIG"
RELEASE_VERSION="${RELEASE_VERSION:-$PROCESSOR_IMAGE_TAG}"
GIT_SHA="${GIT_SHA:-$(git -C "$ROOT_DIR" rev-parse --short HEAD)}"
SOURCE_BRANCH="${SOURCE_BRANCH:-$(git -C "$ROOT_DIR" branch --show-current)}"
BUILD_UTC="${BUILD_UTC:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
CONFIG_SCHEMA_VERSION="${CONFIG_SCHEMA_VERSION:-2}"
TAGS=$(jq -nc \
  --arg mapping "$PROCESSOR_MAPPING_ID" \
  --arg release "$RELEASE_VERSION" \
  --arg git_sha "$GIT_SHA" \
  --arg schema "$CONFIG_SCHEMA_VERSION" \
  '{"managed-by":"oci-object-event-2-table","mapping-id":$mapping,
    "release-version":$release,"git-sha":$git_sha,"config-schema-version":$schema}')
oci --auth instance_principal --region "$REGION" container-instances container-instance create \
  --compartment-id "$COMPARTMENT_ID" --availability-domain "$CONTAINER_AVAILABILITY_DOMAIN" \
  --display-name "$CONTAINER_NAME" --shape "$PROCESSOR_SHAPE" \
  --shape-config "{\"ocpus\":$PROCESSOR_OCPUS,\"memoryInGBs\":$PROCESSOR_MEMORY_GBS}" \
  --containers "file://$CONFIG" --vnics "[{\"subnetId\":\"$SUBNET_ID\"}]" \
  --freeform-tags "$TAGS" \
  --wait-for-state SUCCEEDED --wait-for-state FAILED

DEPLOYMENT_ID=$(
  oci --auth instance_principal --region "$REGION" container-instances container-instance list \
    --compartment-id "$COMPARTMENT_ID" --all --output json |
    jq -r --arg name "$CONTAINER_NAME" --arg mapping "$PROCESSOR_MAPPING_ID" '
      [.data.items[] |
       select(."display-name" == $name and ."freeform-tags"."mapping-id" == $mapping and
              ."lifecycle-state" != "DELETED")] |
      sort_by(."time-created") | last.id // empty'
)
DEPLOYMENT_PYTHON="${DEPLOYMENT_PYTHON_BIN:-$ROOT_DIR/.venv-verification-py312/bin/python}"
if [[ ! -x "$DEPLOYMENT_PYTHON" ]] && command -v python3 >/dev/null && python3 -c 'import mysql.connector, oci' >/dev/null 2>&1; then
  DEPLOYMENT_PYTHON=$(command -v python3)
fi
if [[ -x "$DEPLOYMENT_PYTHON" ]]; then
  OCI_AUTH_MODE=instance_principal "$DEPLOYMENT_PYTHON" "$ROOT_DIR/deploy/record_deployment.py" \
    --component PROCESSOR --deployment-name "$CONTAINER_NAME" \
    --deployment-id "$DEPLOYMENT_ID" --mapping-id "$PROCESSOR_MAPPING_ID" \
    --release-version "$RELEASE_VERSION" --git-sha "$GIT_SHA" \
    --source-branch "$SOURCE_BRANCH" --build-utc "$BUILD_UTC" \
    --image-name "$PROCESSOR_IMAGE_NAME" --image-tag "$PROCESSOR_IMAGE_TAG" \
    --config-schema-version "$CONFIG_SCHEMA_VERSION" ||
    echo "WARNING: Processor deployment succeeded but deployment history could not be recorded." >&2
else
  echo "WARNING: Processor deployment succeeded but no deployment Python with MySQL Connector and OCI SDK is available to record history." >&2
fi
