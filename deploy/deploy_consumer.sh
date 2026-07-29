#!/usr/bin/env bash
# Deploy the consumer for one mapping. Mapping-specific values are explicit.
set -euo pipefail
umask 077
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
[[ -r "$ENV_FILE" ]] || { echo "Missing $ENV_FILE" >&2; exit 1; }
set -a; . "$ENV_FILE"; set +a
for value in OCI_STREAM_ID PROCESSING_MODE EXPECTED_PARTITION_COUNT CONSUMER_REPLICA_COUNT CONSUMER_PARTITIONS COMPARTMENT_ID REGION REGION_KEY SUBNET_ID CONTAINER_AVAILABILITY_DOMAIN CONSUMER_SHAPE CONSUMER_OCPUS CONSUMER_MEMORY_GBS DB_SECRET_OCID DB_HOST DB_PORT DB_USER DB_NAME STREAM_DATA_DB_NAME CONSUMER_IMAGE_TAG REPOSITORY_PREFIX CONSUMER_IMAGE_NAME; do
  [[ -n "${!value:-}" ]] || { echo "$value is required" >&2; exit 1; }
done
case "$PROCESSING_MODE" in
  FIFO) [[ "$EXPECTED_PARTITION_COUNT" == 1 && "$CONSUMER_REPLICA_COUNT" == 1 ]] || { echo 'FIFO requires 1 partition and 1 replica.' >&2; exit 1; } ;;
  PARALLEL) (( EXPECTED_PARTITION_COUNT >= 2 && CONSUMER_REPLICA_COUNT >= 1 && CONSUMER_REPLICA_COUNT <= EXPECTED_PARTITION_COUNT )) || { echo 'Invalid parallel replica count.' >&2; exit 1; } ;;
  *) echo 'PROCESSING_MODE must be FIFO or PARALLEL.' >&2; exit 1 ;;
esac
IFS=',' read -r -a PARTITION_LIST <<< "$CONSUMER_PARTITIONS"
[[ ${#PARTITION_LIST[@]} -ge 1 ]] || { echo 'CONSUMER_PARTITIONS is required.' >&2; exit 1; }
declare -A SEEN_PARTITIONS=()
for partition in "${PARTITION_LIST[@]}"; do
  [[ "$partition" =~ ^[0-9]+$ ]] && (( partition < EXPECTED_PARTITION_COUNT )) || { echo 'CONSUMER_PARTITIONS contains an invalid partition.' >&2; exit 1; }
  [[ -z "${SEEN_PARTITIONS[$partition]:-}" ]] || { echo 'CONSUMER_PARTITIONS contains a duplicate partition.' >&2; exit 1; }
  SEEN_PARTITIONS[$partition]=1
done
[[ "$PROCESSING_MODE" != FIFO || "$CONSUMER_PARTITIONS" == 0 ]] || { echo 'FIFO consumer must be assigned partition 0.' >&2; exit 1; }
command -v oci >/dev/null || { echo 'Missing OCI CLI.' >&2; exit 1; }
NAMESPACE=$(oci --auth instance_principal os ns get --query data --raw-output)
IMAGE="$REGION_KEY.ocir.io/$NAMESPACE/${REPOSITORY_PREFIX,,}/$CONSUMER_IMAGE_NAME:$CONSUMER_IMAGE_TAG"
PARTITION_SUFFIX=${CONSUMER_PARTITIONS//,/-}
CONTAINER_NAME="${CONSUMER_CONTAINER_NAME_PREFIX}-${PROCESSING_MODE,,}-p${PARTITION_SUFFIX}"
CONFIG=$(mktemp); trap 'rm -f "$CONFIG"' EXIT
jq -n --arg name "$CONTAINER_NAME" --arg image "$IMAGE" --arg stream "$OCI_STREAM_ID" --arg mode "$PROCESSING_MODE" --arg partitions "$EXPECTED_PARTITION_COUNT" --arg replicas "$CONSUMER_REPLICA_COUNT" --arg assignment "$CONSUMER_PARTITIONS" --arg secret "$DB_SECRET_OCID" --arg host "$DB_HOST" --arg port "$DB_PORT" --arg user "$DB_USER" --arg database "$DB_NAME" --arg streamdata "$STREAM_DATA_DB_NAME" \
  '[{displayName:$name,imageUrl:$image,isResourcePrincipalDisabled:false,environmentVariables:{OCI_STREAM_ID:$stream,PROCESSING_MODE:$mode,EXPECTED_PARTITION_COUNT:$partitions,CONSUMER_REPLICA_COUNT:$replicas,CONSUMER_PARTITIONS:$assignment,DB_SECRET_OCID:$secret,DB_HOST:$host,DB_PORT:$port,DB_USER:$user,DB_NAME:$database,STREAM_DATA_DB_NAME:$streamdata}}]' > "$CONFIG"
oci --auth instance_principal container-instances container-instance create \
  --compartment-id "$COMPARTMENT_ID" --availability-domain "$CONTAINER_AVAILABILITY_DOMAIN" \
  --display-name "$CONTAINER_NAME" --shape "$CONSUMER_SHAPE" \
  --shape-config "{\"ocpus\":$CONSUMER_OCPUS,\"memoryInGBs\":$CONSUMER_MEMORY_GBS}" \
  --containers "file://$CONFIG" --vnics "[{\"subnetId\":\"$SUBNET_ID\"}]" \
  --wait-for-state ACTIVE --wait-for-state FAILED
