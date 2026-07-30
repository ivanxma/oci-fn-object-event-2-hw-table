#!/usr/bin/env bash
# Build deploy/env.sh on an OCI UI/deployment VM using its instance principal.
set -euo pipefail
umask 077

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUTPUT="$ROOT_DIR/deploy/env.sh"
FORCE=false

usage() {
  cat <<'EOF'
Usage: ./deploy/setup_env.sh [--output PATH] [--force]

Discovers the current OCI VM's compartment and region, then prompts for:
  - compartment and region
  - availability domain
  - VCN and subnet
  - Vault, symmetric encryption key, and database JSON secret
  - OCIR repository prefix

OCI calls use only --auth instance_principal. The generated file contains
resource OCIDs and configuration, never an OCI auth token or database password.
EOF
}

while (($#)); do
  case "$1" in
    --output)
      (($# >= 2)) || { echo "--output requires a path." >&2; exit 2; }
      OUTPUT=$2
      shift 2
      ;;
    --force)
      FORCE=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

for command_name in oci jq curl openssl; do
  command -v "$command_name" >/dev/null || {
    echo "Missing required command: $command_name" >&2
    exit 1
  }
done

if [[ -e "$OUTPUT" && "$FORCE" != true ]]; then
  echo "$OUTPUT already exists. Re-run with --force to replace it." >&2
  exit 1
fi

prompt_required() {
  local target=$1 label=$2 default_value=${3:-} answer
  if [[ -n "$default_value" ]]; then
    read -r -p "$label [$default_value]: " answer
    answer=${answer:-$default_value}
  else
    read -r -p "$label: " answer
  fi
  [[ -n "$answer" ]] || { echo "$label is required." >&2; exit 1; }
  printf -v "$target" '%s' "$answer"
}

select_from_tsv() {
  local target=$1 label=$2 tsv=$3
  local -a ids=() labels=()
  local item_id item_label choice
  while IFS=$'\t' read -r item_id item_label; do
    [[ -n "$item_id" ]] || continue
    ids+=("$item_id")
    labels+=("${item_label:-$item_id}")
  done <<< "$tsv"
  if ((${#ids[@]} == 0)); then
    prompt_required "$target" "$label OCID/name"
    return
  fi
  echo
  echo "$label:"
  local index
  for ((index=0; index<${#ids[@]}; index++)); do
    printf '  %d) %s\n' "$((index + 1))" "${labels[$index]}"
  done
  while true; do
    read -r -p "Choose 1-${#ids[@]}: " choice
    if [[ "$choice" =~ ^[0-9]+$ ]] && ((choice >= 1 && choice <= ${#ids[@]})); then
      printf -v "$target" '%s' "${ids[$((choice - 1))]}"
      return
    fi
    echo "Enter a number from 1 to ${#ids[@]}." >&2
  done
}

oci_json() {
  oci --auth instance_principal --region "$REGION" "$@" --output json
}

METADATA=$(
  curl -fsS --connect-timeout 1 --max-time 2 \
    -H "Authorization: Bearer Oracle" \
    http://169.254.169.254/opc/v2/instance/ 2>/dev/null || true
)
DEFAULT_COMPARTMENT=$(jq -r '.compartmentId // empty' <<< "${METADATA:-{}}" 2>/dev/null || true)
DEFAULT_REGION=$(jq -r '.region // empty' <<< "${METADATA:-{}}" 2>/dev/null || true)
DEFAULT_SERVER_NAME=$(jq -r '.hostname // .displayName // .privateIp // empty' <<< "${METADATA:-{}}" 2>/dev/null || true)

echo "OCI Object Event to MySQL environment setup"
echo "Authentication: instance principal"
prompt_required COMPARTMENT_ID "Compartment OCID" "$DEFAULT_COMPARTMENT"
[[ "$COMPARTMENT_ID" == ocid1.compartment.* || "$COMPARTMENT_ID" == ocid1.tenancy.* ]] || {
  echo "Compartment must be an OCI compartment or tenancy OCID." >&2
  exit 1
}
prompt_required REGION "OCI region" "$DEFAULT_REGION"

REGION_KEY=$(
  oci_json iam region list --all |
    jq -r --arg region "$REGION" '.data[] | select(.name == $region) | .key' |
    head -1
)
if [[ -z "$REGION_KEY" || "$REGION_KEY" == null ]]; then
  prompt_required REGION_KEY "OCIR region key"
fi
REGION_KEY=$(printf '%s' "$REGION_KEY" | tr '[:upper:]' '[:lower:]')

AD_TSV=$(
  oci_json iam availability-domain list --compartment-id "$COMPARTMENT_ID" --all |
    jq -r '.data[] | [.name, .name] | @tsv'
)
select_from_tsv CONTAINER_AVAILABILITY_DOMAIN "Availability domain" "$AD_TSV"

VCN_TSV=$(
  oci_json network vcn list --compartment-id "$COMPARTMENT_ID" --all |
    jq -r '.data[] | select(."lifecycle-state" == "AVAILABLE") |
      [.id, ((."display-name" // "(unnamed VCN)") + " | " + (."cidr-block" // "no CIDR"))] | @tsv'
)
select_from_tsv VCN_ID "VCN" "$VCN_TSV"

SUBNET_TSV=$(
  oci_json network subnet list --compartment-id "$COMPARTMENT_ID" --vcn-id "$VCN_ID" --all |
    jq -r '.data[] | select(."lifecycle-state" == "AVAILABLE") |
      [.id, ((."display-name" // "(unnamed subnet)") + " | " + (."cidr-block" // "no CIDR") +
      " | " + (."availability-domain" // "regional"))] | @tsv'
)
select_from_tsv SUBNET_ID "Processor subnet" "$SUBNET_TSV"

VAULT_TSV=$(
  oci_json kms management vault list --compartment-id "$COMPARTMENT_ID" --all |
    jq -r '.data[] | select(."lifecycle-state" == "ACTIVE") |
      [.id, ((."display-name" // "(unnamed Vault)") + " | " + .id)] | @tsv'
)
select_from_tsv VAULT_ID "Vault" "$VAULT_TSV"

VAULT_JSON=$(oci_json kms management vault get --vault-id "$VAULT_ID")
KMS_ENDPOINT=$(jq -r '.data."management-endpoint" // empty' <<< "$VAULT_JSON")
[[ -n "$KMS_ENDPOINT" ]] || {
  echo "Could not resolve the selected Vault management endpoint." >&2
  exit 1
}
KEY_TSV=$(
  oci_json kms management key list --endpoint "$KMS_ENDPOINT" \
    --compartment-id "$COMPARTMENT_ID" --all |
    jq -r '.data[] |
      select(."lifecycle-state" == "ENABLED" and ((.algorithm // "") | ascii_upcase) == "AES") |
      [.id, ((."display-name" // "(unnamed key)") + " | AES | " + .id)] | @tsv'
)
select_from_tsv VAULT_KEY_ID "Symmetric Vault encryption key" "$KEY_TSV"

SECRET_TSV=$(
  oci_json vault secret list --compartment-id "$COMPARTMENT_ID" \
    --vault-id "$VAULT_ID" --all |
    jq -r '.data[] | select(."lifecycle-state" == "ACTIVE") |
      [.id, ((."secret-name" // "(unnamed secret)") + " | " + .id)] | @tsv'
)
select_from_tsv DB_SECRET_OCID "Processor database JSON secret" "$SECRET_TSV"
[[ "$DB_SECRET_OCID" == ocid1.vaultsecret.* ]] || {
  echo "The database secret must be a Vault secret OCID." >&2
  exit 1
}

prompt_required REPOSITORY_PREFIX "OCIR repository prefix" "object-storage-heatwave"
PROCESSOR_IMAGE_NAME=object-storage-stream-processor
prompt_required PROCESSOR_IMAGE_TAG "Immutable processor image tag" "$(date -u +%Y%m%d%H%M%S)"
NAMESPACE=$(oci_json os ns get | jq -r '.data // empty')
[[ -n "$NAMESPACE" ]] || { echo "Could not resolve the Object Storage/OCIR namespace." >&2; exit 1; }
REPOSITORY_PREFIX_LOWER=$(printf '%s' "$REPOSITORY_PREFIX" | tr '[:upper:]' '[:lower:]')
PROCESSOR_IMAGE_URL="$REGION_KEY.ocir.io/$NAMESPACE/$REPOSITORY_PREFIX_LOWER/$PROCESSOR_IMAGE_NAME:$PROCESSOR_IMAGE_TAG"
FLASK_SECRET_KEY=$(openssl rand -hex 32)

prompt_required CONTROL_DATABASE "Control database" "stream_db"
prompt_required STREAM_DATA_DB_NAME "Durable stream database" "stream_data"
prompt_required UI_SERVER_NAME "UI server name" "${DEFAULT_SERVER_NAME:-_}"

TMP_FILE=$(mktemp "${OUTPUT}.tmp.XXXXXX")
trap 'rm -f "$TMP_FILE"' EXIT
write_export() {
  printf 'export %s=%q\n' "$1" "$2" >> "$TMP_FILE"
}

{
  printf '%s\n' '# Generated by deploy/setup_env.sh using an OCI instance principal.'
  printf '%s\n' '# This file is intentionally git-ignored. Keep mode 0600.'
} > "$TMP_FILE"

write_export COMPARTMENT_ID "$COMPARTMENT_ID"
write_export REGION "$REGION"
write_export REGION_KEY "$REGION_KEY"
write_export VCN_ID "$VCN_ID"
write_export SUBNET_ID "$SUBNET_ID"
write_export CONTAINER_AVAILABILITY_DOMAIN "$CONTAINER_AVAILABILITY_DOMAIN"
write_export VAULT_ID "$VAULT_ID"
write_export VAULT_KEY_ID "$VAULT_KEY_ID"
write_export DB_SECRET_OCID "$DB_SECRET_OCID"
write_export REPOSITORY_PREFIX "$REPOSITORY_PREFIX"
write_export PROCESSOR_IMAGE_NAME "$PROCESSOR_IMAGE_NAME"
write_export PROCESSOR_IMAGE_TAG "$PROCESSOR_IMAGE_TAG"
write_export PROCESSOR_IMAGE_URL "$PROCESSOR_IMAGE_URL"
write_export FLASK_SECRET_KEY "$FLASK_SECRET_KEY"
write_export CONTROL_DATABASE "$CONTROL_DATABASE"
write_export STREAM_DATA_DB_NAME "$STREAM_DATA_DB_NAME"
write_export UI_SERVER_NAME "$UI_SERVER_NAME"

cat >> "$TMP_FILE" <<'EOF'
export PROCESSOR_CONTAINER_NAME_PREFIX='object-storage-stream-processor'
export PROCESSOR_SHAPE='CI.Standard.E4.Flex'
export PROCESSOR_OCPUS='1'
export PROCESSOR_MEMORY_GBS='16'
export WRITER_WORKERS='4'
export BATCH_ROWS='10000'
export OBJECT_STORAGE_RANGE_BYTES='33554432'
export OBJECT_STORAGE_READ_TIMEOUT_SECONDS='300'
export PROCESSOR_PROCESSING_LEASE_SECONDS='300'
export OCI_AUTH_MODE='resource_principal'
export OCI_EVENT_RULE_MANAGEMENT_ENABLED='true'
export OCI_STREAMING_MANAGEMENT_ENABLED='true'
export OCI_CONTAINER_ORCHESTRATION_ENABLED='true'
export OCI_EVENT_RULE_PREFIX='object-event-2-table'
export UI_SERVICE_NAME='object-storage-heatwave-ui'
export UI_CONTAINER_NAME='object-storage-heatwave-ui'
export UI_BIND_PORT='8080'
export GENERATE_SELF_SIGNED_CERT='false'
export TLS_CERT_FILE=''
export TLS_KEY_FILE=''
export OBJECT_STORAGE_NAMESPACE=''
export OBJECT_STORAGE_BUCKET_NAME=''
export OBJECT_STORAGE_OBJECT_NAME_PATTERN=''
export DB_HOST=''
export DB_PORT='3306'
export DB_USER=''
export DB_NAME=''
export DB_SSL_DISABLED='false'
export OCI_STREAM_ID=''
export PROCESSING_MODE='FIFO'
export EXPECTED_PARTITION_COUNT='1'
export PROCESSOR_REPLICA_COUNT='1'
export PROCESSOR_PARTITIONS='0'
export PROCESSOR_MAPPING_ID=''
EOF

mkdir -p "$(dirname "$OUTPUT")"
chmod 600 "$TMP_FILE"
mv "$TMP_FILE" "$OUTPUT"
trap - EXIT

echo
echo "Created $OUTPUT (mode 0600)"
echo "Processor image: $PROCESSOR_IMAGE_URL"
echo "Next: review the file, configure TLS, then run ./tests/integration/verify_streaming_deployment.sh"
