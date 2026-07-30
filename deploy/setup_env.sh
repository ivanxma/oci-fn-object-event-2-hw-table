#!/usr/bin/env bash
# Build deploy/env.sh on an OCI UI/deployment VM using its instance principal.
set -euo pipefail
umask 077

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
OUTPUT="$ROOT_DIR/deploy/env.sh"
FORCE=false
NON_INTERACTIVE=false
DB_PASSWORD_FILE="${DB_PASSWORD_FILE:-}"
PASSWORD_FILE_CONSUMED=false

usage() {
  cat <<'EOF'
Usage: ./deploy/setup_env.sh [--output PATH] [--force] [--non-interactive] [--db-password-file PATH]

Discovers the current OCI VM's compartment and region, then prompts for:
  - compartment and region
  - availability domain
  - VCN and subnet
  - Vault, symmetric encryption key, and database JSON secret
  - OCIR repository prefix

OCI calls use only --auth instance_principal. The generated file contains
resource OCIDs and non-secret configuration; database access uses a Vault OCID.

With --non-interactive, supply DB_SECRET_OCID or DB_HOST, DB_USER, and
--db-password-file. The password file must be mode 0600 and is removed only
after the Vault secret OCID is atomically written to the generated env.sh.
Compartment, region, AD, VCN, Vault, and key are derived from instance metadata
and those resources where possible. Explicit environment values take priority.
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
    --non-interactive)
      NON_INTERACTIVE=true
      shift
      ;;
    --db-password-file)
      (($# >= 2)) || { echo "--db-password-file requires a path." >&2; exit 2; }
      DB_PASSWORD_FILE=$2
      shift 2
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
  if [[ "$NON_INTERACTIVE" == true ]]; then
    answer=${!target:-$default_value}
    [[ -n "$answer" ]] || { echo "$label is required for non-interactive setup." >&2; exit 1; }
    printf -v "$target" '%s' "$answer"
    return
  fi
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
  if [[ "$NON_INTERACTIVE" == true ]]; then
    local selected=${!target:-}
    [[ -n "$selected" ]] || { echo "$label is required for non-interactive setup." >&2; exit 1; }
    if ((${#ids[@]} > 0)); then
      local matched=false candidate
      for candidate in "${ids[@]}"; do
        [[ "$candidate" != "$selected" ]] || { matched=true; break; }
      done
      [[ "$matched" == true ]] || { echo "$label is not available in the selected compartment: $selected" >&2; exit 1; }
    fi
    printf -v "$target" '%s' "$selected"
    return
  fi
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
METADATA_JSON=${METADATA:-}; METADATA_JSON=${METADATA_JSON:-"{}"}
DEFAULT_COMPARTMENT=$(jq -r '.compartmentId // empty' <<< "$METADATA_JSON" 2>/dev/null || true)
DEFAULT_REGION=$(jq -r '.region // empty' <<< "$METADATA_JSON" 2>/dev/null || true)
DEFAULT_SERVER_NAME=$(jq -r '.hostname // .displayName // .privateIp // empty' <<< "$METADATA_JSON" 2>/dev/null || true)
DEFAULT_AD=$(jq -r '.availabilityDomain // empty' <<< "$METADATA_JSON" 2>/dev/null || true)

echo "OCI Object Event to MySQL environment setup"
echo "Authentication: instance principal"
prompt_required COMPARTMENT_ID "Compartment OCID" "$DEFAULT_COMPARTMENT"
[[ "$COMPARTMENT_ID" == ocid1.compartment.* || "$COMPARTMENT_ID" == ocid1.tenancy.* ]] || {
  echo "Compartment must be an OCI compartment or tenancy OCID." >&2
  exit 1
}
prompt_required REGION "OCI region" "$DEFAULT_REGION"
source "$ROOT_DIR/deploy/oci_context.sh"
# Resolve the setup VM subnet when its instance-principal metadata permits it;
# an explicit value still wins for a different processor subnet.
oci_context_resolve || true

REGION_KEY=$(
  oci_json iam region list --all |
    jq -r --arg region "$REGION" '.data[] | select(.name == $region) | .key' |
    head -1
)
if [[ -z "$REGION_KEY" || "$REGION_KEY" == null ]]; then
  prompt_required REGION_KEY "OCIR region key"
fi
REGION_KEY=$(printf '%s' "$REGION_KEY" | tr '[:upper:]' '[:lower:]')

if [[ "$NON_INTERACTIVE" == true ]]; then
  CONTAINER_AVAILABILITY_DOMAIN=${CONTAINER_AVAILABILITY_DOMAIN:-$DEFAULT_AD}
  if [[ -n "${SUBNET_ID:-}" && -z "${VCN_ID:-}" ]]; then
    VCN_ID=$(
      oci_json network subnet get --subnet-id "$SUBNET_ID" |
        jq -r '.data."vcn-id" // empty'
    )
  fi
  if [[ -n "${DB_SECRET_OCID:-}" && ( -z "${VAULT_ID:-}" || -z "${VAULT_KEY_ID:-}" ) ]]; then
    SECRET_METADATA=$(oci_json vault secret get --secret-id "$DB_SECRET_OCID")
    VAULT_ID=${VAULT_ID:-$(jq -r '.data."vault-id" // empty' <<< "$SECRET_METADATA")}
    VAULT_KEY_ID=${VAULT_KEY_ID:-$(jq -r '.data."key-id" // empty' <<< "$SECRET_METADATA")}
  fi
fi

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

DB_SECRET_NAME=${DB_SECRET_NAME:-stream_hw_secret_key}
if [[ -z "${DB_SECRET_OCID:-}" ]]; then
  prompt_required DB_HOST "Database host"
  prompt_required DB_PORT "Database port" "3306"
  [[ "$DB_PORT" =~ ^[0-9]+$ ]] && (( DB_PORT >= 1 && DB_PORT <= 65535 )) || { echo "Database port must be 1..65535." >&2; exit 1; }
  prompt_required DB_USER "Database user"
  prompt_required DB_NAME "Default database" "${CONTROL_DATABASE:-stream_db}"
  prompt_required CONTROL_DATABASE "Control database" "stream_db"
  prompt_required STREAM_DATA_DB_NAME "Durable stream database" "stream_data"
  prompt_required STAGING_DATABASE "Staging database" "staging_db"
  if [[ -n "$DB_PASSWORD_FILE" ]]; then
    [[ -f "$DB_PASSWORD_FILE" && ! -L "$DB_PASSWORD_FILE" ]] || { echo "Password file must be a regular file." >&2; exit 1; }
    mode=$(stat -c '%a' "$DB_PASSWORD_FILE" 2>/dev/null || stat -f '%Lp' "$DB_PASSWORD_FILE")
    [[ "$mode" == 600 ]] || { echo "Password file must have mode 0600." >&2; exit 1; }
    DB_PASSWORD=$(<"$DB_PASSWORD_FILE")
    PASSWORD_FILE_CONSUMED=true
  elif [[ "$NON_INTERACTIVE" == true ]]; then
    echo "DB_SECRET_OCID or --db-password-file is required for non-interactive setup." >&2; exit 1
  else
    read -r -s -p "Database password: " DB_PASSWORD; echo
  fi
  [[ -n "$DB_PASSWORD" ]] || { echo "Database password is required." >&2; exit 1; }
  SECRET_PAYLOAD=$(jq -cn --arg host "$DB_HOST" --argjson port "$DB_PORT" --arg user "$DB_USER" --arg credential "$DB_PASSWORD" --arg database "$DB_NAME" --arg control_database "$CONTROL_DATABASE" --arg stream_data_database "$STREAM_DATA_DB_NAME" --arg staging_database "$STAGING_DATABASE" '{host:$host,port:$port,user:$user,credential:$credential,database:$database,control_database:$control_database,stream_data_database:$stream_data_database,staging_database:$staging_database}')
  SECRET_CONTENT=$(printf '%s' "$SECRET_PAYLOAD" | base64 | tr -d '\n')
  EXISTING_SECRET=$(oci_json vault secret list --compartment-id "$COMPARTMENT_ID" --vault-id "$VAULT_ID" --all | jq -r --arg name "$DB_SECRET_NAME" '.data[] | select(."secret-name" == $name and ."lifecycle-state" == "ACTIVE") | .id' | head -1)
  if [[ -n "$EXISTING_SECRET" ]]; then
    oci_json vault secret update-base64 --secret-id "$EXISTING_SECRET" --secret-content-content "$SECRET_CONTENT" --secret-content-name "${DB_SECRET_NAME}-$(date -u +%Y%m%d%H%M%S)" --secret-content-stage CURRENT >/dev/null
    DB_SECRET_OCID=$EXISTING_SECRET
  else
    DB_SECRET_OCID=$(oci_json vault secret create-base64 --compartment-id "$COMPARTMENT_ID" --vault-id "$VAULT_ID" --key-id "$VAULT_KEY_ID" --secret-name "$DB_SECRET_NAME" --description 'Processor database connectivity configuration.' --secret-content-content "$SECRET_CONTENT" --secret-content-name "${DB_SECRET_NAME}-v1" --secret-content-stage CURRENT | jq -r '.data.id')
  fi
  unset DB_PASSWORD SECRET_PAYLOAD SECRET_CONTENT
fi
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

CONTROL_DATABASE=${CONTROL_DATABASE:-stream_db}
STREAM_DATA_DB_NAME=${STREAM_DATA_DB_NAME:-stream_data}
STAGING_DATABASE=${STAGING_DATABASE:-staging_db}
prompt_required OBJECT_STORAGE_BUCKET_NAME "Object Storage bucket name"
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

write_export DB_SECRET_OCID "$DB_SECRET_OCID"
write_export DB_SECRET_NAME "$DB_SECRET_NAME"
write_export REPOSITORY_PREFIX "$REPOSITORY_PREFIX"
write_export PROCESSOR_IMAGE_NAME "$PROCESSOR_IMAGE_NAME"
write_export PROCESSOR_IMAGE_TAG "$PROCESSOR_IMAGE_TAG"
write_export FLASK_SECRET_KEY "$FLASK_SECRET_KEY"
write_export CONTROL_DATABASE "$CONTROL_DATABASE"
write_export STREAM_DATA_DB_NAME "$STREAM_DATA_DB_NAME"
write_export STAGING_DATABASE "$STAGING_DATABASE"
write_export OBJECT_STORAGE_BUCKET_NAME "$OBJECT_STORAGE_BUCKET_NAME"
write_export UI_SERVER_NAME "$UI_SERVER_NAME"
write_export UI_IMAGE_NAME "${UI_IMAGE_NAME:-object-storage-heatwave-ui}"
write_export UI_IMAGE_TAG "${UI_IMAGE_TAG:-$PROCESSOR_IMAGE_TAG}"
write_export GENERATE_SELF_SIGNED_CERT "${GENERATE_SELF_SIGNED_CERT:-true}"
write_export TLS_CERT_FILE "${TLS_CERT_FILE:-}"
write_export TLS_KEY_FILE "${TLS_KEY_FILE:-}"

mkdir -p "$(dirname "$OUTPUT")"
chmod 600 "$TMP_FILE"
mv "$TMP_FILE" "$OUTPUT"
trap - EXIT

if [[ "$PASSWORD_FILE_CONSUMED" == true ]]; then
  rm -f -- "$DB_PASSWORD_FILE"
fi

echo
echo "Created $OUTPUT (mode 0600)"
echo "Processor image: $PROCESSOR_IMAGE_URL"
echo "Next: review the file, configure TLS, then run ./tests/integration/verify_streaming_deployment.sh"
