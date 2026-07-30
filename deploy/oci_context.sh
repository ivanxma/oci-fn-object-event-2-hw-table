#!/usr/bin/env bash
# Resolve OCI placement from the deployment VM. Source after deploy/env.sh.
set -euo pipefail

oci_context_resolve() {
  command -v oci >/dev/null || { echo 'Missing OCI CLI for OCI context discovery.' >&2; return 1; }
  command -v jq >/dev/null || { echo 'Missing jq for OCI context discovery.' >&2; return 1; }
  local meta instance_id vnic_id vm_subnet_id subnet_json candidates candidate_count
  meta=$(curl -fsS --connect-timeout 1 --max-time 2 -H 'Authorization: Bearer Oracle' http://169.254.169.254/opc/v2/instance/ 2>/dev/null || true)
  local metadata_json="${meta:-}"; metadata_json=${metadata_json:-"{}"}
  COMPARTMENT_ID=${COMPARTMENT_ID:-$(jq -r '.compartmentId // empty' <<< "$metadata_json")}
  REGION=${REGION:-$(jq -r '.region // empty' <<< "$metadata_json")}
  CONTAINER_AVAILABILITY_DOMAIN=${CONTAINER_AVAILABILITY_DOMAIN:-$(jq -r '.availabilityDomain // empty' <<< "$metadata_json")}
  instance_id=$(jq -r '.id // empty' <<< "$metadata_json")
  [[ -n "$COMPARTMENT_ID" && -n "$REGION" ]] || { echo 'Could not derive compartment and region from VM metadata.' >&2; return 1; }
  REGION_KEY=${REGION_KEY:-$(oci --auth instance_principal --region "$REGION" iam region list --all --output json | jq -r --arg r "$REGION" '.data[] | select(.name == $r) | .key' | head -1 | tr '[:upper:]' '[:lower:]')}
  if [[ -n "$instance_id" ]]; then
    vnic_id=$(oci --auth instance_principal --region "$REGION" compute vnic-attachment list --compartment-id "$COMPARTMENT_ID" --instance-id "$instance_id" --all --output json | jq -r '.data[] | select(."lifecycle-state" == "ATTACHED") | ."vnic-id"' | head -1)
    [[ -z "$vnic_id" ]] || vm_subnet_id=$(oci --auth instance_principal --region "$REGION" network vnic get --vnic-id "$vnic_id" --output json | jq -r '.data."subnet-id" // empty')
  fi
  if [[ -n "${vm_subnet_id:-}" ]]; then
    subnet_json=$(oci --auth instance_principal --region "$REGION" network subnet get --subnet-id "$vm_subnet_id" --output json)
    VCN_ID=${VCN_ID:-$(jq -r '.data."vcn-id" // empty' <<< "$subnet_json")}
  fi
  [[ -n "${VCN_ID:-}" ]] || { echo 'Could not derive the UI VM VCN.' >&2; return 1; }

  if [[ -n "${SUBNET_ID:-}" ]]; then
    subnet_json=$(oci --auth instance_principal --region "$REGION" network subnet get --subnet-id "$SUBNET_ID" --output json)
    [[ "$(jq -r '.data."vcn-id" // empty' <<< "$subnet_json")" == "$VCN_ID" ]] || {
      echo 'The Processor subnet must be in the UI VM VCN.' >&2; return 1;
    }
    [[ "$(jq -r '.data."prohibit-public-ip-on-vnic" // false' <<< "$subnet_json")" == true ]] || {
      echo 'The Processor subnet must prohibit public IPs.' >&2; return 1;
    }
  else
    candidates=$(
      oci --auth instance_principal --region "$REGION" network subnet list \
        --compartment-id "$COMPARTMENT_ID" --vcn-id "$VCN_ID" --all --output json |
        jq -r --arg ad "$CONTAINER_AVAILABILITY_DOMAIN" '.data[] |
          select(."lifecycle-state" == "AVAILABLE") |
          select(."prohibit-public-ip-on-vnic" == true) |
          select((."availability-domain" // "") == "" or ."availability-domain" == $ad) |
          .id'
    )
    candidate_count=$(grep -c . <<< "$candidates" || true)
    if ((candidate_count == 1)); then
      SUBNET_ID=$candidates
    elif ((candidate_count == 0)); then
      echo 'No available private Processor subnet exists in the UI VM VCN and availability domain.' >&2
      return 1
    else
      echo 'Multiple private Processor subnets exist in the UI VM VCN; select one during interactive setup or provide a runtime SUBNET_ID override.' >&2
      return 1
    fi
  fi
  export COMPARTMENT_ID REGION REGION_KEY CONTAINER_AVAILABILITY_DOMAIN VCN_ID SUBNET_ID
}
