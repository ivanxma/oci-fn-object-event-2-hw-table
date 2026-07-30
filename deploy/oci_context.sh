#!/usr/bin/env bash
# Resolve OCI placement from the deployment VM. Source after deploy/env.sh.
set -euo pipefail

oci_context_resolve() {
  command -v oci >/dev/null || { echo 'Missing OCI CLI for OCI context discovery.' >&2; return 1; }
  command -v jq >/dev/null || { echo 'Missing jq for OCI context discovery.' >&2; return 1; }
  local meta instance_id vnic_id
  meta=$(curl -fsS --connect-timeout 1 --max-time 2 -H 'Authorization: Bearer Oracle' http://169.254.169.254/opc/v2/instance/ 2>/dev/null || true)
  COMPARTMENT_ID=${COMPARTMENT_ID:-$(jq -r '.compartmentId // empty' <<< "${meta:-{}}")}
  REGION=${REGION:-$(jq -r '.region // empty' <<< "${meta:-{}}")}
  CONTAINER_AVAILABILITY_DOMAIN=${CONTAINER_AVAILABILITY_DOMAIN:-$(jq -r '.availabilityDomain // empty' <<< "${meta:-{}}")}
  instance_id=$(jq -r '.id // empty' <<< "${meta:-{}}")
  [[ -n "$COMPARTMENT_ID" && -n "$REGION" ]] || { echo 'Could not derive compartment and region from VM metadata.' >&2; return 1; }
  REGION_KEY=${REGION_KEY:-$(oci --auth instance_principal --region "$REGION" iam region list --all --output json | jq -r --arg r "$REGION" '.data[] | select(.name == $r) | .key' | head -1 | tr '[:upper:]' '[:lower:]')}
  if [[ -z "${SUBNET_ID:-}" && -n "$instance_id" ]]; then
    vnic_id=$(oci --auth instance_principal --region "$REGION" compute vnic-attachment list --compartment-id "$COMPARTMENT_ID" --instance-id "$instance_id" --all --output json | jq -r '.data[] | select(."lifecycle-state" == "ATTACHED") | ."vnic-id"' | head -1)
    [[ -z "$vnic_id" ]] || SUBNET_ID=$(oci --auth instance_principal --region "$REGION" network vnic get --vnic-id "$vnic_id" --output json | jq -r '.data."subnet-id" // empty')
  fi
  export COMPARTMENT_ID REGION REGION_KEY CONTAINER_AVAILABILITY_DOMAIN SUBNET_ID
}
