#!/usr/bin/env bash
# Show the deployed UI, OCI Processor instances, release metadata, and UI logs.
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
LOG_LINES=50
SHOW_LOGS=true

usage() {
  cat <<'EOF'
Usage: ./deploy/service_status.sh [--log-lines NUMBER] [--no-logs]

Run this command on the UI/deployment VM. It reports:
  - current checkout and deployed UI release metadata;
  - systemd, Podman, and local HTTPS status for the Flask UI;
  - live OCI Container Instance Processor state, image, and release tag; and
  - recent UI service logs (unless --no-logs is supplied).

Processor stdout/stderr is not exported to the VM by this deployment. Review
processor failures in Event Processor and Durable Messages in the UI, or use
your separately configured OCI Logging destination.
EOF
}

while (($#)); do
  case "$1" in
    --log-lines)
      (($# >= 2)) || { echo "--log-lines requires a number." >&2; exit 2; }
      LOG_LINES=$2
      shift 2
      ;;
    --no-logs)
      SHOW_LOGS=false
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

[[ "$LOG_LINES" =~ ^[1-9][0-9]*$ ]] || { echo "--log-lines must be a positive integer." >&2; exit 2; }
[[ -r "$ENV_FILE" ]] || { echo "Missing deployment environment: $ENV_FILE" >&2; exit 1; }
for command in curl git jq oci podman sudo systemctl; do
  command -v "$command" >/dev/null || { echo "Missing required command: $command" >&2; exit 1; }
done

set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a
# shellcheck disable=SC1091
. "$ROOT_DIR/deploy/oci_context.sh"
oci_context_resolve

UI_SERVICE_NAME="${UI_SERVICE_NAME:-object-storage-heatwave-ui}"
UI_CONTAINER_NAME="${UI_CONTAINER_NAME:-$UI_SERVICE_NAME}"
PROCESSOR_CONTAINER_NAME_PREFIX="${PROCESSOR_CONTAINER_NAME_PREFIX:-object-storage-stream-processor}"
RUNTIME_ENV="$ROOT_DIR/ui/.ui-runtime.env"

heading() {
  printf '\n== %s ==\n' "$1"
}

heading "Current source"
printf 'Branch: %s\n' "$(git -C "$ROOT_DIR" branch --show-current)"
printf 'Commit: %s\n' "$(git -C "$ROOT_DIR" rev-parse --short HEAD)"
if [[ -n "$(git -C "$ROOT_DIR" status --porcelain)" ]]; then
  echo 'Worktree: changes present'
else
  echo 'Worktree: clean'
fi
if [[ -r "$RUNTIME_ENV" ]]; then
  awk -F= '/^(RELEASE_VERSION|GIT_SHA|SOURCE_BRANCH|BUILD_UTC|UI_IMAGE_TAG)=/ { print $1 ": " $2 }' "$RUNTIME_ENV"
else
  echo 'Deployed release metadata: unavailable (UI runtime environment not found)'
fi

heading "UI"
ui_state=$(sudo systemctl is-active "$UI_SERVICE_NAME" 2>/dev/null || true)
printf 'Service: %s\n' "${ui_state:-unknown}"
sudo systemctl show "$UI_SERVICE_NAME" --no-pager --property=ActiveState,SubState,MainPID 2>/dev/null || true
printf 'Container: '
sudo podman ps --filter "name=^${UI_CONTAINER_NAME}$" --format '{{.Names}} {{.Status}} {{.Image}}' || true
printf 'HTTPS localhost: '
curl -kfsS -o /dev/null -w '%{http_code}\n' https://127.0.0.1/ || echo 'unavailable'

heading "Processor instances"
processor_ids=$(
  oci --auth instance_principal --region "$REGION" container-instances container-instance list \
    --compartment-id "$COMPARTMENT_ID" --all --output json |
    jq -r --arg prefix "$PROCESSOR_CONTAINER_NAME_PREFIX" '
      .data.items[] |
      select(."freeform-tags"."managed-by" == "oci-object-event-2-table" or
             (."display-name" | startswith($prefix))) |
      .id'
)
if [[ -z "$processor_ids" ]]; then
  echo 'No managed Processor container instances found.'
else
  while IFS= read -r processor_id; do
    processor=$(oci --auth instance_principal --region "$REGION" container-instances container-instance get --container-instance-id "$processor_id" --output json)
    jq -r '
      .data as $processor |
      "Name: " + ($processor."display-name" // "unknown"),
      "State: " + ($processor."lifecycle-state" // "unknown"),
      "Created: " + ($processor."time-created" // "unknown"),
      "Release: " + ($processor."freeform-tags"."release-version" // "unknown"),
      "Commit: " + ($processor."freeform-tags"."git-sha" // "unknown"),
      "Image: " + ($processor.containers[0]."image-url" // "unknown"),
      "ID: " + ($processor.id // "unknown"),
      ""
    ' <<< "$processor"
  done <<< "$processor_ids"
fi
echo 'Processor logs: not exported to this VM. Use Event Processor / Durable Messages for captured failures, or the configured OCI Logging destination.'

if [[ "$SHOW_LOGS" == true ]]; then
  heading "UI logs (latest $LOG_LINES lines)"
  sudo journalctl --no-pager -u "$UI_SERVICE_NAME" -n "$LOG_LINES" -o short-iso || true
fi
