#!/usr/bin/env bash
# One-command, non-interactive installation for a fresh Oracle Linux validation VM.
set -euo pipefail
umask 077

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONFIG_FILE=""
INSTALL_UI="${INSTALL_UI:-true}"

usage() {
  cat <<'EOF'
Usage: ./deploy/install_validation_vm.sh --config PATH

The config is a mode-0600 shell file containing at least:
  export DB_SECRET_OCID='ocid1.vaultsecret...'
  export OBJECT_STORAGE_BUCKET_NAME='existing-bucket'
  export CONTROL_DATABASE='stream_db_validation'
  export STREAM_DATA_DB_NAME='stream_data_validation'

Optional values include REPOSITORY_PREFIX, PROCESSOR_IMAGE_TAG,
GENERATE_SELF_SIGNED_CERT, UI_SERVER_NAME, and INSTALL_UI.
Database access uses only the Vault secret OCID, and OCIR uses the instance principal.
EOF
}

while (($#)); do
  case "$1" in
    --config)
      (($# >= 2)) || { echo "--config requires a path." >&2; exit 2; }
      CONFIG_FILE=$2
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

[[ -n "$CONFIG_FILE" && -r "$CONFIG_FILE" ]] || { echo "A readable --config file is required." >&2; exit 1; }
mode=$(stat -c '%a' "$CONFIG_FILE" 2>/dev/null || stat -f '%Lp' "$CONFIG_FILE")
[[ "$mode" == 600 ]] || { echo "Validation config must be mode 0600." >&2; exit 1; }

set -a
# shellcheck disable=SC1090
. "$CONFIG_FILE"
set +a

for value in DB_SECRET_OCID OBJECT_STORAGE_BUCKET_NAME CONTROL_DATABASE STREAM_DATA_DB_NAME; do
  [[ -n "${!value:-}" ]] || { echo "$value is required in $CONFIG_FILE." >&2; exit 1; }
done
[[ "$DB_SECRET_OCID" == ocid1.vaultsecret.* ]] || { echo "DB_SECRET_OCID is invalid." >&2; exit 1; }
"$ROOT_DIR/deploy/bootstrap_streaming.sh"
export REPOSITORY_PREFIX="${REPOSITORY_PREFIX:-object-storage-heatwave-validation}"
export PROCESSOR_IMAGE_TAG="${PROCESSOR_IMAGE_TAG:-validation-$(date -u +%Y%m%d%H%M%S)}"
export GENERATE_SELF_SIGNED_CERT="${GENERATE_SELF_SIGNED_CERT:-true}"
"$ROOT_DIR/deploy/setup_env.sh" --non-interactive --force

sudo dnf install -y python3.12 python3.12-pip
VERIFY_PYTHON="$ROOT_DIR/.venv-verification-py312/bin/python"
python3.12 -m venv "$ROOT_DIR/.venv-verification-py312"
"$VERIFY_PYTHON" -m pip install --upgrade pip >/dev/null
"$VERIFY_PYTHON" -m pip install "mysql-connector-python==9.7.0" "oci==2.183.0" >/dev/null

"$ROOT_DIR/deploy/build_processor_image.sh"
"$ROOT_DIR/tests/integration/verify_streaming_deployment.sh"
OCI_AUTH_MODE=instance_principal "$VERIFY_PYTHON" "$ROOT_DIR/deploy/initialize_databases.py"
"$ROOT_DIR/tests/integration/verify_durable_capture.sh"

if [[ "$INSTALL_UI" == true ]]; then
  "$ROOT_DIR/deploy/deploy_ui.sh"
fi

echo "PASS: non-interactive validation VM installation completed."
