#!/usr/bin/env bash
# One-command, non-interactive installation for a fresh Oracle Linux validation VM.
set -euo pipefail
umask 077

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$ROOT_DIR/deploy/ol9_packages.sh"
CONFIG_FILE=""
INSTALL_UI="${INSTALL_UI:-true}"

usage() {
  cat <<'EOF'
Usage: ./deploy/install_validation_vm.sh --config PATH

The config is a mode-0600 shell file containing at least:
  export OBJECT_STORAGE_BUCKET_NAME='existing-bucket'
  export DB_HOST='mysql-private-host'
  export DB_USER='stream_user'
  export DB_PASSWORD_FILE='/absolute/path/to/mode-0600-password-file'

The installer creates or updates the default OCI Vault database secret
stream_hw_secret_key and removes DB_PASSWORD_FILE after env.sh is written.
When the compartment has multiple active Vaults or AES keys, also provide
VAULT_ID and VAULT_KEY_ID. An existing DB_SECRET_OCID is supported only for
reuse/migration and is not the clean-install path.

Optional values include DB_PORT (default: 3306), DB_NAME,
CONTROL_DATABASE (default: stream_db),
STREAM_DATA_DB_NAME (default: stream_data), STAGING_DATABASE (default:
staging_db), REPOSITORY_PREFIX, PROCESSOR_IMAGE_TAG,
GENERATE_SELF_SIGNED_CERT, UI_SERVER_NAME, and INSTALL_UI.
Runtime database access uses only the generated Vault secret OCID, and OCIR
uses the instance principal.
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

for value in OBJECT_STORAGE_BUCKET_NAME; do
  [[ -n "${!value:-}" ]] || { echo "$value is required in $CONFIG_FILE." >&2; exit 1; }
done
if [[ -n "${DB_SECRET_OCID:-}" ]]; then
  [[ "$DB_SECRET_OCID" == ocid1.vaultsecret.* ]] || { echo "DB_SECRET_OCID is invalid." >&2; exit 1; }
else
  for value in DB_HOST DB_USER DB_PASSWORD_FILE; do
    [[ -n "${!value:-}" ]] || { echo "$value is required for a clean installation." >&2; exit 1; }
  done
  [[ "$DB_PASSWORD_FILE" == /* ]] || { echo "DB_PASSWORD_FILE must be an absolute path." >&2; exit 1; }
  [[ -f "$DB_PASSWORD_FILE" && ! -L "$DB_PASSWORD_FILE" ]] || { echo "DB_PASSWORD_FILE must be a regular file." >&2; exit 1; }
  password_mode=$(stat -c '%a' "$DB_PASSWORD_FILE" 2>/dev/null || stat -f '%Lp' "$DB_PASSWORD_FILE")
  [[ "$password_mode" == 600 ]] || { echo "DB_PASSWORD_FILE must have mode 0600." >&2; exit 1; }
fi
export DB_PORT="${DB_PORT:-3306}"
export DB_NAME="${DB_NAME:-${TARGET_DATABASE:-target_db}}"
export CONTROL_DATABASE="${CONTROL_DATABASE:-stream_db}"
export STREAM_DATA_DB_NAME="${STREAM_DATA_DB_NAME:-stream_data}"
export STAGING_DATABASE="${STAGING_DATABASE:-staging_db}"
"$ROOT_DIR/deploy/bootstrap_streaming.sh"
export REPOSITORY_PREFIX="${REPOSITORY_PREFIX:-object-storage-heatwave-validation}"
export PROCESSOR_IMAGE_TAG="${PROCESSOR_IMAGE_TAG:-validation-$(date -u +%Y%m%d%H%M%S)}"
export GENERATE_SELF_SIGNED_CERT="${GENERATE_SELF_SIGNED_CERT:-true}"
"$ROOT_DIR/deploy/setup_env.sh" --non-interactive --force
if [[ -n "${DB_PASSWORD_FILE:-}" && -e "$DB_PASSWORD_FILE" ]]; then
  echo "Password file was not removed after Vault secret creation." >&2
  exit 1
fi
unset DB_PASSWORD_FILE
set -a
# setup_env.sh is a child process, so load its generated, secret-free runtime
# references into this installer before database initialization and deployment.
# shellcheck disable=SC1091
. "$ROOT_DIR/deploy/env.sh"
set +a
[[ "${DB_SECRET_OCID:-}" == ocid1.vaultsecret.* ]] || {
  echo "Generated env.sh does not contain a valid DB_SECRET_OCID." >&2
  exit 1
}

ol9_dnf_install python3.12 python3.12-pip
VERIFY_PYTHON="$ROOT_DIR/.venv-verification-py312/bin/python"
python3.12 -m venv "$ROOT_DIR/.venv-verification-py312"
"$VERIFY_PYTHON" -m pip install --upgrade pip >/dev/null
"$VERIFY_PYTHON" -m pip install "mysql-connector-python==9.7.0" "oci==2.183.0" >/dev/null

OCI_AUTH_MODE=instance_principal "$VERIFY_PYTHON" "$ROOT_DIR/deploy/initialize_databases.py"
"$ROOT_DIR/tests/integration/verify_durable_capture.sh"
"$ROOT_DIR/tests/integration/verify_streaming_deployment.sh"
"$ROOT_DIR/deploy/build_processor_image.sh"

if [[ "$INSTALL_UI" == true ]]; then
  "$ROOT_DIR/deploy/deploy_ui.sh"
fi

echo "PASS: non-interactive validation VM installation completed."
