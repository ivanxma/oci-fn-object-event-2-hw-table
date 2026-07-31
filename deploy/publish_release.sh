#!/usr/bin/env bash
# Publish versioned Processor and UI images, then activate the UI release.
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
VERSION=""
DEPLOY_UI=true

usage() {
  cat <<'EOF'
Usage: ./deploy/publish_release.sh [--version VERSION] [--skip-ui]

Publishes immutable tags processor-VERSION and ui-VERSION to the single OCIR
repository configured by OCI_REGISTRY_REPOSITORY_ID. The UI image is built,
pushed, and activated by deploy_ui.sh. VERSION defaults to the current short
Git revision. --skip-ui publishes only the Processor image.
EOF
}

while (($#)); do
  case "$1" in
    --version)
      (($# >= 2)) || { echo "--version requires a value." >&2; exit 2; }
      VERSION=$2
      shift 2
      ;;
    --skip-ui)
      DEPLOY_UI=false
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

VERSION="${VERSION:-$(git -C "$ROOT_DIR" rev-parse --short HEAD)}"
VERSION="${VERSION#processor-}"
VERSION="${VERSION#ui-}"
[[ "$VERSION" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,126}$ ]] || {
  echo "VERSION must contain only letters, digits, dot, underscore, or hyphen." >&2
  exit 1
}

export RELEASE_VERSION="$VERSION"
export PROCESSOR_IMAGE_TAG_OVERRIDE="$VERSION"
export UI_IMAGE_TAG_OVERRIDE="$VERSION"

"$ROOT_DIR/deploy/build_processor_image.sh"
if [[ "$DEPLOY_UI" == true ]]; then
  MIGRATION_PYTHON="${DEPLOYMENT_PYTHON_BIN:-$ROOT_DIR/.venv-verification-py312/bin/python}"
  if [[ ! -x "$MIGRATION_PYTHON" ]] && command -v python3 >/dev/null &&
     python3 -c 'import mysql.connector, oci' >/dev/null 2>&1; then
    MIGRATION_PYTHON=$(command -v python3)
  fi
  [[ -x "$MIGRATION_PYTHON" ]] || {
    echo "A Python runtime with MySQL Connector and OCI SDK is required for release migrations." >&2
    exit 1
  }
  OCI_AUTH_MODE=instance_principal "$MIGRATION_PYTHON" "$ROOT_DIR/deploy/initialize_databases.py"
  "$ROOT_DIR/deploy/deploy_ui.sh"
  echo "Release published: processor-$VERSION and ui-$VERSION"
else
  echo "Release published: processor-$VERSION"
fi
