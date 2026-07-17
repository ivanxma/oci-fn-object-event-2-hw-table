#!/usr/bin/env bash
# Installs the local tooling required to build and deploy this OCI Function.
set -euo pipefail

if ! command -v dnf >/dev/null; then
  echo "This bootstrap script currently supports Oracle Linux (dnf)." >&2
  exit 1
fi

# Oracle Linux 9's enabled repositories commonly ship Python 3.9 as `python3`;
# the host only needs it for OCI/Fn tooling.  The deployed function itself is
# built from the Python 3.13 image specified in Dockerfile.
sudo dnf install -y curl ca-certificates git jq podman python3 python3-pip

# OCI CLI is often preinstalled on OCI Compute.  Keep a user-local installation
# fallback so no system Python packages are modified.
if ! command -v oci >/dev/null; then
  curl -fsSL https://raw.githubusercontent.com/oracle/oci-cli/master/scripts/install/install.sh \
    -o /tmp/oci-cli-install.sh
  bash /tmp/oci-cli-install.sh --accept-all-defaults
  export PATH="$HOME/bin:$PATH"
fi

if ! command -v fn >/dev/null; then
  curl -LSs https://raw.githubusercontent.com/fnproject/cli/master/install | sh
  export PATH="$HOME/.fn/bin:$HOME/bin:$PATH"
fi

mkdir -p "$HOME/.fn"
if ! grep -q '^container-enginetype: podman$' "$HOME/.fn/config.yaml" 2>/dev/null; then
  printf '\ncontainer-enginetype: podman\n' >> "$HOME/.fn/config.yaml"
fi

echo "Installed tooling:"
oci --version
fn version
podman --version
echo "For this shell, use: export PATH=\"$HOME/.fn/bin:$HOME/bin:\$PATH\""
