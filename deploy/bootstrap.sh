#!/usr/bin/env bash
# Install all Oracle Linux tooling required by Function, UI, and test deployment.
set -euo pipefail
umask 077

export PATH="$HOME/.fn/bin:$HOME/bin:$HOME/.local/bin:$PATH"

if ! command -v dnf >/dev/null 2>&1; then
  echo "This bootstrap script supports Oracle Linux with dnf. Run it on the deployment VM." >&2
  exit 1
fi
command -v sudo >/dev/null 2>&1 || { echo "sudo is required for host package installation." >&2; exit 1; }

# The application runtimes are Python 3.13 container images. Host Python is
# retained only for the OCI CLI installer and small non-application utilities.
# Installing the complete package set here avoids conditional gaps later when
# nginx/openssl happen to exist but SELinux or firewall helpers do not.
sudo dnf install -y \
  ca-certificates coreutils curl firewalld git gzip jq nginx openssl \
  podman policycoreutils-python-utils python3 python3-pip tar

BOOTSTRAP_TMP=$(mktemp -d)
cleanup() { rm -rf "$BOOTSTRAP_TMP"; }
trap cleanup EXIT

# OCI CLI is commonly preinstalled on OCI Compute. Use its supported user-local
# installer without modifying the system Python environment when it is absent.
if ! command -v oci >/dev/null 2>&1; then
  curl --fail --silent --show-error --location \
    https://raw.githubusercontent.com/oracle/oci-cli/master/scripts/install/install.sh \
    --output "$BOOTSTRAP_TMP/oci-cli-install.sh"
  bash "$BOOTSTRAP_TMP/oci-cli-install.sh" --accept-all-defaults
  hash -r
fi

if ! command -v fn >/dev/null 2>&1; then
  curl --fail --silent --show-error --location \
    https://raw.githubusercontent.com/fnproject/cli/master/install \
    --output "$BOOTSTRAP_TMP/fn-install.sh"
  bash "$BOOTSTRAP_TMP/fn-install.sh"
  hash -r
fi

for command in curl git jq podman python3 oci fn nginx openssl setsebool firewall-cmd timeout tar gzip; do
  command -v "$command" >/dev/null 2>&1 || {
    echo "Bootstrap completed package installation, but required command '$command' is still unavailable." >&2
    exit 1
  }
done

mkdir -p "$HOME/.fn"
touch "$HOME/.fn/config.yaml"
chmod 600 "$HOME/.fn/config.yaml"
if grep -q '^container-enginetype:' "$HOME/.fn/config.yaml"; then
  sed -i 's/^container-enginetype:.*/container-enginetype: podman/' "$HOME/.fn/config.yaml"
else
  printf '\ncontainer-enginetype: podman\n' >> "$HOME/.fn/config.yaml"
fi

# The deployment scripts set this PATH themselves. Persisting it also makes the
# tools available to operators in a fresh interactive shell.
PATH_LINE='export PATH="$HOME/.fn/bin:$HOME/bin:$HOME/.local/bin:$PATH"'
touch "$HOME/.bashrc"
grep -Fqx "$PATH_LINE" "$HOME/.bashrc" || printf '\n%s\n' "$PATH_LINE" >> "$HOME/.bashrc"

podman info >/dev/null

echo "Bootstrap complete. Installed tooling:"
oci --version
fn version
podman --version
python3 --version
nginx -v
echo "Application containers use Python 3.13; host Python is not used as the Flask or Function runtime."
