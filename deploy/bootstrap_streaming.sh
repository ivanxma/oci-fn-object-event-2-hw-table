#!/usr/bin/env bash
# Fresh Oracle Linux VM prerequisites for UI + OCI Streaming processor validation.
set -euo pipefail
command -v dnf >/dev/null || { echo 'Oracle Linux with dnf is required.' >&2; exit 1; }
sudo dnf install -y ca-certificates curl git golang jq podman podman-docker python3 python3-pip
if ! command -v oci >/dev/null; then
  curl -fsSL https://raw.githubusercontent.com/oracle/oci-cli/master/scripts/install/install.sh -o /tmp/oci-cli-install.sh
  bash /tmp/oci-cli-install.sh --accept-all-defaults
fi
export PATH="$HOME/bin:$PATH"
for command in git go jq docker podman python3 oci; do command -v "$command" >/dev/null || { echo "Missing $command" >&2; exit 1; }; done
if ! command -v docker-credential-ocir >/dev/null; then
  HELPER_DIR=$(mktemp -d)
  trap 'rm -rf "$HELPER_DIR"' EXIT
  git clone --depth 1 https://github.com/jan-g/ip-credential.git "$HELPER_DIR/ip-credential"
  (
    cd "$HELPER_DIR/ip-credential"
    go mod vendor
    go build -o "$HELPER_DIR/docker-credential-ocir" docker-credential-ocir.go
  )
  sudo install -m 0755 "$HELPER_DIR/docker-credential-ocir" /usr/local/bin/docker-credential-ocir
fi
command -v docker-credential-ocir >/dev/null || { echo 'Missing docker-credential-ocir.' >&2; exit 1; }
oci --auth instance_principal iam region list >/dev/null
echo 'PASS: streaming validation VM prerequisites and instance-principal OCIR helper installed'
