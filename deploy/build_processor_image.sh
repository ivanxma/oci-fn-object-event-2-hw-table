#!/usr/bin/env bash
# Build and push the mode-aware OCI Streaming processor image to OCIR.
# The build host authenticates through docker-credential-ocir and its OCI
# instance principal. No static registry credential is accepted.
set -euo pipefail
umask 077
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
[[ -r "$ENV_FILE" ]] || { echo "Missing $ENV_FILE" >&2; exit 1; }
set -a; . "$ENV_FILE"; set +a
source "$ROOT_DIR/deploy/oci_context.sh"
oci_context_resolve
for value in REGION_KEY REPOSITORY_PREFIX PROCESSOR_IMAGE_NAME PROCESSOR_IMAGE_TAG; do
  [[ -n "${!value:-}" ]] || { echo "$value is required" >&2; exit 1; }
done
for command in oci docker docker-credential-ocir; do command -v "$command" >/dev/null || { echo "Missing $command" >&2; exit 1; }; done
NAMESPACE=$(oci --auth instance_principal os ns get --query data --raw-output)
OCI_REGISTRY_REPOSITORY=${OCI_REGISTRY_REPOSITORY:-"${REPOSITORY_PREFIX,,}/$PROCESSOR_IMAGE_NAME"}
IMAGE="$REGION_KEY.ocir.io/$NAMESPACE/$OCI_REGISTRY_REPOSITORY:$PROCESSOR_IMAGE_TAG"
[[ -n "$NAMESPACE" && "$NAMESPACE" != null ]] || { echo 'Could not resolve the OCIR namespace using the instance principal.' >&2; exit 1; }
mkdir -p "$HOME/.docker" "$HOME/.config/containers"
printf '{\n  "credHelpers": {\n    "%s.ocir.io": "ocir"\n  }\n}\n' "$REGION_KEY" > "$HOME/.docker/config.json"
cp "$HOME/.docker/config.json" "$HOME/.config/containers/auth.json"
chmod 700 "$HOME/.docker" "$HOME/.config/containers"
chmod 600 "$HOME/.docker/config.json" "$HOME/.config/containers/auth.json"
RELEASE_VERSION="${RELEASE_VERSION:-$PROCESSOR_IMAGE_TAG}"
GIT_SHA="${GIT_SHA:-$(git -C "$ROOT_DIR" rev-parse --short HEAD)}"
SOURCE_BRANCH="${SOURCE_BRANCH:-$(git -C "$ROOT_DIR" branch --show-current)}"
BUILD_UTC="${BUILD_UTC:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
docker build --file "$ROOT_DIR/processor/Dockerfile" --tag "$IMAGE" \
  --build-arg "RELEASE_VERSION=$RELEASE_VERSION" --build-arg "GIT_SHA=$GIT_SHA" \
  --build-arg "SOURCE_BRANCH=$SOURCE_BRANCH" --build-arg "BUILD_UTC=$BUILD_UTC" \
  --build-arg "PROCESSOR_IMAGE_NAME=$PROCESSOR_IMAGE_NAME" --build-arg "PROCESSOR_IMAGE_TAG=$PROCESSOR_IMAGE_TAG" \
  --build-arg "CONFIG_SCHEMA_VERSION=${CONFIG_SCHEMA_VERSION:-2}" "$ROOT_DIR"
docker push "$IMAGE"
IMAGE_DIGEST=$(docker inspect --format='{{index .RepoDigests 0}}' "$IMAGE" 2>/dev/null || true)
echo "Processor image pushed: $IMAGE"
echo "Processor release: $RELEASE_VERSION ($GIT_SHA, $BUILD_UTC) ${IMAGE_DIGEST:-digest-unavailable}"
