#!/usr/bin/env bash
# Deploy the Flask UI as a systemd-managed Podman container behind HTTPS.
# The container is intentionally bound only to localhost; nginx owns port 443.
set -euo pipefail
umask 077

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$ROOT_DIR/deploy/ol9_packages.sh"
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
UI_IMAGE_TAG_OVERRIDE="${UI_IMAGE_TAG_OVERRIDE:-}"
[[ -r "$ENV_FILE" ]] || { echo "Copy deploy/env.sh.example to deploy/env.sh and set deployment values." >&2; exit 1; }
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a
source "$ROOT_DIR/deploy/oci_context.sh"
oci_context_resolve

for command in oci podman docker-credential-ocir sudo systemctl; do
  command -v "$command" >/dev/null || { echo "Missing $command." >&2; exit 1; }
done
[[ -n "${FLASK_SECRET_KEY:-}" ]] || { echo "FLASK_SECRET_KEY must be set in $ENV_FILE." >&2; exit 1; }

UI_SERVICE_NAME="${UI_SERVICE_NAME:-object-storage-heatwave-ui}"
UI_CONTAINER_NAME="${UI_CONTAINER_NAME:-$UI_SERVICE_NAME}"
UI_IMAGE_NAME="${UI_IMAGE_NAME:-object-storage-heatwave-ui}"
UI_IMAGE_TAG="${UI_IMAGE_TAG_OVERRIDE:-${UI_IMAGE_TAG:-$(git -C "$ROOT_DIR" rev-parse --short HEAD)}}"
RELEASE_VERSION="${RELEASE_VERSION:-$UI_IMAGE_TAG}"
GIT_SHA="${GIT_SHA:-$(git -C "$ROOT_DIR" rev-parse --short HEAD)}"
SOURCE_BRANCH="${SOURCE_BRANCH:-$(git -C "$ROOT_DIR" branch --show-current)}"
BUILD_UTC="${BUILD_UTC:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
UI_BIND_PORT="${UI_BIND_PORT:-8080}"
# Connection profiles and authenticated DB sessions are intentionally held in
# process memory and are not serializable. Keep one Gunicorn process and use
# threads for concurrency so every request sees the same server-side session.
UI_WORKERS="${UI_WORKERS:-1}"
UI_THREADS="${UI_THREADS:-8}"
UI_SERVER_NAME="${UI_SERVER_NAME:-_}"
CONTROL_DATABASE="${CONTROL_DATABASE:-stream_db}"
STREAM_DATA_DB_NAME="${STREAM_DATA_DB_NAME:-stream_data}"
STAGING_DATABASE="${STAGING_DATABASE:-staging_db}"
PROCESSOR_SHAPE="${PROCESSOR_SHAPE:-CI.Standard.E4.Flex}"
PROCESSOR_OCPUS="${PROCESSOR_OCPUS:-1}"
PROCESSOR_MEMORY_GBS="${PROCESSOR_MEMORY_GBS:-16}"
WRITER_WORKERS="${WRITER_WORKERS:-4}"
[[ -n "${OCI_REGISTRY_REPOSITORY_ID:-}" ]] || {
  echo "OCI_REGISTRY_REPOSITORY_ID is required to publish and deploy the UI image." >&2
  exit 1
}
REPOSITORY_JSON=$(oci --auth instance_principal --region "$REGION" artifacts container repository get --repository-id "$OCI_REGISTRY_REPOSITORY_ID" --output json)
RESOLVED_REPOSITORY=$(jq -r '.data."display-name" // empty' <<< "$REPOSITORY_JSON")
[[ "$(jq -r '.data."compartment-id" // empty' <<< "$REPOSITORY_JSON")" == "$COMPARTMENT_ID" &&
   "$(jq -r '.data."lifecycle-state" // empty' <<< "$REPOSITORY_JSON")" == "AVAILABLE" ]] || {
  echo "The configured OCI Container Registry repository is unavailable or outside the deployment compartment." >&2
  exit 1
}
[[ -z "${OCI_REGISTRY_REPOSITORY:-}" || "$OCI_REGISTRY_REPOSITORY" == "$RESOLVED_REPOSITORY" ]] || {
  echo "OCI_REGISTRY_REPOSITORY does not match OCI_REGISTRY_REPOSITORY_ID." >&2
  exit 1
}
OCI_REGISTRY_REPOSITORY=$RESOLVED_REPOSITORY
OBJECT_STORAGE_NAMESPACE=${OBJECT_STORAGE_NAMESPACE:-$(oci --auth instance_principal --region "$REGION" os ns get --query data --raw-output)}
[[ -n "$OBJECT_STORAGE_NAMESPACE" && "$OBJECT_STORAGE_NAMESPACE" != null ]] || {
  echo "Could not resolve the Object Storage/OCIR namespace." >&2
  exit 1
}
case "$UI_IMAGE_TAG" in
  ui-*) UI_REGISTRY_IMAGE_TAG="$UI_IMAGE_TAG" ;;
  *) UI_REGISTRY_IMAGE_TAG="ui-$UI_IMAGE_TAG" ;;
esac
UI_REGISTRY_IMAGE_NAME="$REGION_KEY.ocir.io/$OBJECT_STORAGE_NAMESPACE/$OCI_REGISTRY_REPOSITORY"
UI_IMAGE="$UI_REGISTRY_IMAGE_NAME:$UI_REGISTRY_IMAGE_TAG"
REGISTRY_AUTH_FILE="$HOME/.config/containers/auth.json"
OCI_EVENT_RULE_MANAGEMENT_ENABLED="${OCI_EVENT_RULE_MANAGEMENT_ENABLED:-true}"
OCI_STREAMING_MANAGEMENT_ENABLED="${OCI_STREAMING_MANAGEMENT_ENABLED:-true}"
OCI_CONTAINER_ORCHESTRATION_ENABLED="${OCI_CONTAINER_ORCHESTRATION_ENABLED:-true}"
OCI_EVENT_RULE_PREFIX="${OCI_EVENT_RULE_PREFIX:-object-storage-heatwave}"
RUNTIME_ENV="$ROOT_DIR/ui/.ui-runtime.env"
INSTANCE_DIR="$ROOT_DIR/ui/instance"
SOURCE_TLS_CERT_FILE="${TLS_CERT_FILE:-}"
SOURCE_TLS_KEY_FILE="${TLS_KEY_FILE:-}"
DEPLOY_TLS_DIR="/etc/$UI_SERVICE_NAME/tls"
TLS_CERT_FILE="$DEPLOY_TLS_DIR/tls.crt"
TLS_KEY_FILE="$DEPLOY_TLS_DIR/tls.key"
CURRENT_USER=$(id -un)
CURRENT_GROUP=$(id -gn)

if [[ "$OCI_EVENT_RULE_MANAGEMENT_ENABLED" == "true" || "$OCI_STREAMING_MANAGEMENT_ENABLED" == "true" ]]; then
  for value in COMPARTMENT_ID REGION; do
    [[ -n "${!value:-}" ]] || { echo "$value is required when OCI Streaming or Events management is enabled." >&2; exit 1; }
  done
fi
if [[ "$OCI_CONTAINER_ORCHESTRATION_ENABLED" == "true" ]]; then
  for value in SUBNET_ID CONTAINER_AVAILABILITY_DOMAIN PROCESSOR_SHAPE PROCESSOR_OCPUS PROCESSOR_MEMORY_GBS DB_SECRET_OCID; do
    [[ -n "${!value:-}" ]] || { echo "$value is required when Container orchestration is enabled." >&2; exit 1; }
  done
fi

case "$UI_BIND_PORT" in
  ''|*[!0-9]*) echo "UI_BIND_PORT must be numeric." >&2; exit 1 ;;
esac

if ! command -v nginx >/dev/null || ! command -v openssl >/dev/null; then
  ol9_dnf_install nginx openssl policycoreutils-python-utils
fi

mkdir -p "$INSTANCE_DIR"
sudo chown -R "$CURRENT_USER:$CURRENT_GROUP" "$INSTANCE_DIR"
chmod 700 "$INSTANCE_DIR"
sudo install -d -m 700 "$DEPLOY_TLS_DIR"
if [[ -z "$SOURCE_TLS_CERT_FILE" || -z "$SOURCE_TLS_KEY_FILE" ]]; then
  if [[ "${GENERATE_SELF_SIGNED_CERT:-false}" != "true" ]]; then
    echo "TLS_CERT_FILE and TLS_KEY_FILE must reference readable certificate files; set GENERATE_SELF_SIGNED_CERT=true for a temporary self-signed certificate." >&2
    exit 1
  fi
  sudo openssl req -x509 -newkey rsa:4096 -sha256 -nodes -days 365 \
    -keyout "$TLS_KEY_FILE" -out "$TLS_CERT_FILE" -subj "/CN=${UI_SERVER_NAME}" >/dev/null 2>&1
else
  [[ -r "$SOURCE_TLS_CERT_FILE" && -r "$SOURCE_TLS_KEY_FILE" ]] || { echo "Configured TLS certificate or key is not readable." >&2; exit 1; }
  sudo install -m 644 "$SOURCE_TLS_CERT_FILE" "$TLS_CERT_FILE"
  sudo install -m 600 "$SOURCE_TLS_KEY_FILE" "$TLS_KEY_FILE"
fi
sudo chmod 600 "$TLS_KEY_FILE"
sudo chmod 644 "$TLS_CERT_FILE"

# The UI keeps database credentials in its server-side session only.  The
# deployment environment contributes the Flask signing key, control DB, and
# non-secret OCI Streaming/Events scope. No alternate execution-service ID is required.
printf 'FLASK_SECRET_KEY=%s\nCONTROL_DATABASE=%s\nSESSION_COOKIE_SECURE=1\nOCI_EVENT_RULE_MANAGEMENT_ENABLED=%s\nOCI_STREAMING_MANAGEMENT_ENABLED=%s\nOCI_CONTAINER_ORCHESTRATION_ENABLED=%s\nOCI_EVENT_RULE_PREFIX=%s\nOCI_COMPARTMENT_ID=%s\nOCI_REGION=%s\nOCI_REGION_KEY=%s\nOCI_OBJECT_STORAGE_NAMESPACE=%s\nOCI_REGISTRY_REPOSITORY_ID=%s\nOCI_REGISTRY_REPOSITORY=%s\nOBJECT_STORAGE_BUCKET_NAME=%s\nVAULT_ID=%s\nVAULT_KEY_ID=%s\nSUBNET_ID=%s\nCONTAINER_AVAILABILITY_DOMAIN=%s\nPROCESSOR_SHAPE=%s\nPROCESSOR_OCPUS=%s\nPROCESSOR_MEMORY_GBS=%s\nPROCESSOR_IMAGE_URL=%s\nPROCESSOR_CONTAINER_NAME_PREFIX=%s\nWRITER_WORKERS=%s\nDB_SECRET_OCID=%s\nDB_HOST=%s\nDB_PORT=%s\nDB_USER=%s\nDB_NAME=%s\nSTREAM_DATA_DB_NAME=%s\nUI_IMAGE_NAME=%s\nUI_IMAGE_TAG=%s\nRELEASE_VERSION=%s\nGIT_SHA=%s\nSOURCE_BRANCH=%s\nBUILD_UTC=%s\nCONFIG_SCHEMA_VERSION=%s\n' \
  "$FLASK_SECRET_KEY" "$CONTROL_DATABASE" "$OCI_EVENT_RULE_MANAGEMENT_ENABLED" "$OCI_STREAMING_MANAGEMENT_ENABLED" "$OCI_CONTAINER_ORCHESTRATION_ENABLED" "$OCI_EVENT_RULE_PREFIX" "${COMPARTMENT_ID:-}" "${REGION:-}" "${REGION_KEY:-}" "$OBJECT_STORAGE_NAMESPACE" "$OCI_REGISTRY_REPOSITORY_ID" "$OCI_REGISTRY_REPOSITORY" "${OBJECT_STORAGE_BUCKET_NAME:-}" "${VAULT_ID:-}" "${VAULT_KEY_ID:-}" "${SUBNET_ID:-}" "${CONTAINER_AVAILABILITY_DOMAIN:-}" "${PROCESSOR_SHAPE:-}" "${PROCESSOR_OCPUS:-}" "${PROCESSOR_MEMORY_GBS:-}" "${PROCESSOR_IMAGE_URL:-}" "${PROCESSOR_CONTAINER_NAME_PREFIX:-object-storage-stream-processor}" "${WRITER_WORKERS:-4}" "${DB_SECRET_OCID:-}" "${DB_HOST:-}" "${DB_PORT:-3306}" "${DB_USER:-}" "${DB_NAME:-}" "${STREAM_DATA_DB_NAME:-}" "$UI_REGISTRY_IMAGE_NAME" "$UI_REGISTRY_IMAGE_TAG" "$RELEASE_VERSION" "$GIT_SHA" "$SOURCE_BRANCH" "$BUILD_UTC" "${CONFIG_SCHEMA_VERSION:-2}" > "$RUNTIME_ENV"
chmod 600 "$RUNTIME_ENV"
printf 'STAGING_DATABASE=%s\n' "$STAGING_DATABASE" >> "$RUNTIME_ENV"

# A system service uses the system Podman store/runtime rather than a user's
# login-session runtime, so it remains available after reboot and logout.
mkdir -p "$HOME/.config/containers"
printf '{\n  "credHelpers": {\n    "%s.ocir.io": "ocir"\n  }\n}\n' "$REGION_KEY" > "$REGISTRY_AUTH_FILE"
chmod 700 "$HOME/.config/containers"
chmod 600 "$REGISTRY_AUTH_FILE"
EXISTING_UI_IMAGE=$(
  oci --auth instance_principal --region "$REGION" artifacts container image list \
    --compartment-id "$COMPARTMENT_ID" --all --output json |
    jq -r --arg repository "$OCI_REGISTRY_REPOSITORY" --arg version "$UI_REGISTRY_IMAGE_TAG" \
      '.data.items[] |
       select(."repository-name" == $repository and .version == $version and ."lifecycle-state" != "DELETED") |
       .id' |
    head -1
)
if [[ -n "$EXISTING_UI_IMAGE" ]]; then
  sudo env "PATH=$PATH" podman pull --authfile "$REGISTRY_AUTH_FILE" "$UI_IMAGE"
else
  sudo podman build --tag "$UI_IMAGE" --file "$ROOT_DIR/ui/Dockerfile" \
    --build-arg "RELEASE_VERSION=$RELEASE_VERSION" --build-arg "GIT_SHA=$GIT_SHA" \
    --build-arg "SOURCE_BRANCH=$SOURCE_BRANCH" --build-arg "BUILD_UTC=$BUILD_UTC" \
    --build-arg "UI_IMAGE_NAME=$UI_REGISTRY_IMAGE_NAME" --build-arg "UI_IMAGE_TAG=$UI_REGISTRY_IMAGE_TAG" \
    --build-arg "CONFIG_SCHEMA_VERSION=${CONFIG_SCHEMA_VERSION:-2}" "$ROOT_DIR/ui"
  sudo env "PATH=$PATH" podman push --authfile "$REGISTRY_AUTH_FILE" "$UI_IMAGE"
fi

SERVICE_FILE="/etc/systemd/system/${UI_SERVICE_NAME}.service"
NGINX_FILE="/etc/nginx/conf.d/${UI_SERVICE_NAME}.conf"
sudo tee "$SERVICE_FILE" >/dev/null <<EOF
[Unit]
Description=Object Storage HeatWave Flask UI
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$ROOT_DIR/ui
ExecStartPre=-/usr/bin/podman rm -f $UI_CONTAINER_NAME
ExecStart=/usr/bin/podman run --rm --name $UI_CONTAINER_NAME --network host --env-file $RUNTIME_ENV -v $INSTANCE_DIR:/app/instance:Z,U $UI_IMAGE gunicorn --bind 127.0.0.1:$UI_BIND_PORT --workers $UI_WORKERS --threads $UI_THREADS --timeout 120 myapp.app:create_app()
ExecStop=/usr/bin/podman stop --ignore --time 10 $UI_CONTAINER_NAME
Restart=always
RestartSec=5
PrivateTmp=true

[Install]
WantedBy=multi-user.target
EOF

sudo tee "$NGINX_FILE" >/dev/null <<EOF
server {
    listen 443 ssl;
    server_name $UI_SERVER_NAME;
    ssl_certificate $TLS_CERT_FILE;
    ssl_certificate_key $TLS_KEY_FILE;
    ssl_protocols TLSv1.2 TLSv1.3;
    client_max_body_size 30m;

    location / {
        proxy_pass http://127.0.0.1:$UI_BIND_PORT;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto https;
        proxy_http_version 1.1;
    }
}
EOF

sudo nginx -t
if command -v getenforce >/dev/null && [[ "$(getenforce)" == "Enforcing" ]]; then
  sudo setsebool -P httpd_can_network_connect 1
fi
sudo systemctl daemon-reload
sudo systemctl enable "$UI_SERVICE_NAME"
sudo systemctl restart "$UI_SERVICE_NAME"
sudo systemctl enable nginx
sudo systemctl restart nginx

if command -v firewall-cmd >/dev/null; then
  sudo systemctl enable --now firewalld || true
  zone=$(sudo firewall-cmd --get-active-zones 2>/dev/null | awk 'NR==1 {print $1}')
  zone=${zone:-$(sudo firewall-cmd --get-default-zone 2>/dev/null || echo public)}
  sudo firewall-cmd --zone="$zone" --permanent --add-service=https
  sudo firewall-cmd --reload
else
  echo "firewall-cmd is unavailable; allow TCP/443 using the host firewall." >&2
fi

DEPLOYMENT_PYTHON="${DEPLOYMENT_PYTHON_BIN:-$ROOT_DIR/.venv-verification-py312/bin/python}"
if [[ ! -x "$DEPLOYMENT_PYTHON" ]] && command -v python3 >/dev/null && python3 -c 'import mysql.connector, oci' >/dev/null 2>&1; then
  DEPLOYMENT_PYTHON=$(command -v python3)
fi
if [[ -x "$DEPLOYMENT_PYTHON" ]]; then
  OCI_AUTH_MODE=instance_principal "$DEPLOYMENT_PYTHON" "$ROOT_DIR/deploy/record_deployment.py" \
    --component UI --deployment-name "$UI_SERVICE_NAME" \
    --release-version "$RELEASE_VERSION" --git-sha "$GIT_SHA" \
    --source-branch "$SOURCE_BRANCH" --build-utc "$BUILD_UTC" \
    --image-name "$UI_REGISTRY_IMAGE_NAME" --image-tag "$UI_REGISTRY_IMAGE_TAG" \
    --config-schema-version "${CONFIG_SCHEMA_VERSION:-2}" ||
    echo "WARNING: UI deployment succeeded but deployment history could not be recorded." >&2
else
  echo "WARNING: UI deployment succeeded but no deployment Python with MySQL Connector and OCI SDK is available to record history." >&2
fi

sudo systemctl --no-pager --full status "$UI_SERVICE_NAME"
echo "UI HTTPS deployment complete. Confirm the OCI NSG/security list allows inbound TCP/443."
