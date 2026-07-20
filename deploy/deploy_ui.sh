#!/usr/bin/env bash
# Deploy the Flask UI as a systemd-managed Podman container behind HTTPS.
# The container is intentionally bound only to localhost; nginx owns port 443.
set -euo pipefail
umask 077

export PATH="$HOME/.fn/bin:$HOME/bin:$HOME/.local/bin:$PATH"

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
[[ -r "$ENV_FILE" ]] || { echo "Copy deploy/env.sh.example to deploy/env.sh and set deployment values." >&2; exit 1; }
chmod 600 "$ENV_FILE"
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

for command in oci podman sudo systemctl curl; do
  command -v "$command" >/dev/null || { echo "Missing $command." >&2; exit 1; }
done
[[ -n "${FLASK_SECRET_KEY:-}" ]] || { echo "FLASK_SECRET_KEY must be set in $ENV_FILE." >&2; exit 1; }

UI_SERVICE_NAME="${UI_SERVICE_NAME:-object-storage-heatwave-ui}"
UI_CONTAINER_NAME="${UI_CONTAINER_NAME:-$UI_SERVICE_NAME}"
UI_BIND_PORT="${UI_BIND_PORT:-8080}"
UI_SERVER_NAME="${UI_SERVER_NAME:-_}"
CONTROL_DATABASE="${CONTROL_DATABASE:-${DB_NAME:-fndb}}"
OCI_FUNCTION_CONFIGURATION_ENABLED="${OCI_FUNCTION_CONFIGURATION_ENABLED:-${OCI_TIMEOUT_DEPLOY_ENABLED:-true}}"
OCI_EVENT_RULE_MANAGEMENT_ENABLED="${OCI_EVENT_RULE_MANAGEMENT_ENABLED:-true}"
OCI_EVENT_RULE_PREFIX="${OCI_EVENT_RULE_PREFIX:-${FUNCTION_NAME:-object-storage-heatwave}}"
RUNTIME_ENV="$ROOT_DIR/ui/.ui-runtime.env"
INSTANCE_DIR="$ROOT_DIR/ui/instance"
SOURCE_TLS_CERT_FILE="${TLS_CERT_FILE:-}"
SOURCE_TLS_KEY_FILE="${TLS_KEY_FILE:-}"
DEPLOY_TLS_DIR="/etc/$UI_SERVICE_NAME/tls"
TLS_CERT_FILE="$DEPLOY_TLS_DIR/tls.crt"
TLS_KEY_FILE="$DEPLOY_TLS_DIR/tls.key"
CURRENT_USER=$(id -un)
CURRENT_GROUP=$(id -gn)

if [[ -n "$SOURCE_TLS_CERT_FILE" && -z "$SOURCE_TLS_KEY_FILE" ]] \
   || [[ -z "$SOURCE_TLS_CERT_FILE" && -n "$SOURCE_TLS_KEY_FILE" ]]; then
  echo "TLS_CERT_FILE and TLS_KEY_FILE must either both be set or both be empty." >&2
  exit 1
fi

OCI_FUNCTION_ID=""
if [[ "$OCI_FUNCTION_CONFIGURATION_ENABLED" == "true" || "$OCI_EVENT_RULE_MANAGEMENT_ENABLED" == "true" ]]; then
  for value in COMPARTMENT_ID APP_NAME FUNCTION_NAME REGION; do
    [[ -n "${!value:-}" ]] || { echo "$value is required when OCI Function or Events management is enabled." >&2; exit 1; }
  done
  OCI=(oci --auth instance_principal)
  APP_ID=$("${OCI[@]}" fn application list --compartment-id "$COMPARTMENT_ID" --all \
    --query "data[?\"display-name\"=='$APP_NAME'].id | [0]" --raw-output)
  [[ -n "$APP_ID" && "$APP_ID" != null ]] || { echo "Function application $APP_NAME was not found." >&2; exit 1; }
  OCI_FUNCTION_ID=$("${OCI[@]}" fn function list --application-id "$APP_ID" --all \
    --query "data[?\"display-name\"=='$FUNCTION_NAME'].id | [0]" --raw-output)
  [[ -n "$OCI_FUNCTION_ID" && "$OCI_FUNCTION_ID" != null ]] || { echo "Function $FUNCTION_NAME was not found in $APP_NAME." >&2; exit 1; }
fi

case "$UI_BIND_PORT" in
  ''|*[!0-9]*) echo "UI_BIND_PORT must be numeric." >&2; exit 1 ;;
esac
(( UI_BIND_PORT >= 1024 && UI_BIND_PORT <= 65535 )) || { echo "UI_BIND_PORT must be from 1024 to 65535." >&2; exit 1; }

# Keep this fallback so deploy_ui.sh remains self-diagnosing when an operator
# skipped bootstrap.sh. Install the complete UI host set together; checking only
# nginx/openssl previously missed SELinux and firewall helpers.
if ! command -v nginx >/dev/null 2>&1 || ! command -v openssl >/dev/null 2>&1 \
   || ! command -v setsebool >/dev/null 2>&1 || ! command -v firewall-cmd >/dev/null 2>&1; then
  command -v dnf >/dev/null 2>&1 || { echo "Missing UI host packages and dnf is unavailable; run deploy/bootstrap.sh on Oracle Linux." >&2; exit 1; }
  sudo dnf install -y nginx openssl policycoreutils-python-utils
  sudo dnf install -y firewalld
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
  if sudo test -r "$TLS_CERT_FILE" && sudo test -r "$TLS_KEY_FILE"; then
    echo "Reusing existing generated TLS certificate for $UI_SERVICE_NAME."
  else
    sudo openssl req -x509 -newkey rsa:4096 -sha256 -nodes -days 365 \
      -keyout "$TLS_KEY_FILE" -out "$TLS_CERT_FILE" -subj "/CN=${UI_SERVER_NAME}" >/dev/null 2>&1
    echo "Generated a self-signed TLS certificate for $UI_SERVICE_NAME."
  fi
else
  [[ -r "$SOURCE_TLS_CERT_FILE" && -r "$SOURCE_TLS_KEY_FILE" ]] || { echo "Configured TLS certificate or key is not readable." >&2; exit 1; }
  sudo install -m 644 "$SOURCE_TLS_CERT_FILE" "$TLS_CERT_FILE"
  sudo install -m 600 "$SOURCE_TLS_KEY_FILE" "$TLS_KEY_FILE"
fi
sudo chmod 600 "$TLS_KEY_FILE"
sudo chmod 644 "$TLS_CERT_FILE"

# The UI keeps database credentials in its server-side session only.  The
# deployment environment contributes the Flask signing key, control DB, and
# non-secret OCI Function identity used for timeout reconciliation.
printf 'FLASK_SECRET_KEY=%s\nCONTROL_DATABASE=%s\nSESSION_COOKIE_SECURE=1\nOCI_FUNCTION_CONFIGURATION_ENABLED=%s\nOCI_EVENT_RULE_MANAGEMENT_ENABLED=%s\nOCI_EVENT_RULE_PREFIX=%s\nOCI_FUNCTION_ID=%s\nOCI_COMPARTMENT_ID=%s\nOCI_REGION=%s\nOCI_OBJECT_STORAGE_NAMESPACE=%s\n' \
  "$FLASK_SECRET_KEY" "$CONTROL_DATABASE" "$OCI_FUNCTION_CONFIGURATION_ENABLED" "$OCI_EVENT_RULE_MANAGEMENT_ENABLED" "$OCI_EVENT_RULE_PREFIX" "$OCI_FUNCTION_ID" "${COMPARTMENT_ID:-}" "${REGION:-}" "${OBJECT_STORAGE_NAMESPACE:-}" > "$RUNTIME_ENV"
chmod 600 "$RUNTIME_ENV"

# A system service uses the system Podman store/runtime rather than a user's
# login-session runtime, so it remains available after reboot and logout.
sudo podman build --tag "$UI_SERVICE_NAME:latest" "$ROOT_DIR/ui"
sudo podman run --rm --entrypoint python "$UI_SERVICE_NAME:latest" -c \
  'import sys, mysql.connector; assert sys.version_info >= (3,13); assert tuple(map(int,mysql.connector.__version__.split(".")[:2])) >= (9,7); print(f"UI runtime: Python {sys.version.split()[0]}, Connector/Python {mysql.connector.__version__}")'

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
ExecStart=/usr/bin/podman run --rm --name $UI_CONTAINER_NAME --network host --env-file $RUNTIME_ENV -v $INSTANCE_DIR:/app/instance:Z,U $UI_SERVICE_NAME:latest python -m flask --app myapp.app run --host 127.0.0.1 --port $UI_BIND_PORT
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
  if ! getsebool httpd_can_network_connect 2>/dev/null | grep -q -- '--> on'; then
    sudo setsebool -P httpd_can_network_connect 1
  fi
fi
sudo systemctl daemon-reload
sudo systemctl enable "$UI_SERVICE_NAME"
sudo systemctl restart "$UI_SERVICE_NAME"
sudo systemctl enable nginx
sudo systemctl restart nginx

if command -v firewall-cmd >/dev/null; then
  sudo timeout 30s systemctl enable --now firewalld || true
  zone=$(sudo firewall-cmd --get-active-zones 2>/dev/null | awk 'NR==1 {print $1}')
  zone=${zone:-$(sudo firewall-cmd --get-default-zone 2>/dev/null || echo public)}
  sudo timeout 20s firewall-cmd --zone="$zone" --permanent --add-service=https
  sudo timeout 20s firewall-cmd --reload
else
  echo "firewall-cmd is unavailable; allow TCP/443 using the host firewall." >&2
fi

UI_READY=false
for _attempt in $(seq 1 30); do
  if sudo systemctl is-active --quiet "$UI_SERVICE_NAME" && curl --fail --silent --show-error --insecure \
      "https://127.0.0.1/login" >/dev/null; then
    UI_READY=true
    break
  fi
  sleep 1
done
if [[ "$UI_READY" != true ]]; then
  echo "UI did not pass its HTTPS health check within 30 seconds." >&2
  sudo systemctl --no-pager --full status "$UI_SERVICE_NAME" >&2 || true
  sudo journalctl -u "$UI_SERVICE_NAME" -n 100 --no-pager >&2 || true
  exit 1
fi
sudo systemctl is-active --quiet nginx || { echo "nginx is not active after deployment." >&2; exit 1; }
echo "UI HTTPS deployment complete and healthy: https://${UI_SERVER_NAME}/ (local check returned 200)."
echo "Confirm the OCI NSG/security list allows inbound TCP/443."
