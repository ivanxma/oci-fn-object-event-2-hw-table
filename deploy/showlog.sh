#!/usr/bin/env bash
# Show OCI Function invocation logs from the preceding number of minutes.
# Usage: ./showlog.sh [minutes] [limit]
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/env.sh}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE. Copy env.sh.example to env.sh and set local values." >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

MINUTES="${1:-15}"
LIMIT="${2:-100}"
if ! [[ "$MINUTES" =~ ^[1-9][0-9]*$ ]]; then
  echo "Minutes must be a positive integer." >&2
  exit 2
fi
if ! [[ "$LIMIT" =~ ^[1-9][0-9]*$ ]]; then
  echo "Limit must be a positive integer." >&2
  exit 2
fi

: "${COMPARTMENT_ID:?Set COMPARTMENT_ID in env.sh}"
: "${APP_NAME:?Set APP_NAME in env.sh}"
: "${FUNCTION_NAME:?Set FUNCTION_NAME in env.sh}"
: "${FUNCTION_LOG_GROUP_ID:?Set FUNCTION_LOG_GROUP_ID in env.sh after enabling Function Invocation Logs}"
: "${FUNCTION_LOG_ID:?Set FUNCTION_LOG_ID in env.sh after enabling Function Invocation Logs}"
if ! [[ "$APP_NAME" =~ ^[A-Za-z0-9._-]+$ && "$FUNCTION_NAME" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "APP_NAME and FUNCTION_NAME may contain only letters, digits, dot, underscore, and hyphen." >&2
  exit 2
fi

OCI_BIN="${OCI_BIN:-oci}"
OCI_AUTH="${OCI_AUTH:-instance_principal}"
RAW_LOGS=$(mktemp)
trap 'rm -f "$RAW_LOGS"' EXIT

# OCI Logging Search expects RFC 3339 timestamps. OCI Compute uses GNU date.
START_TIME=$(date -u -d "${MINUTES} minutes ago" +'%Y-%m-%dT%H:%M:%SZ')
END_TIME=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

# Query the single invocation log. This avoids a broad compartment search, which
# can otherwise include Audit logs and require unrelated Audit permissions.
SEARCH_QUERY="search \"${COMPARTMENT_ID}/${FUNCTION_LOG_GROUP_ID}/${FUNCTION_LOG_ID}\" | source='${APP_NAME}' and subject='${FUNCTION_NAME}' | sort by datetime desc"

echo "Showing up to $LIMIT invocation log entries from $START_TIME through $END_TIME (UTC)." >&2
"$OCI_BIN" --auth "$OCI_AUTH" logging-search search-logs \
  --search-query "$SEARCH_QUERY" \
  --time-start "$START_TIME" \
  --time-end "$END_TIME" \
  --limit "$LIMIT" \
  --output json > "$RAW_LOGS"

# OCI writes one default lifecycle record when an invocation starts and another
# when it finishes. Group them by opcRequestId so one invocation is one row.
TABLE=$(jq -r '
  def clean: tostring | gsub("[\\t\\r\\n]+"; " ");
  def first_or_empty: if length == 0 then "" else .[0] end;
  [ .data.results[]?.data.logContent
    | {
        request_id: (.data.opcRequestId // .data.requestId // .id),
        time: (.time // ""),
        message: (.data.message // "")
      }
  ]
  | sort_by(.request_id, .time)
  | group_by(.request_id)
  | map(
      . as $entries
      | {
          request_id: $entries[0].request_id,
          started: ([$entries[] | select(.message == "Received function invocation request") | .time] | first_or_empty),
          completed: ([$entries[] | select(.message | startswith("Served function invocation request")) | .time] | first_or_empty),
          received_message: ([$entries[] | select(.message == "Received function invocation request") | .message] | first_or_empty),
          completion_message: ([$entries[] | select(.message | startswith("Served function invocation request")) | .message] | first_or_empty),
          function_output: ([$entries[] | select(.message != "Received function invocation request" and (.message | startswith("Served function invocation request") | not)) | .message] | join(" | "))
        }
    )
  | sort_by(.started) | reverse
  | (["START (UTC)", "END (UTC)", "REQUEST ID", "RECEIVED MESSAGE", "COMPLETION MESSAGE", "FUNCTION OUTPUT"] | @tsv),
    (.[] | [.started, .completed, .request_id, .received_message, .completion_message, .function_output] | map(clean) | @tsv)
' "$RAW_LOGS")

if command -v column >/dev/null 2>&1; then
  printf '%s\n' "$TABLE" | column -t -s $'\t'
else
  printf '%s\n' "$TABLE"
fi
