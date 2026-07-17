#!/usr/bin/env bash
set -euo pipefail
REPORT=${1:-"$(dirname "$0")/function-report.html"}
COMPARTMENT_ID=${2:-unknown}; VCN_ID=${3:-unknown}; SUBNET_ID=${4:-unknown}; APP_ID=${5:-unknown}; FUNCTION_ID=${6:-unknown}
esc() { printf '%s' "$1" | sed 's/&/\&amp;/g; s/</\&lt;/g; s/>/\&gt;/g; s/"/\&quot;/g'; }
cat > "$REPORT" <<EOF
<!doctype html><html lang="en"><head><meta charset="utf-8"><title>OCI Function Deployment Report</title><style>body{font:16px system-ui;margin:3rem;max-width:960px;color:#202124}h1{color:#9d1622}table{border-collapse:collapse;width:100%}th,td{border:1px solid #ddd;padding:.7rem;text-align:left}th{background:#f7f7f7}code{word-break:break-all}</style></head><body><h1>Object Storage Event Function</h1><p>Generated $(date -u +'%Y-%m-%dT%H:%M:%SZ'). Database connection details are intentionally excluded.</p><table><tr><th>Item</th><th>Value</th></tr><tr><td>Compartment</td><td><code>$(esc "$COMPARTMENT_ID")</code></td></tr><tr><td>VCN</td><td><code>$(esc "$VCN_ID")</code></td></tr><tr><td>Private subnet</td><td><code>$(esc "$SUBNET_ID")</code></td></tr><tr><td>Function application</td><td><code>$(esc "$APP_ID")</code></td></tr><tr><td>Function</td><td><code>$(esc "$FUNCTION_ID")</code></td></tr><tr><td>Event types</td><td>Configured in env.sh</td></tr></table><h2>Validation</h2><p>The function creates and writes Object Storage events to the schema and table configured by <code>DB_NAME</code> and <code>DB_TABLE</code>. No database endpoint, username, or password is included in this report.</p></body></html>
EOF
echo "Wrote $REPORT"
