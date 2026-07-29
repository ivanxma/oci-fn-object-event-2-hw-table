#!/usr/bin/env bash
# Retired OCI Function deployment entry point.
#
# Object Storage events now target OCI Streaming and are processed by the
# long-running Container Instance consumer.  This file is deliberately a safe
# redirect: it performs no registry authentication, image build, OCI mutation,
# or Function deployment.
set -euo pipefail

cat >&2 <<'EOF'
The OCI Function deployment path is retired.

Use the Streaming deployment workflow instead:
  1. deploy/bootstrap_streaming.sh
  2. deploy/build_consumer_image.sh
  3. deploy/verify_streaming_deployment.sh
  4. deploy/deploy_ui.sh
  5. deploy/deploy_consumer.sh  # one explicit partition assignment at a time

Run the full verification gate in report/work-progress-streaming-migration.html
before creating a Container Instance.
EOF
exit 2
