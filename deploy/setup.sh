#!/usr/bin/env bash
# One-command, rerunnable Oracle Linux deployment orchestration.
set -euo pipefail
umask 077

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
ENV_FILE="${ENV_FILE:-$ROOT_DIR/deploy/env.sh}"
SKIP_BOOTSTRAP=false
SMOKE_TEST=false
RESET_PERFORMANCE=false

while (($#)); do
  case "$1" in
    --skip-bootstrap) SKIP_BOOTSTRAP=true ;;
    --smoke-test) SMOKE_TEST=true ;;
    --reset-performance) RESET_PERFORMANCE=true ;;
    -h|--help)
      echo "Usage: $0 [--skip-bootstrap] [--smoke-test] [--reset-performance]"
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
  shift
done

if [[ "$SKIP_BOOTSTRAP" != true ]]; then
  "$ROOT_DIR/deploy/bootstrap.sh"
fi

[[ -r "$ENV_FILE" ]] || {
  echo "Bootstrap is complete, but $ENV_FILE is missing." >&2
  echo "Copy deploy/env.sh.example to deploy/env.sh, fill the protected values, chmod 600 it, and rerun." >&2
  exit 1
}
chmod 600 "$ENV_FILE"
export ENV_FILE

"$ROOT_DIR/deploy/deploy.sh"
"$ROOT_DIR/deploy/deploy_ui.sh"

if [[ "$SMOKE_TEST" == true || "$RESET_PERFORMANCE" == true ]]; then
  PERFORMANCE_ARGS=()
  [[ "$RESET_PERFORMANCE" == true ]] && PERFORMANCE_ARGS+=(--reset)
  [[ "$SMOKE_TEST" == true ]] && PERFORMANCE_ARGS+=(--smoke-test)
  "$ROOT_DIR/performance_test/setup.sh" "${PERFORMANCE_ARGS[@]}"
fi

"$ROOT_DIR/deploy/validate.sh"
echo "Full deployment workflow completed successfully."
