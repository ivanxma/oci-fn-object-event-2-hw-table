#!/usr/bin/env bash
# Retry OL9 package installation when OS Management temporarily owns RPM state.

ol9_dnf_install() {
  local attempts=${DNF_INSTALL_ATTEMPTS:-6}
  local delay=${DNF_INSTALL_RETRY_SECONDS:-15}
  local attempt
  [[ "$attempts" =~ ^[1-9][0-9]*$ ]] || {
    echo "DNF_INSTALL_ATTEMPTS must be a positive integer." >&2
    return 2
  }
  [[ "$delay" =~ ^[1-9][0-9]*$ ]] || {
    echo "DNF_INSTALL_RETRY_SECONDS must be a positive integer." >&2
    return 2
  }
  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if sudo dnf install -y "$@"; then
      return 0
    fi
    if ((attempt == attempts)); then
      echo "OL9 package installation failed after $attempts attempts." >&2
      return 1
    fi
    echo "WARN: OL9 package manager was busy; retrying in ${delay}s ($attempt/$attempts)." >&2
    sleep "$delay"
  done
}
