#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
  echo "usage: $0 <env-file> [vtapi-path]" >&2
  exit 64
fi

ENV_FILE=$1
VTAPI_BIN=${2:-/usr/local/bin/vtapi}

if [[ ! -f "$ENV_FILE" ]]; then
  echo "env file not found: $ENV_FILE" >&2
  exit 66
fi

if [[ ! -x "$VTAPI_BIN" ]]; then
  echo "vtapi binary not executable: $VTAPI_BIN" >&2
  exit 66
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

: "${VT_ROLE:?VT_ROLE is required}"
: "${VT_BIND_ADDR:?VT_BIND_ADDR is required}"

if [[ "$VT_ROLE" == "storage" ]]; then
  : "${VT_STORAGE_PATH:?VT_STORAGE_PATH is required for storage}"
fi

exec "$VTAPI_BIN"
