#!/usr/bin/env bash
set -euo pipefail

DRY_RUN=1
if [[ ${1:-} == "--execute" ]]; then
  DRY_RUN=0
fi

STORAGE_UNITS=${VT_CANARY_STORAGE_UNITS:-"rust-victoria-trace-storage@a rust-victoria-trace-storage@b"}
INSERT_UNIT=${VT_CANARY_INSERT_UNIT:-rust-victoria-trace-insert}
SELECT_UNIT=${VT_CANARY_SELECT_UNIT:-rust-victoria-trace-select}

run() {
  if (( DRY_RUN )); then
    printf '+'
    for arg in "$@"; do
      printf ' %q' "$arg"
    done
    printf '\n'
  else
    "$@"
  fi
}

cat <<'EOF'
rollback order:
1. switch external write and read traffic back to VictoriaTraces
2. stop select
3. stop insert
4. stop storage after traffic is drained
EOF

run sudo systemctl stop "$SELECT_UNIT"
run sudo systemctl stop "$INSERT_UNIT"
for unit in $STORAGE_UNITS; do
  run sudo systemctl stop "$unit"
done

if (( DRY_RUN )); then
  echo "dry-run complete; rerun with --execute to stop services"
fi
