#!/usr/bin/env bash
set -euo pipefail

DRY_RUN=1
if [[ ${1:-} == "--execute" ]]; then
  DRY_RUN=0
fi

STORAGE_UNITS=${VT_CANARY_STORAGE_UNITS:-"rust-victoria-trace-storage@a rust-victoria-trace-storage@b"}
INSERT_UNIT=${VT_CANARY_INSERT_UNIT:-rust-victoria-trace-insert}
SELECT_UNIT=${VT_CANARY_SELECT_UNIT:-rust-victoria-trace-select}
SMOKE_SCRIPT=${VT_CANARY_SMOKE_SCRIPT:-/usr/local/bin/smoke-canary.sh}

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

echo "rollout mode: $([[ $DRY_RUN -eq 1 ]] && echo dry-run || echo execute)"
run sudo systemctl daemon-reload

for unit in $STORAGE_UNITS; do
  run sudo systemctl enable --now "$unit"
done
run sudo systemctl enable --now "$INSERT_UNIT"
run sudo systemctl enable --now "$SELECT_UNIT"

if (( DRY_RUN )); then
  echo "dry-run complete; rerun with --execute to start services"
  exit 0
fi

for unit in $STORAGE_UNITS "$INSERT_UNIT" "$SELECT_UNIT"; do
  sudo systemctl is-active --quiet "$unit"
done

"$SMOKE_SCRIPT"

cat <<'EOF'
next traffic step:
1. shift 1% write traffic to insert
2. keep reads on select only for canary probes
3. hold 10 minutes while watching metrics
4. if healthy, move to 5%, 20%, 50%, then 100%
EOF
