#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: package-linux-x86_64.sh [--output-dir <dir>] [--skip-build] [--disable-mimalloc]

Builds a Linux x86_64 release bundle containing:
- vtapi
- vtbench
- README and release docs
- ops/
- scripts/production/
- docs/runbooks/

Run this on a Linux x86_64 build machine from anywhere inside the repository.
EOF
}

OUTPUT_DIR=""
SKIP_BUILD=0
DISABLE_MIMALLOC=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-dir)
      OUTPUT_DIR=${2:?missing value for --output-dir}
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --disable-mimalloc)
      DISABLE_MIMALLOC=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 64
      ;;
  esac
done

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../.." && pwd)

if [[ $(uname -s) != "Linux" ]]; then
  echo "this script must run on Linux" >&2
  exit 69
fi

ARCH=$(uname -m)
if [[ "$ARCH" != "x86_64" && "$ARCH" != "amd64" ]]; then
  echo "this script is for Linux x86_64 build hosts; got $ARCH" >&2
  exit 69
fi

OUTPUT_DIR=${OUTPUT_DIR:-"$REPO_ROOT/dist"}
STAGE_DIR="$OUTPUT_DIR/linux-x86_64"
TARBALL="$OUTPUT_DIR/rust-victoria-trace-linux-x86_64.tar.gz"
CHECKSUM="$OUTPUT_DIR/rust-victoria-trace-linux-x86_64.tar.gz.sha256"

mkdir -p "$OUTPUT_DIR"
rm -rf "$STAGE_DIR"
mkdir -p "$STAGE_DIR"

cd "$REPO_ROOT"

if (( ! SKIP_BUILD )); then
  BUILD_ARGS=(cargo build --release -p vtapi -p vtbench)
  if (( DISABLE_MIMALLOC )); then
    BUILD_ARGS+=(--no-default-features)
  fi
  "${BUILD_ARGS[@]}"
fi

install -m 0755 target/release/vtapi "$STAGE_DIR/vtapi"
install -m 0755 target/release/vtbench "$STAGE_DIR/vtbench"
install -m 0644 README.md "$STAGE_DIR/README.md"
install -m 0644 docs/2026-04-06-otlp-ingest-performance-report.md "$STAGE_DIR/2026-04-06-otlp-ingest-performance-report.md"
install -m 0644 docs/production-release-guide.md "$STAGE_DIR/production-release-guide.md"
cp -R ops "$STAGE_DIR/ops"
mkdir -p "$STAGE_DIR/scripts"
cp -R scripts/production "$STAGE_DIR/scripts/production"
mkdir -p "$STAGE_DIR/docs"
cp -R docs/runbooks "$STAGE_DIR/docs/runbooks"

tar -C "$OUTPUT_DIR" -czf "$TARBALL" linux-x86_64
sha256sum "$(basename "$TARBALL")" > "$CHECKSUM"

echo "bundle ready:"
echo "  tarball: $TARBALL"
echo "  sha256 : $CHECKSUM"
