#!/bin/bash
# Manual ADBC cross-platform Docker test
# Usage: bash run_docker_test.sh [amd64|arm64|amd64,arm64]
# Requires: Docker, POSTGRES connection
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SLING_CLI_DIR="$(cd "$SCRIPT_DIR/../../../../.." && pwd)"
GIT_DIR="$(cd "$SLING_CLI_DIR/.." && pwd)"
CMD_DIR="$SLING_CLI_DIR/cmd/sling"

# Determine host architecture
HOST_ARCH=$(uname -m)
case "$HOST_ARCH" in
  x86_64)  HOST_ARCH="amd64" ;;
  aarch64|arm64) HOST_ARCH="arm64" ;;
esac

ENV_YAML="$HOME/.sling/env.yaml"

if [ -z "$POSTGRES" ]; then
  if [ -f "$ENV_YAML" ]; then
    POSTGRES=$(awk '/^  POSTGRES:/{found=1; next} found && /url:/{print $2; exit} found && /^  [^ ]/{exit}' "$ENV_YAML")
  fi
  if [ -z "$POSTGRES" ]; then
    echo "ERROR: POSTGRES env var not set and could not extract from $ENV_YAML"
    exit 1
  fi
  echo "Extracted POSTGRES url from $ENV_YAML"
  export POSTGRES
fi

if [ -z "$SLING_CLI_TOKEN" ]; then
  if [ -f "$ENV_YAML" ]; then
    SLING_CLI_TOKEN=$(awk '/SLING_CLI_TOKEN:/{print $2; exit}' "$ENV_YAML")
  fi
  if [ -n "$SLING_CLI_TOKEN" ]; then
    echo "Extracted SLING_CLI_TOKEN from $ENV_YAML"
    export SLING_CLI_TOKEN
  fi
fi

build_binary() {
  local arch=$1
  echo "=== Building sling for linux/$arch ==="
  docker run --rm --platform linux/$arch \
    -v "$GIT_DIR":/go/src/git \
    -v go_cache_docker:/go/cache \
    -v go_pkg_cache:/go/pkg \
    -w /go/src/git/sling-cli/cmd/sling \
    -e GOOS=linux -e GOARCH=$arch -e CGO_ENABLED=1 -e GOCACHE=/go/cache \
    golang:1.25-bookworm \
    sh -c 'go mod tidy && go build -o sling_linux_'"$arch"
}

build_image() {
  local arch=$1
  echo "=== Building Docker image for linux/$arch ==="
  cp "$CMD_DIR/sling_linux_$arch" "$SCRIPT_DIR/sling"
  docker build --platform linux/$arch -f "$SCRIPT_DIR/Dockerfile.$arch" -t sling-adbc-test-$arch "$SCRIPT_DIR"
  rm -f "$SCRIPT_DIR/sling"
}

run_test() {
  local arch=$1
  echo "=== Running ADBC test on linux/$arch ==="
  docker run --rm --platform linux/$arch \
    -v "$SCRIPT_DIR/p.40.adbc_duckdb.yaml":/tmp/p.40.adbc_duckdb.yaml:ro \
    -e POSTGRES="$POSTGRES" \
    -e SLING_CLI_TOKEN="$SLING_CLI_TOKEN" \
    -e DUCKDB_ADBC='{"type":"duckdb","instance":"/tmp/adbc_test.db","use_adbc":true}' \
    sling-adbc-test-$arch \
    run -d -p /tmp/p.40.adbc_duckdb.yaml
}

ARCHS="${1:-$HOST_ARCH}"

for arch in $(echo "$ARCHS" | tr ',' ' '); do
  build_binary "$arch"
  build_image "$arch"
  run_test "$arch"
  echo "=== linux/$arch: PASSED ==="
done

rm -f "$CMD_DIR/sling_linux_amd64" "$CMD_DIR/sling_linux_arm64"
echo "=== All tests passed ==="
