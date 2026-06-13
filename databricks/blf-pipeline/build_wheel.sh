#!/usr/bin/env bash
# Build a manylinux aarch64 wheel for Databricks Serverless (Linux ARM64)
# using a local Docker image derived from ghcr.io/pyo3/maturin.
# Output goes to ./dist/ so Databricks Asset Bundles can locate the wheel.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Docker on Windows needs a Windows-style path (C:/...) for volume mounts.
# pwd -W gives that format in Git Bash; fall back to the Unix path on Linux.
if command -v cygpath &>/dev/null; then
  REPO_ROOT_DOCKER="$(cygpath -w "${REPO_ROOT}")"
elif [[ "${REPO_ROOT}" =~ ^/[a-zA-Z]/ ]]; then
  # Git Bash path like /c/Users/... → C:/Users/...
  DRIVE="${REPO_ROOT:1:1}"
  REPO_ROOT_DOCKER="${DRIVE^^}:${REPO_ROOT:2}"
else
  REPO_ROOT_DOCKER="${REPO_ROOT}"
fi

mkdir -p "${REPO_ROOT}/dist"

# Build a local image with zig + aarch64 target pre-installed (cached after first run).
docker build -q -t vector-blf-maturin "${SCRIPT_DIR}"

docker run --rm -v "${REPO_ROOT_DOCKER}:/io" vector-blf-maturin \
    build --release --features python \
    --target aarch64-unknown-linux-gnu \
    --zig \
    -i python3.12 \
    --out dist
