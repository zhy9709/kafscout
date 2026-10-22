#!/usr/bin/env bash
set -euo pipefail
export TMPDIR="${TMPDIR:-/var/tmp}" GOPROXY=off GOSUMDB=off
mkdir -p "$TMPDIR"
cd "$(dirname "$0")/.."
artifacts=${1:-/var/tmp/codex/kafscout-bin}
mkdir -p "$artifacts"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -buildvcs=false -trimpath -ldflags='-s -w' -o "$artifacts/kafscout-linux-amd64" .
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -mod=vendor -buildvcs=false -trimpath -ldflags='-s -w' -o "$artifacts/kafscout-windows-amd64.exe" .
cp README.md "$artifacts/README.md"
(cd "$artifacts" && sha256sum kafscout-linux-amd64 kafscout-windows-amd64.exe README.md > SHA256SUMS)
cat "$artifacts/SHA256SUMS"
