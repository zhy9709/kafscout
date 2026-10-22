#!/usr/bin/env bash
# No image pulls, no topic creation, no production, no offset commits.
set -euo pipefail
export TMPDIR=/var/tmp/codex
mkdir -p "$TMPDIR"
cd "$(dirname "$0")/.."
export DOCKER_HOST=unix:///var/run/docker.sock
unset DOCKER_CONTEXT DOCKER_TLS DOCKER_TLS_VERIFY DOCKER_CERT_PATH
if ! command -v docker >/dev/null || ! docker info >/dev/null 2>&1; then
  echo 'SKIP: local Docker daemon unavailable'
  exit 0
fi
if ! docker image inspect apache/kafka:3.9.1 >/dev/null 2>&1; then
  echo 'SKIP: apache/kafka:3.9.1 is not cached; image pulls are prohibited'
  exit 0
fi
docker compose -p kafscout-smoke -f scripts/compose.yaml up -d --pull never
trap 'docker compose -p kafscout-smoke -f scripts/compose.yaml down' EXIT
ready=0
for ((i=0;i<30;i++)); do
  if docker compose -p kafscout-smoke -f scripts/compose.yaml exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:9092 >/dev/null 2>&1; then ready=1; break; fi
  sleep 1
done
if [[ "$ready" != 1 ]]; then echo 'FAIL: Kafka did not become ready'; exit 1; fi
GOPROXY=off GOSUMDB=off CGO_ENABLED=0 go build -mod=vendor -o "$TMPDIR/kafscout-smoke" .
# By default verify read-only metadata for a nonexistent topic (error, no auto-create).
# Set KAFSCOUT_SMOKE_TOPIC to a pre-existing fixture topic to exercise range consumption.
topic=${KAFSCOUT_SMOKE_TOPIC:-kafscout-nonexistent}
set +e
"$TMPDIR/kafscout-smoke" --brokers 127.0.0.1:19092 --topic "$topic" --start '2026-10-22 00:00:00' --end '2026-10-22 01:00:00' --query smoke --quiet >"$TMPDIR/kafscout-compose.out" 2>"$TMPDIR/kafscout-compose.err"
status=$?
set -e
cat "$TMPDIR/kafscout-compose.out" "$TMPDIR/kafscout-compose.err"
if [[ -z "${KAFSCOUT_SMOKE_TOPIC:-}" ]]; then
  [[ "$status" = 1 ]]
  grep -q 'topic metadata:' "$TMPDIR/kafscout-compose.err"
else
  [[ "$status" = 0 ]]
fi
echo 'PASS: local Kafka smoke'
