#!/usr/bin/env bash
# Runner da demo para gravação (asciinema/agg). Sobe um Redis isolado,
# roda a demo e derruba o Redis. Ver scripts/record-demo.md.
set -e

REDIS_NAME="ss-demo-redis"
REDIS_PORT="6398"

cleanup() { docker rm -f "$REDIS_NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker rm -f "$REDIS_NAME" >/dev/null 2>&1 || true
docker run --rm -d --name "$REDIS_NAME" -p "${REDIS_PORT}:6379" redis:7-alpine >/dev/null
sleep 2

AUTH_ENABLED=false \
REDIS_URL="redis://localhost:${REDIS_PORT}/0" \
DEMO_PACE="${DEMO_PACE:-0.9}" \
  .venv/bin/python scripts/demo.py

sleep 2
