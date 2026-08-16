#!/usr/bin/env bash
# Run pgwire client-API integration tests against a real PostgreSQL server.
#
# Starts a PostgreSQL container with podman, waits for it to accept
# connections, runs `cargo test`, and tears the container down again.
#
# Usage:
#   ./run.sh                    # run all client API integration tests
#   ./run.sh --test cancel      # extra args are forwarded to `cargo test`
#   KEEP_CONTAINER=1 ./run.sh   # do not remove the container afterwards
#
# Environment variables:
#   PGWIRE_ITEST_IMAGE      container image  (default docker.io/library/postgres:18)
#   PGWIRE_ITEST_CONTAINER  container name   (default pgwire-client-api-itest)
#   PGWIRE_ITEST_PORT       host port        (default 54329)
#
# Notes:
# - PostgreSQL 18 is used because the protocol negotiation tests need a
#   server that speaks protocol 3.2. Older servers make those tests fail.
# - To run the tests against an already-running PostgreSQL (no podman), set
#   PGWIRE_ITEST_HOST/PORT and call `cargo test -p client-api-tests` directly.
set -euo pipefail

cd "$(dirname "$0")"

IMAGE=${PGWIRE_ITEST_IMAGE:-docker.io/library/postgres:18}
CONTAINER=${PGWIRE_ITEST_CONTAINER:-pgwire-client-api-itest}
PORT=${PGWIRE_ITEST_PORT:-54329}

# clean up a leftover container from a previous run
podman rm -f "$CONTAINER" >/dev/null 2>&1 || true

echo ">>> starting $IMAGE as $CONTAINER (127.0.0.1:$PORT)"
podman run -d --name "$CONTAINER" \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_HOST_AUTH_METHOD=scram-sha-256 \
    -p "127.0.0.1:${PORT}:5432" \
    "$IMAGE" >/dev/null

if [ -z "${KEEP_CONTAINER:-}" ]; then
    cleanup() {
        podman rm -f "$CONTAINER" >/dev/null 2>&1 || true
    }
    trap cleanup EXIT
fi

echo ">>> waiting for PostgreSQL to accept connections"
for _ in $(seq 1 60); do
    if podman exec "$CONTAINER" pg_isready -q -U postgres >/dev/null 2>&1; then
        break
    fi
    sleep 1
done
# fail hard if it never became ready
podman exec "$CONTAINER" pg_isready -U postgres

echo ">>> running client API integration tests"
PGWIRE_ITEST_PORT="$PORT" cargo test -p client-api-tests "$@"
