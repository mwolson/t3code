#!/usr/bin/env bash
set -euo pipefail
command -v docker >/dev/null
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
tree_dir=$(cd "$script_dir/../../.." && pwd)
container_id=$(docker run -d --rm -e POSTGRES_USER=t3relay -e POSTGRES_PASSWORD=test-only -e POSTGRES_DB=t3relay -p 127.0.0.1::5432 postgres:17-alpine -c max_connections=20)
cleanup() {
    docker stop "$container_id" >/dev/null
}
trap cleanup EXIT
ready=""
for _ in $(seq 1 30); do
    if docker exec "$container_id" pg_isready -h 127.0.0.1 -U t3relay >/dev/null 2>&1; then
        ready=yes
        break
    fi
    sleep 1
done
[[ -n "$ready" ]] || { echo "Test Postgres did not become ready" >&2; exit 1; }
port=$(docker port "$container_id" 5432/tcp | sed 's/.*://')
export T3CODE_PERSONAL_RELAY_TEST_DATABASE_URL="postgres://t3relay:test-only@127.0.0.1:$port/t3relay"
export PERSONAL_RELAY_TEST_PORT="$port"
cd "$tree_dir"
if [[ $# -gt 0 ]]; then
    "$@"
else
    ./node_modules/.bin/vp test run --config infra/personal-relay/vite.config.ts --fileParallelism false infra/personal-relay/test/pg infra/personal-relay/test/Foundation.pg.test.ts infra/personal-relay/src/enroll/EnvironmentEnrollment.pg.test.ts
fi
