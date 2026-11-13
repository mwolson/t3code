# Personal background relay

A single-owner Node 26.12.1 relay for iOS notifications and Live Activities. T3 Connect
continues to use its primary relay. This service exposes only the background API,
`/livez` and `/health`; administration runs through a separate process.

Build from the workspace root:

```bash
./node_modules/.bin/vp run --filter t3code-personal-relay build
```

The bundle contains `dist/main.mjs`, `dist/admin.mjs` and shared chunks. The runtime
graph check rejects deployment-only dependencies. The Dockerfile consumes the
bundle plus `relay-migrations/`, staged from `infra/relay/migrations/postgres`.
It installs no dependencies. Its entrypoint is `node /app/main.mjs`.

Run `main.mjs migrate` with database configuration before `main.mjs serve`.
Migrations read `PERSONAL_RELAY_MIGRATIONS_DIR` (default: `dist/migrations`), with
`relay/<folder>/migration.sql` and `personal/<file>.sql`. Each migration and its
checksum commit atomically. Serving acquires dedicated worker leadership first,
checks every migration checksum, prunes retained data, then starts the worker and
HTTP listener. A second worker exits with status 3. Lost leadership terminates the
process. `self-check` constructs the real serving graph and reused planner with
throwaway configuration, without querying Postgres or sending requests.

The listener binds `0.0.0.0`. `/livez` has no database dependency; `/health` queries
Postgres. Request bodies are limited to 64 KiB. Protected HTTP operations have a five-second budget. The native sql-pg
pool bounds connection establishment separately; account transactions set
two-second lock and three-second statement timeouts. APNs network operations run
outside account transactions. Retries persist independently per device and channel.
Device registration backfills eligible alerts without resetting final deliveries.
Alerts without a usable token stay pending until a later registration or their
expiry. A rejected alerting Live Activity can fall back once to a notification.

## Configuration

| Variable                                                            | Value or default                                                         |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `PERSONAL_RELAY_PUBLIC_ORIGIN`                                      | Canonical HTTPS origin, default `https://t3-relay.mikemail.work`         |
| `PERSONAL_RELAY_PORT`                                               | `8080`                                                                   |
| `PERSONAL_RELAY_OWNER_SUBJECT`                                      | Required Clerk subject                                                   |
| `PERSONAL_RELAY_CLERK_ISSUER`                                       | `https://clerk.t3.codes`                                                 |
| `PERSONAL_RELAY_CLERK_AUDIENCE`                                     | `t3-code-relay`                                                          |
| `PERSONAL_RELAY_CLERK_JWKS_URL`                                     | `https://clerk.t3.codes/.well-known/jwks.json`                           |
| `PERSONAL_RELAY_CLERK_JWKS_FILE`                                    | Test-only public JWKS file, overrides remote fetching                    |
| `PERSONAL_RELAY_DATABASE_HOST`                                      | `127.0.0.1`                                                              |
| `PERSONAL_RELAY_DATABASE_PORT`                                      | `5432`                                                                   |
| `PERSONAL_RELAY_DATABASE_NAME`                                      | `t3relay`                                                                |
| `PERSONAL_RELAY_DATABASE_USER`                                      | `t3relay`                                                                |
| `PERSONAL_RELAY_DATABASE_PASSWORD`                                  | Required password                                                        |
| `PERSONAL_RELAY_DATABASE_POOL_MAX`                                  | Serving maximum, default `8`, allowed `1` to `8`                         |
| `PERSONAL_RELAY_TOKEN_SIGNING_PRIVATE_KEY`                          | Required Ed25519 PKCS8 PEM                                               |
| `PERSONAL_RELAY_ALLOWED_BUNDLE_IDS`                                 | Comma-separated, default `com.mwolson.t3code.v2.dev`                     |
| `PERSONAL_RELAY_ALLOWED_APS_ENVIRONMENTS`                           | Comma-separated `sandbox`/`production`, default `sandbox`                |
| `PERSONAL_RELAY_LOG_LEVEL`                                          | `info`, case-insensitive Effect level                                    |
| `APNS_ENABLED`                                                      | `false`; disabled deliveries become suppressed without a network attempt |
| `APNS_TEAM_ID`, `APNS_KEY_ID`, `APNS_PRIVATE_KEY`, `APNS_BUNDLE_ID` | Required with APNs enabled; PKCS8 P-256 PEM for the private key          |
| `APNS_ENVIRONMENT`                                                  | `sandbox`                                                                |
| `PERSONAL_RELAY_APNS_ORIGIN_OVERRIDE`                               | Test-only fake APNs origin                                               |
| `NODE_EXTRA_CA_CERTS`                                               | Test CA when using fake APNs                                             |

`migrate` needs only database configuration and optional migration path. The init
pool has one connection. Admin uses two pooled connections and no worker leadership.
Its configuration failures pass through the sanitized admin error boundary.

## Administration

Use the running server's existing T3 home for the proof command. It refuses a
missing environment identity or signing key without initializing another home.
Admin allows 60 seconds for proof input, followed by a five-second command budget.

Pipe the server command `t3 connect background-relay enrollment-proof --relay-url
<origin> [--label <name>]` into `node /app/admin.mjs enroll`. Enrollment stdout
contains only the credential and refuses terminal output. Pipe that credential
into `t3 connect background-relay set --url <origin> --credential-stdin` using
shell `pipefail`. Re-enrollment rotates the previous credential; a changed
environment key also removes old-key activity and alerts.

Other admin commands are `revoke-environment <id>`, `list-environments`,
`list-devices`, `config` and `status`. Diagnostics and JSON readouts go to stderr.
Device output includes token suffixes only. Status reports unavailable recent-event
counters explicitly; those events currently exist only in sanitized logs.
No admin endpoint is exposed over HTTP.

## Local verification

The TLS fake APNs server generates a fresh self-signed localhost certificate and
key in memory when its test module loads, using the test-only `selfsigned`
dependency. It includes DNS `localhost` and IP `127.0.0.1` subject alternative
names. No OpenSSL executable or committed key is needed. The process harness
writes only the generated public certificate to its scratch directory for
`NODE_EXTRA_CA_CERTS` and removes it on shutdown.

The ordinary package command needs no Postgres:

```bash
./node_modules/.bin/vp run --filter t3code-personal-relay test
```

Run the serial Postgres suite with one disposable container:

```bash
./node_modules/.bin/vp run --filter t3code-personal-relay test:pg
```

Run the combined personal tests with the same fixture:

```bash
infra/personal-relay/scripts/test-postgres.sh ./node_modules/.bin/vp test run --fileParallelism false infra/personal-relay
```

Repeat U2's scenarios through the real sender and TLS fake APNs:

```bash
T3CODE_PERSONAL_RELAY_REAL_APNS=1 infra/personal-relay/scripts/test-postgres.sh ./node_modules/.bin/vp test run --fileParallelism false infra/personal-relay/test/pg
```

Run the process harness after building. It uses packed relay/admin processes,
a real Postgres database, local TLS fake APNs, test JWKS, real DPoP mobile client,
and a test publisher with real signed claims. No Apple or Clerk request is made.
Its scratch path stays under the parent workspace's `tmp/relay/integration`.

```bash
infra/personal-relay/scripts/test-postgres.sh node infra/personal-relay/scripts/e2e-local.ts
```
