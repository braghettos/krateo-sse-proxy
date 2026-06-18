# krateo-sse-proxy

Stateful in-memory **Server-Sent-Events hub** for the Krateo portal notifications/events bell.
Each pod runs its own poller (querying ClickHouse) and an SSE hub. Deployed by the
`krateo-sse-proxy` chart in
[`krateo-clickstack-chart`](https://github.com/braghettos/krateo-clickstack-chart).

> Split out of the former multi-image `krateo-clickstack` code repo so each component is a
> single-image repo on the canonical Krateo CI (one multi-platform `release-tag.yaml`).

## Endpoints

| Endpoint | Kind | Query params |
| --- | --- | --- |
| `GET /events` | JSON snapshot — `SSEK8sEvent[]` of recent events | `composition_id` (UUID, server-side filter; absent ⇒ all activity), `limit` (1–200, default & cap 200) |
| `GET /notifications` (and `/notifications/`) | SSE stream (`text/event-stream`) | `composition_id` (subscribe to one composition's events; absent ⇒ global firehose, client filters by SSE event name) |
| `GET /health` | liveness/readiness probe (unauthenticated) | — |

Each event object now includes `compositionId` (the `krateo.io/composition-id`, empty for
cluster-wide events).

`composition_id` and `limit` are bound as ClickHouse query parameters (`{name:Type}`), never
string-concatenated into SQL; `composition_id` is additionally validated as a UUID.

## Authentication

`/events` and `/notifications` validate the user's krateo JWT **the same way snowplow does** —
stateless HMAC (`HS256`) verification against the shared `JWT_SIGN_KEY` secret via
`github.com/krateoplatformops/plumbing/jwtutil` (no JWKS, no call-out to `authn`). The portal
RESTAction forwards it as `Authorization: Bearer <jwt>`. Because the browser `EventSource` API
cannot set headers, `/notifications` also accepts the token via the `krateo-session` cookie or a
`?access_token=` / `?token=` query parameter.

Auth is **opt-in**: when `JWT_SIGN_KEY` is unset the endpoints stay open (previous behaviour);
set it to enforce validation. `/health` is always unauthenticated.

### Configuration (env)

| Var | Default | Purpose |
| --- | --- | --- |
| `CLICKHOUSE_URL` | in-cluster ClickHouse | events source |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `default` / empty | ClickHouse basic auth |
| `LISTEN_ADDR` | `:8080` | listen address |
| `JWT_SIGN_KEY` | empty (auth disabled) | shared HMAC secret; must match snowplow/authn |
| `REFRESH_SESSION_COOKIE` | `krateo-session` | cookie name the SSE path reads the token from |

## Build & release
Image: `ghcr.io/braghettos/krateo-sse-proxy`. Pushing a semver tag (`X.Y.Z`) builds and pushes
a multi-platform (`linux/amd64,linux/arm64`) image via the canonical `release-tag.yaml`.
