# krateo-sse-proxy

Stateful in-memory **Server-Sent-Events hub** for the Krateo portal notifications/events bell.
Each pod runs its own poller (querying ClickHouse) and an SSE hub, serving `/events` (SSE
stream) and `/notifications` (HTTP snapshot). Deployed by the `krateo-sse-proxy` chart in
[`krateo-clickstack-chart`](https://github.com/braghettos/krateo-clickstack-chart).

> Split out of the former multi-image `krateo-clickstack` code repo so each component is a
> single-image repo on the canonical Krateo CI (one multi-platform `release-tag.yaml`).

## Build & release
Image: `ghcr.io/braghettos/krateo-sse-proxy`. Pushing a semver tag (`X.Y.Z`) builds and pushes
a multi-platform (`linux/amd64,linux/arm64`) image via the canonical `release-tag.yaml`.
