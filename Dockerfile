# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM golang:1.22-alpine AS builder

WORKDIR /app

COPY go.mod ./
COPY *.go ./

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o sse-proxy .

# ---------------------------------------------------------------------------
# Runtime stage – minimal image with no shell
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /app/sse-proxy /sse-proxy

EXPOSE 8080

ENTRYPOINT ["/sse-proxy"]
