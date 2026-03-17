# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Copy dependency manifests first (layer cache)
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a fully static binary
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /indexer .

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM scratch

# Pull in CA certificates so the Go MongoDB driver can validate TLS if needed
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /indexer /indexer

# DATA_DIR  – mount point for the shared audio volume
# MONGO_URI – overridden by docker-compose / env
# MONGO_DB  – target database
ENV DATA_DIR=/data/devices \
    MONGO_URI=mongodb://mongo:27017 \
    MONGO_DB=boxuploader

ENTRYPOINT ["/indexer"]
