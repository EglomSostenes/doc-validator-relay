# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

# git is needed for `go mod download` with some indirect deps
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /build

# Download deps first — this layer is cached as long as go.mod/go.sum don't change
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build a fully static binary (no libc dependency in the final image)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-w -s" -o relay ./cmd/relay

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM scratch

# Pull in CA certs and timezone data from the builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

COPY --from=builder /build/relay /relay

ENTRYPOINT ["/relay"]
