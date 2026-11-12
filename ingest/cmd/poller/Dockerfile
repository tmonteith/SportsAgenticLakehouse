# syntax=docker/dockerfile:1
# Multi-arch image for the lakehouse poller (linux/amd64 and linux/arm64).
# Build from the ingest/ directory (the Go module root):
#   docker buildx build --platform linux/amd64,linux/arm64 -f cmd/poller/Dockerfile .

# The Go toolchain runs on the build host and cross-compiles, so no emulation is needed.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY internal/ internal/
COPY cmd/poller/ cmd/poller/
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/poller ./cmd/poller

# Static, shell-less runtime with CA certificates; runs as uid/gid 65532 (nonroot).
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/poller /poller
USER 65532:65532
ENTRYPOINT ["/poller"]
