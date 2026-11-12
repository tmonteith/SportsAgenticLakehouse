# syntax=docker/dockerfile:1
# Multi-arch image for the lakehouse extractor (linux/amd64 and linux/arm64).
# Build from the ingest/ directory (the Go module root):
#   docker buildx build --platform linux/amd64,linux/arm64 -f cmd/extractor/Dockerfile .

# The Go toolchain runs on the build host and cross-compiles, so no emulation is needed.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY internal/ internal/
COPY cmd/extractor/ cmd/extractor/
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/extractor ./cmd/extractor

# Static, shell-less runtime with CA certificates; runs as uid/gid 65532 (nonroot).
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/extractor /extractor
USER 65532:65532
ENTRYPOINT ["/extractor"]
