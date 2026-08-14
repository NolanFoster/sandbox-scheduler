# Build in a full toolchain, ship almost nothing.
FROM golang:1.24 AS builder
WORKDIR /workspace

# Dependencies first, so a source-only change reuses the module cache layer.
COPY go.mod go.sum ./
RUN go mod download

COPY api/ api/
COPY cmd/ cmd/
COPY internal/ internal/
COPY pkg/ pkg/

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev

# CGO_ENABLED=0 for a static binary: distroless static has no libc to link
# against, and a dynamically linked binary fails at exec with an error that
# says nothing useful about the cause.
# -w -s drops DWARF and the symbol table, which is most of the binary size.
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-w -s -X main.version=${VERSION}" \
    -o sandbox-scheduler ./cmd/sandbox-scheduler

# Static distroless: no shell, no package manager, no libc. A scheduler holding
# credentials for every provider in a fleet is a bad place to have an
# interactive shell available if something else goes wrong.
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/sandbox-scheduler .

# 65532 is distroless' nonroot user. Set here as well as in the Pod spec so the
# image is safe to run somewhere that does not set a securityContext.
USER 65532:65532

ENTRYPOINT ["/sandbox-scheduler"]
