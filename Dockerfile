# Build stage.
#
# Pinned to the same Go minor the module requires, so a container build and a local build
# cannot disagree about the toolchain.
FROM golang:1.27-alpine AS build
WORKDIR /src

# Dependencies are fetched before the source is copied, so that editing a .go file does not
# invalidate the module cache layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# One image, several entrypoints (ADR-007). Each binary is listed explicitly rather than
# globbed over ./cmd/..., so a new directory does not silently join the image without
# someone deciding it should.
#
# CGO is disabled so the result is a static binary that runs on a distroless-ish base; the
# -trimpath flag keeps local build paths out of the binary.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ingest ./cmd/ingest \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/consumer ./cmd/consumer

# Runtime stage.
FROM alpine:3.20 AS runtime

# ca-certificates for outbound TLS; tzdata so timestamps logged by a container match the
# ones logged on a host. wget ships with busybox here and backs the healthcheck.
RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -u 10001 app

USER app
WORKDIR /home/app

COPY --from=build /out/ingest   /bin/ingest
COPY --from=build /out/migrate  /bin/migrate
COPY --from=build /out/consumer /bin/consumer

# The ingest HTTP port. The consumer and gateway listeners are published per-service in
# compose rather than baked in here.
EXPOSE 8081

# The default entrypoint is the long-running service; the migrate container overrides it.
# A default that exited immediately would make `docker run <image>` look like a crash.
ENTRYPOINT ["/bin/ingest"]
