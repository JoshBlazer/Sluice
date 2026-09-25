# Builds natively on the build machine and cross-compiles for the target
# platform, so multi-arch images don't need slow emulated Go builds.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS builder
ARG TARGETOS=linux
ARG TARGETARCH
ARG VERSION=dev
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -ldflags "-X main.version=${VERSION}" -o /out/sluice ./cmd/sluice && \
    go build -o /out/sluice-cli ./cmd/sluice-cli
# The migrate CLI ships in the image so deployments apply the schema that matches
# this exact build (the Helm chart runs it as a pre-install/pre-upgrade job).
# Built from a scratch module because `go install` can't cross-compile to a fixed path.
RUN mkdir /tmp/migrate && cd /tmp/migrate && go mod init build >/dev/null 2>&1 && \
    go get github.com/golang-migrate/migrate/v4/cmd/migrate@v4.18.1 && \
    go build -tags postgres -o /out/migrate github.com/golang-migrate/migrate/v4/cmd/migrate

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/sluice /sluice
COPY --from=builder /out/sluice-cli /sluice-cli
COPY --from=builder /out/migrate /migrate
COPY migrations /migrations
ENTRYPOINT ["/sluice"]
