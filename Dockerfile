FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/sluice ./cmd/sluice && \
    CGO_ENABLED=0 go build -o /out/sluice-cli ./cmd/sluice-cli

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/sluice /sluice
COPY --from=builder /out/sluice-cli /sluice-cli
ENTRYPOINT ["/sluice"]
