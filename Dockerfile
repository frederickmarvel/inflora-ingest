# syntax=docker/dockerfile:1.6
FROM golang:1.25-alpine AS builder
WORKDIR /src/inflora-ingest
COPY inflora-shared /src/inflora-shared
COPY inflora-ingest/go.mod inflora-ingest/go.sum ./
RUN go mod download
COPY inflora-ingest .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/inflora-ingest ./cmd/server

FROM alpine:3.21
RUN apk add --no-cache ca-certificates wget && addgroup -S inflora && adduser -S -G inflora inflora
COPY --from=builder /out/inflora-ingest /usr/local/bin/inflora-ingest
USER inflora:inflora
ENTRYPOINT ["/usr/local/bin/inflora-ingest"]
EXPOSE 8081
