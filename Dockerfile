FROM golang:1.25-alpine AS builder

WORKDIR /src

# Install CA certificates for secure module downloads and outbound TLS.
RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build a static Linux binary.
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/vximporter ./vximporter.go

FROM alpine:3.22

WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=builder /out/vximporter /usr/local/bin/vximporter

ENTRYPOINT ["/usr/local/bin/vximporter"]