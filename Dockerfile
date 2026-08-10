FROM golang:1.25-alpine AS builder

WORKDIR /src

# Install CA certificates for secure module downloads and outbound TLS.
RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build a static Linux binary.
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/vximporter .

FROM alpine:3.22

WORKDIR /app

RUN apk add --no-cache bash ca-certificates

RUN addgroup -S app && adduser -S -G app app

COPY --from=builder /out/vximporter /usr/local/bin/vximporter
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
USER app
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
