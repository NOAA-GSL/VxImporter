GO ?= go

.PHONY: build test test-integration

build:
	$(GO) build -o vximporter ./vximporter.go

test:
	$(GO) test ./...

test-integration:
	$(GO) test -tags integration ./...
