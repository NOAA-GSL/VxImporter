GO ?= go
GO_SOURCES := vximporter.go vximporter_test.go tests/integration/integration_test.go
SHELL_SOURCES := entrypoint.sh import_archive.sh import_file.sh

.PHONY: build test test-integration fmt lint

build:
	$(GO) build -o vximporter .

test:
	$(GO) test ./...

test-integration:
	$(GO) test -tags integration ./...

fmt:
	gofmt -w $(GO_SOURCES)

lint:
	$(GO) vet ./...
	shellcheck -S warning $(SHELL_SOURCES)
