GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)

.PHONY: all
all: help

.PHONY: lint
lint:
	golangci-lint run

.PHONY: tidy
tidy:
	go mod tidy -v

.PHONY: prepare
prepare: tidy lint

.PHONY: build
build:
	go build -v -o jaeger-kusto

.PHONY: proto-gen
proto-gen:
	@echo "Generating V2 storage proto stubs..."
	protoc \
		--proto_path=proto \
		--go_out=proto-gen --go_opt=module=github.com/dodopizza/jaeger-kusto/proto-gen \
		--go-grpc_out=proto-gen --go-grpc_opt=module=github.com/dodopizza/jaeger-kusto/proto-gen \
		--go_opt=Mopentelemetry/proto/common/v1/common.proto=go.opentelemetry.io/proto/otlp/common/v1 \
		--go_opt=Mopentelemetry/proto/resource/v1/resource.proto=go.opentelemetry.io/proto/otlp/resource/v1 \
		--go_opt=Mopentelemetry/proto/trace/v1/trace.proto=go.opentelemetry.io/proto/otlp/trace/v1 \
		--go-grpc_opt=Mopentelemetry/proto/common/v1/common.proto=go.opentelemetry.io/proto/otlp/common/v1 \
		--go-grpc_opt=Mopentelemetry/proto/resource/v1/resource.proto=go.opentelemetry.io/proto/otlp/resource/v1 \
		--go-grpc_opt=Mopentelemetry/proto/trace/v1/trace.proto=go.opentelemetry.io/proto/otlp/trace/v1 \
		storage/v2/trace_storage.proto \
		storage/v2/dependency_storage.proto \
		opentelemetry/proto/collector/trace/v1/trace_service.proto

.PHONY: test
test:
	@echo "Running tests under test folder"
	@go test -v \
		--tags=integration \
		-timeout 300s ./test/... | \
	sed "/PASS/s//$(printf "\033[32mPASS\033[0m")/" | \
	sed "/FAIL/s//$(printf "\033[31mFAIL\033[0m")/"

.PHONY: help
help:
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@echo "  ${YELLOW}lint                   ${RESET} Run linters via golangci-lint"
	@echo "  ${YELLOW}tidy                   ${RESET} Run tidy for go module to remove unused dependencies"
	@echo "  ${YELLOW}prepare                ${RESET} Run all available checks"
	@echo "  ${YELLOW}build                  ${RESET} Build the jaeger-kusto binary"
	@echo "  ${YELLOW}proto-gen              ${RESET} Regenerate V2 storage protobuf stubs"
	@echo "  ${YELLOW}test                   ${RESET} Run integration tests"
