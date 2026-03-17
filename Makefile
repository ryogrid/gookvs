.PHONY: test build vet proto

test:
	go test ./pkg/... ./internal/... -v -count=1

build:
	go build -o gookvs-server ./cmd/gookvs-server
	go build -o gookvs-ctl ./cmd/gookvs-ctl

vet:
	go vet ./...

proto:
	@echo "TODO: protoc generation for kvproto"
