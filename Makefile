.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: lint
lint:
	golangci-lint run

.PHONY: test
test:
	go test -v ./...

.PHONY: test-race
test-race:
	go test -v -race ./...

.PHONY: update
update:
	go get -u ...
	go mod tidy
	go mod verify
