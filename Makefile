TAGS = rabbitmq
TEST_ARGS = -failfast -tags $(TAGS)
RABBITMQ_CONTAINER = test-rabbitmq

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: lint
lint:
	golangci-lint run --build-tags $(TAGS)

.PHONY: test-start
test-start:
	# use a non-standard port so that it doesn't collide with the dev
	# environment
	docker run -d --name $(RABBITMQ_CONTAINER) \
		-p 45672:15672 \
		-p 35672:5672 \
		--rm \
		rabbitmq:3-management

.PHONY: test-stop
test-stop:
	docker stop $(RABBITMQ_CONTAINER)

.PHONY: test
test: fmt lint
	@if [ "`docker inspect -f '{{.State.Running}}' $(RABBITMQ_CONTAINER) 2>&1 `" != "true" ]; then \
		echo "================================================"; \
		echo "== Did you remember to run 'make test-start'? =="; \
		echo "================================================"; \
	fi
	go test $(TEST_ARGS) ./...

.PHONY: test-regen
test-regen:
	rm -rf testdata/output
	mkdir -p testdata/output
	go test -regen $(TEST_ARGS) ./...

.PHONY: test-cover
test-cover: fmt
	go test $(TEST_ARGS) -coverprofile=coverage.out ./...

	# show uncovered functions
	go tool cover -func=coverage.out |\
		grep -v 100.0% |\
		grep -v total: |\
		perl -nae 'printf("%7s %s %s\n", $$F[2], $$F[0], $$F[1])' | sort -nr

	go tool cover -html=coverage.out

.PHONY: push
push: test
	git push
	git push --tags

.PHONY: clean
clean:
	rm coverage.out

.PHONY: update
update:
	go get -u ...
	go mod tidy
	go mod verify
