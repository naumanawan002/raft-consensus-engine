.PHONY: all build test run docker-up docker-down clean

BIN_DIR := ./bin

all: build test

build:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/raft-demo ./cmd/cluster-demo
	@echo "Build complete. Artifacts written to $(BIN_DIR)/"

test:
	go test -v -race ./pkg/...

run:
	go run ./cmd/cluster-demo -port :8095

docker-up:
	docker compose up --build

docker-down:
	docker compose down

clean:
	rm -rf $(BIN_DIR)
