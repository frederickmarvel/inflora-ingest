BINARY := inflora-ingest
COMMAND := ./cmd/server

.PHONY: build test run lint tidy clean

build:
	mkdir -p bin
	go build -trimpath -o bin/$(BINARY) $(COMMAND)

test:
	go test -race ./...

run:
	set -a; . ./.env.example; set +a; go run $(COMMAND)

lint:
	golangci-lint run

tidy:
	go mod tidy

clean:
	rm -rf bin
