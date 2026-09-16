BINARY ?= bin/s3get
LDFLAGS ?= -w -s

all: clean test build

test:
	go test ./...

tidy:
	go mod tidy -v

lint:
	golangci-lint run ./...

build:
	mkdir -p $(dir $(BINARY))
	go build -ldflags="$(LDFLAGS)" -trimpath -o $(BINARY) ./cmd

clean:
	rm -rf bin dist

.PHONY: all test tidy lint build clean
