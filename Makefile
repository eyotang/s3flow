BIN_DIR ?= bin
LDFLAGS ?= -w -s

all: test build

test:
	go test ./...

tidy:
	go mod tidy -v

lint:
	golangci-lint run ./...

build: s3get s3push

s3get s3push:
	mkdir -p $(BIN_DIR)
	go build -ldflags="$(LDFLAGS)" -trimpath -o $(BIN_DIR)/$@ ./cmd/$@

clean:
	rm -rf bin dist

.PHONY: all test tidy lint build s3get s3push clean
