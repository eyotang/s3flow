BIN_DIR ?= bin
LDFLAGS ?= -w -s
COMMANDS := s3flow s3get s3push s3unzip s3rg s3tree s3sync s3cp s3mv s3presign

all: test build

test:
	go test ./...

tidy:
	go mod tidy -v

lint:
	golangci-lint run ./...

build: $(COMMANDS)

release: test
	npm test
	bash scripts/release.sh "$(VERSION)"

$(COMMANDS):
	mkdir -p $(BIN_DIR)
	go build -ldflags="$(LDFLAGS)" -trimpath -o $(BIN_DIR)/$@ ./cmd/$@

clean:
	rm -rf bin dist

.PHONY: all test tidy lint build release $(COMMANDS) clean
