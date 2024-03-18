
PROJECT_NAME := "godis-tiny"
BIN_DIR := $(GOPATH)/bin
BINARY_NAME := $(BIN_DIR)/$(PROJECT_NAME)

default: build

build: 
	@echo "  >  Building binary..."
	go build -o $(BINARY_NAME) -v

run: 
	@echo "  >  Running binary..."
	go run .

test: 
	@echo "  >  Testing..."
	go test -v ./...

clean:
	@echo "  >  Cleaning build cache"
	go clean
