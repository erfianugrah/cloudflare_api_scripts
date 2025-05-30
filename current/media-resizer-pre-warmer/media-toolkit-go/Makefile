# Makefile for media-toolkit-go

# Binary name
BINARY_NAME=media-toolkit
BINARY_DIR=bin
MAIN_PATH=cmd/toolkit/main.go

# Version information
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
DATE := $(shell date -u '+%Y-%m-%d_%H:%M:%S')

# Build flags
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)"

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod

# Targets
.PHONY: all build clean test coverage lint help install deps tidy

all: deps tidy test build

# Build the binary
build:
	@echo "Building $(BINARY_NAME) version $(VERSION)..."
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME) $(MAIN_PATH)
	@echo "Binary built: $(BINARY_DIR)/$(BINARY_NAME)"

# Build for multiple platforms
build-all: build-linux build-darwin build-windows

build-linux:
	@echo "Building for Linux..."
	@mkdir -p $(BINARY_DIR)
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME)-linux-amd64 $(MAIN_PATH)

build-darwin:
	@echo "Building for macOS..."
	@mkdir -p $(BINARY_DIR)
	GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME)-darwin-amd64 $(MAIN_PATH)
	GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME)-darwin-arm64 $(MAIN_PATH)

build-windows:
	@echo "Building for Windows..."
	@mkdir -p $(BINARY_DIR)
	GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME)-windows-amd64.exe $(MAIN_PATH)

# Install dependencies
deps:
	@echo "Installing dependencies..."
	$(GOMOD) download

# Tidy dependencies
tidy:
	@echo "Tidying dependencies..."
	$(GOMOD) tidy

# Run tests
test:
	@echo "Running tests..."
	$(GOTEST) -v -race ./...

# Run tests with coverage
coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -v -race -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Lint code (requires golangci-lint)
lint:
	@echo "Running linter..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not found. Install it from https://golangci-lint.run/usage/install/" && exit 1)
	golangci-lint run

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	$(GOCLEAN)
	rm -rf $(BINARY_DIR)
	rm -f coverage.out coverage.html

# Install binary to GOPATH/bin
install: build
	@echo "Installing binary..."
	cp $(BINARY_DIR)/$(BINARY_NAME) $(GOPATH)/bin/

# Development helpers
dev-setup:
	@echo "Setting up development environment..."
	$(GOGET) -u github.com/golangci/golangci-lint/cmd/golangci-lint

# Run the application
run: build
	./$(BINARY_DIR)/$(BINARY_NAME)

# Run with specific command
run-prewarm: build
	./$(BINARY_DIR)/$(BINARY_NAME) prewarm --help

run-optimize: build
	./$(BINARY_DIR)/$(BINARY_NAME) optimize --help

run-validate: build
	./$(BINARY_DIR)/$(BINARY_NAME) validate --help

run-loadtest: build
	./$(BINARY_DIR)/$(BINARY_NAME) loadtest --help

run-analyze: build
	./$(BINARY_DIR)/$(BINARY_NAME) analyze --help

# Show version
version: build
	./$(BINARY_DIR)/$(BINARY_NAME) version

# Docker targets
docker-build:
	@echo "Building Docker image..."
	docker build -t media-toolkit:$(VERSION) .

docker-run:
	@echo "Running Docker container..."
	docker run --rm -it media-toolkit:$(VERSION)

# Help
help:
	@echo "Available targets:"
	@echo "  build          - Build the binary"
	@echo "  build-all      - Build for all platforms"
	@echo "  clean          - Clean build artifacts"
	@echo "  test           - Run tests"
	@echo "  coverage       - Run tests with coverage"
	@echo "  lint           - Run linter"
	@echo "  install        - Install binary to GOPATH/bin"
	@echo "  deps           - Install dependencies"
	@echo "  tidy           - Tidy dependencies"
	@echo "  dev-setup      - Setup development environment"
	@echo "  run            - Build and run the application"
	@echo "  run-*          - Run specific commands with help"
	@echo "  version        - Show version information"
	@echo "  docker-build   - Build Docker image"
	@echo "  docker-run     - Run Docker container"
	@echo "  help           - Show this help message"