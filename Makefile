# ck2sr Makefile

# 应用信息
APP_NAME := ck2sr
VERSION := 1.0.0
BUILD_TIME := $(shell date +%Y-%m-%d\ %H:%M:%S)
GIT_COMMIT := $(shell git rev-parse --short HEAD)

# Go 相关配置
GO_VERSION := 1.25.1
GOCMD := go
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean
GOTEST := $(GOCMD) test
GOGET := $(GOCMD) get
GOMOD := $(GOCMD) mod

# 构建配置
BINARY_NAME := $(APP_NAME)
BINARY_UNIX := $(BINARY_NAME)_unix
BINARY_WINDOWS := $(BINARY_NAME).exe

# 构建标志
LDFLAGS := -ldflags "-X main.AppVersion=$(VERSION) -X 'main.BuildTime=$(BUILD_TIME)' -X main.GitCommit=$(GIT_COMMIT) -w -s"

# Docker 配置
DOCKER_REGISTRY := your-registry.com
DOCKER_IMAGE := $(DOCKER_REGISTRY)/$(APP_NAME)
DOCKER_TAG := $(VERSION)

# Kubernetes 配置
K8S_NAMESPACE := ck2sr

.PHONY: all build clean test coverage deps fmt vet lint docker docker-push k8s-deploy help

# 默认目标
all: deps fmt vet test build

# 构建
build:
	@echo "Building $(APP_NAME)..."
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_NAME) main.go

# 交叉编译
build-linux:
	@echo "Building for Linux..."
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_UNIX) main.go

build-windows:
	@echo "Building for Windows..."
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_WINDOWS) main.go

build-all: build-linux build-windows build

# 清理
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_UNIX)
	rm -f $(BINARY_WINDOWS)
	rm -rf test-results/

# 依赖管理
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy

# 代码格式化
fmt:
	@echo "Formatting code..."
	$(GOCMD) fmt ./...

# 代码检查
vet:
	@echo "Running go vet..."
	$(GOCMD) vet ./...

# 代码检查 (更严格)
lint:
	@echo "Running golangci-lint..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run

# 测试
test:
	@echo "Running tests..."
	mkdir -p test-results
	$(GOTEST) -v -race -coverprofile=test-results/coverage.out ./tests/...

# 测试覆盖率
coverage: test
	@echo "Generating coverage report..."
	$(GOCMD) tool cover -html=test-results/coverage.out -o test-results/coverage.html
	$(GOCMD) tool cover -func=test-results/coverage.out

# 基准测试
bench:
	@echo "Running benchmarks..."
	mkdir -p test-results
	$(GOTEST) -bench=. -benchmem ./tests/... | tee test-results/bench.log

# 构建 Docker 镜像
docker:
	@echo "Building Docker image..."
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .
	docker tag $(DOCKER_IMAGE):$(DOCKER_TAG) $(DOCKER_IMAGE):latest

# 推送 Docker 镜像
docker-push: docker
	@echo "Pushing Docker image..."
	docker push $(DOCKER_IMAGE):$(DOCKER_TAG)
	docker push $(DOCKER_IMAGE):latest

# 本地运行
run:
	@echo "Running $(APP_NAME)..."
	./$(BINARY_NAME) -config configs/config.yaml

# 本地运行 (开发模式)
run-dev:
	@echo "Running $(APP_NAME) in development mode..."
	$(GOCMD) run main.go -config configs/config.yaml -log-level debug

# Docker Compose
compose-up:
	@echo "Starting services with Docker Compose..."
	docker-compose up -d

compose-down:
	@echo "Stopping services with Docker Compose..."
	docker-compose down

compose-logs:
	@echo "Showing Docker Compose logs..."
	docker-compose logs -f

# Kubernetes 部署
k8s-deploy:
	@echo "Deploying to Kubernetes..."
	kubectl apply -f configs/k8s-deployment.yaml

k8s-delete:
	@echo "Deleting from Kubernetes..."
	kubectl delete -f configs/k8s-deployment.yaml

k8s-status:
	@echo "Checking Kubernetes status..."
	kubectl get pods -n $(K8S_NAMESPACE)
	kubectl get services -n $(K8S_NAMESPACE)

k8s-logs:
	@echo "Showing Kubernetes logs..."
	kubectl logs -n $(K8S_NAMESPACE) -l app=ck2sr -f

# 数据库初始化
db-init:
	@echo "Initializing databases..."
	@echo "Please run the following SQL in ClickHouse:"
	@echo "CREATE DATABASE IF NOT EXISTS test;"
	@echo ""
	@echo "Please run the following SQL in StarRocks:"
	@echo "CREATE DATABASE IF NOT EXISTS test;"

# 配置验证
config-validate:
	@echo "Validating configuration..."
	./$(BINARY_NAME) -config configs/config.yaml -validate

# 生成版本信息
version:
	@echo "App Name: $(APP_NAME)"
	@echo "Version: $(VERSION)"
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Go Version: $(GO_VERSION)"

# 安装工具
install-tools:
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/swaggo/swag/cmd/swag@latest

# 生成 API 文档
docs:
	@echo "Generating API documentation..."
	@which swag > /dev/null || (echo "Installing swag..." && go install github.com/swaggo/swag/cmd/swag@latest)
	swag init -g main.go

# 发布准备
release: clean deps fmt vet lint test build-all docker
	@echo "Release $(VERSION) is ready!"

# 帮助信息
help:
	@echo "Available targets:"
	@echo "  build         - Build the application"
	@echo "  build-linux   - Build for Linux"
	@echo "  build-windows - Build for Windows"
	@echo "  build-all     - Build for all platforms"
	@echo "  clean         - Clean build files"
	@echo "  deps          - Download dependencies"
	@echo "  fmt           - Format code"
	@echo "  vet           - Run go vet"
	@echo "  lint          - Run golangci-lint"
	@echo "  test          - Run tests"
	@echo "  coverage      - Generate test coverage report"
	@echo "  bench         - Run benchmarks"
	@echo "  docker        - Build Docker image"
	@echo "  docker-push   - Push Docker image"
	@echo "  run           - Run the application"
	@echo "  run-dev       - Run in development mode"
	@echo "  compose-up    - Start with Docker Compose"
	@echo "  compose-down  - Stop Docker Compose"
	@echo "  k8s-deploy    - Deploy to Kubernetes"
	@echo "  k8s-delete    - Delete from Kubernetes"
	@echo "  k8s-status    - Check Kubernetes status"
	@echo "  db-init       - Show database initialization commands"
	@echo "  config-validate - Validate configuration"
	@echo "  version       - Show version information"
	@echo "  release       - Prepare release"
	@echo "  help          - Show this help message"