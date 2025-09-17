# ck2sr - ClickHouse to StarRocks 数据同步服务

[![Go Version](https://img.shields.io/badge/Go-1.25.1-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)]()

**ck2sr** 是一个高效、可靠、完整的 ClickHouse 到 StarRocks 数据同步服务，支持分布式部署，能够在生产环境中稳定运行。

## 🌟 核心特性

- **策略驱动**：通过配置文件定义数据同步任务，支持灵活的同步策略
- **流量控制**：支持全局和任务级别的速率限制，防止对源系统造成压力
- **并发控制**：支持多工作单元并发同步，提高同步效率
- **时间窗口**：支持在指定时间段内执行同步任务
- **断点续传**：任务状态持久化，支持服务重启后的断点续传
- **数据完整性**：提供数据校验能力，确保同步的数据完整性
- **分布式协同**：支持 Kubernetes 环境下的分布式部署
- **监控告警**：内置 Prometheus 指标和健康检查接口
- **数据处理管道**：支持数据过滤、转换和验证

## 🏗️ 系统架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ClickHouse    │    │      ck2sr      │    │    StarRocks    │
│   (数据源)      │───▶│   (同步服务)    │───▶│   (目标库)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                               │
                               ▼
                    ┌─────────────────┐
                    │  状态存储       │
                    │ (File/K8s CRD)  │
                    └─────────────────┘
```

### 核心组件

- **配置管理 (Config)**：统一的配置加载和管理
- **任务管理 (Worker)**：数据同步任务的生命周期管理
- **调度器 (Scheduler)**：支持 Cron 表达式的任务调度
- **数据管道 (Pipeline)**：可扩展的数据处理管道
- **状态存储 (Storage)**：任务状态的持久化存储
- **监控 (Monitor)**：Prometheus 指标和健康检查

## 🚀 快速开始

### 前置要求

- Go 1.25.1+
- ClickHouse 23.2.5.107+
- StarRocks 4.0+
- Docker (可选)
- Kubernetes (可选，用于分布式部署)

### 安装

#### 从源码构建

```bash
git clone https://github.com/your-org/ck2sr.git
cd ck2sr
go build -o ck2sr main.go
```

#### 使用 Docker

```bash
docker pull ck2sr:latest
# 或者
docker build -t ck2sr .
```

### 配置

复制配置文件模板：

```bash
cp configs/config.yaml config.yaml
```

编辑配置文件，设置数据库连接信息：

```yaml
clickhouse:
  host: "localhost"
  port: 9000
  username: "default"
  password: ""
  database: "default"

starrocks:
  host: "localhost"
  port: 9030
  username: "root"
  password: ""
  database: "test"
  stream_load_url: "http://localhost:8030"

sync_tasks:
  - task_id: "user_data_sync"
    name: "用户数据同步"
    enabled: true
    source_table: "users"
    target_table: "users_sync"

    # 数据范围
    data_range:
      time_column: "created_at"
      where: "status = 'active'"

    # 数据校验
    validate:
      enabled: true
      check_row_count: true
      tolerance_percent: 1.0
```

### 运行

#### 直接运行

```bash
./ck2sr -config config.yaml
```

#### 使用 Docker

```bash
docker run -d \
  --name ck2sr \
  -p 8080:8080 \
  -p 8081:8081 \
  -v $(pwd)/config.yaml:/app/configs/config.yaml \
  ck2sr:latest
```

#### 使用 Docker Compose

```bash
docker-compose up -d
```

#### Kubernetes 部署

```bash
kubectl apply -f configs/k8s-deployment.yaml
```

## 📖 详细文档

### 配置说明

#### 基本配置

- **数据库连接**：配置 ClickHouse 和 StarRocks 的连接信息
- **全局限制**：设置全局的流量控制、并发控制和重试策略
- **监控配置**：启用 Prometheus 指标和健康检查

#### 任务配置

每个同步任务支持以下配置：

```yaml
sync_tasks:
  - task_id: "unique_task_id"
    name: "任务名称"
    description: "任务描述"
    enabled: true
    source_table: "源表名"
    target_table: "目标表名"

    # 数据范围
    data_range:
      time_column: "时间列名"
      start_time: "2023-01-01 00:00:00"
      end_time: "2023-12-31 23:59:59"
      where: "额外的 WHERE 条件"

    # 时间窗口
    time_window:
      start_time: "01:00"
      end_time: "05:00"

    # 流量控制
    rate_limit:
      max_bytes_per_second: 52428800  # 50MB/s
      max_rows_per_second: 50000
      burst_size: 1000

    # 并发控制
    concurrency:
      max_workers: 5
      batch_size: 10000

    # 重试配置
    retry:
      max_retries: 3
      initial_delay: "1s"
      max_delay: "30s"
      backoff_factor: 2.0

    # 数据验证
    validate:
      enabled: true
      check_row_count: true
      check_checksum: true
      check_columns: ["id", "name"]
      tolerance_percent: 1.0

    # 列映射
    column_mapping:
      old_name: "new_name"
      source_id: "target_id"

    # 调度配置
    cron_expression: "0 2 * * *"  # 每天凌晨 2 点
    priority: 1
```

### API 接口

#### 监控接口

- **健康检查**：`GET /health`
- **就绪检查**：`GET /ready`
- **Prometheus 指标**：`GET /metrics`

#### 管理接口

- **任务列表**：`GET /api/tasks`
- **调度列表**：`GET /api/schedules`
- **同步进度**：`GET /api/progress`

### 数据处理管道

ck2sr 支持可扩展的数据处理管道，内置以下处理器：

#### 列映射处理器 (Column Mapping)

```yaml
processors:
  - name: "column_mapping"
    type: "column_mapping"
    enabled: true
    parameters:
      mapping:
        old_column: "new_column"
        source_id: "target_id"
```

#### 行过滤处理器 (Row Filter)

```yaml
processors:
  - name: "row_filter"
    type: "row_filter"
    enabled: true
    parameters:
      conditions:
        - column: "age"
          operator: "gt"
          value: 18
        - column: "status"
          operator: "eq"
          value: "active"
```

#### 数据验证处理器 (Data Validator)

```yaml
processors:
  - name: "data_validator"
    type: "data_validator"
    enabled: true
    parameters:
      rules:
        - column: "email"
          required: true
          type: "string"
          pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        - column: "age"
          required: true
          type: "int"
          min_value: 0
          max_value: 150
```

## 🔧 运维指南

### 监控

#### Prometheus 指标

服务提供以下 Prometheus 指标：

- `ck2sr_tasks_total`：总任务数
- `ck2sr_tasks_running`：正在运行的任务数
- `ck2sr_tasks_completed`：已完成的任务数
- `ck2sr_tasks_failed`：失败的任务数
- `ck2sr_sync_rows_total`：同步的总行数
- `ck2sr_sync_bytes_total`：同步的总字节数
- `ck2sr_sync_duration_seconds`：同步耗时

#### 健康检查

```bash
# 检查服务健康状态
curl http://localhost:8081/health

# 检查服务就绪状态
curl http://localhost:8081/ready
```

### 日志

支持多种日志格式和输出方式：

```yaml
log:
  level: "info"          # debug, info, warn, error
  format: "json"         # json, text
  output: "stdout"       # stdout, file
  file_path: "/var/log/ck2sr/ck2sr.log"
```

### 性能调优

#### 内存优化

- 调整 `batch_size` 控制批处理大小
- 设置合适的 `max_workers` 数量
- 使用 `rate_limit` 控制内存使用

#### 网络优化

- 配置合适的连接池大小
- 调整读写超时时间
- 使用 `burst_size` 控制突发流量

## 🧪 测试

### 运行测试

```bash
# 运行所有测试
go test ./...

# 运行测试并生成覆盖率报告
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# 使用测试脚本
./scripts/test.sh        # Linux/macOS
scripts/test.bat         # Windows
```

### 基准测试

```bash
go test -bench=. -benchmem ./...
```

## 🤝 贡献指南

我们欢迎社区贡献！请遵循以下步骤：

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 创建 Pull Request

### 开发规范

- 遵循 Go 官方编码规范
- 编写单元测试，确保测试覆盖率 > 80%
- 使用中文注释说明函数和重要逻辑
- 提交前运行 `go fmt` 和 `go vet`

## 🐛 问题反馈

如果您遇到问题或有改进建议，请：

1. 查看 [FAQ](docs/FAQ.md)
2. 搜索现有的 [Issues](https://github.com/your-org/ck2sr/issues)
3. 创建新的 Issue，详细描述问题

## 📋 版本历史

- **v1.0.0** (2024-01-xx)
  - 初始版本发布
  - 支持基本的数据同步功能
  - 支持配置化的同步任务
  - 支持数据完整性验证

## 📜 许可证

本项目采用 Apache 2.0 许可证。详见 [LICENSE](LICENSE) 文件。

## 🙏 致谢

感谢以下开源项目的支持：

- [ClickHouse](https://clickhouse.com/) - 高性能列式数据库
- [StarRocks](https://www.starrocks.io/) - 高性能分析数据库
- [Prometheus](https://prometheus.io/) - 监控系统
- [Kubernetes](https://kubernetes.io/) - 容器编排平台

---

如果您觉得这个项目有用，请给我们一个 ⭐️！