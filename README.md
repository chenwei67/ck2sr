# ck2sr - ClickHouse to StarRocks 数据同步工具

[![Go Version](https://img.shields.io/badge/Go-1.25.1-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-v2.0-brightgreen.svg)]()

**ck2sr** 是一个高性能、可扩展的 ClickHouse 到 StarRocks 数据同步工具，采用清晰的分层架构设计，支持多协议、策略驱动、并发同步，适用于生产环境的大规模数据迁移和同步场景。

## ✨ 核心特性

- **🏗️ 分层架构设计**：Application → Business → Protocol → Infrastructure 清晰的四层架构
- **🔌 多协议支持**：灵活选择 ClickHouse (MySQL/HTTP) 和 StarRocks (MySQL/HTTP/FlightSQL) 协议
- **⚙️ 策略驱动配置**：通过 YAML 配置文件灵活控制同步行为，无需修改代码
- **🚀 并发处理机制**：支持多表并发同步和多 Writer 并发写入，提升同步效率
- **📅 任务调度系统**：基于 Scheduler 的任务管理，支持时间窗口和优先级控制
- **💾 断点续传**：任务状态持久化，支持服务重启后从断点继续同步
- **🎯 数据过滤与映射**：支持列过滤、列映射、固定值列等灵活的数据处理
- **📊 监控与日志**：详细的进度报告、统计信息和可配置的日志输出

## 🏗️ 系统架构

### 分层架构

```
┌─────────────────────────────────────────────────────────┐
│          Application Layer (cmd/)                       │
│  ├─ ck2sr: 主程序入口                                    │
│  └─ dtool: 数据生成工具                                  │
└─────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────┐
│       Business Logic Layer (internal/)                  │
│  ├─ scheduler: 任务调度器（时间窗口、优先级管理）         │
│  ├─ sync: 同步任务管理                                   │
│  │   ├─ SyncTask: 多表同步任务管理                       │
│  │   ├─ TableSyncJob: 单表同步作业                       │
│  │   ├─ Pipeline: 异步数据处理管道                       │
│  │   └─ Monitor: 同步进度监控                            │
│  ├─ storage: 状态持久化（文件存储）                       │
│  └─ config: 配置管理                                      │
└─────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────┐
│          Protocol Layer (internal/)                     │
│  ├─ reader: 数据读取器工厂                               │
│  │   ├─ ClickHouse Reader (MySQL/HTTP)                 │
│  │   └─ StarRocks Reader (MySQL/FlightSQL)             │
│  └─ writer: 数据写入器工厂                               │
│      ├─ ClickHouse Writer (MySQL/HTTP)                 │
│      └─ StarRocks Writer (MySQL/HTTP)                  │
└─────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────┐
│      Infrastructure Layer (pkg/)                        │
│  ├─ protocol: 统一协议抽象（DataReader/DataWriter）      │
│  ├─ clickhouse: ClickHouse 客户端封装                   │
│  ├─ starrocks: StarRocks 客户端封装                     │
│  └─ utils: 工具函数库（retry/array/json/time）          │
└─────────────────────────────────────────────────────────┘
```

### 数据流向

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│ ClickHouse  │───▶│   Pipeline   │───▶│  StarRocks  │
│  (Source)   │    │  (Processing)│    │   (Target)  │
└─────────────┘    └──────────────┘    └─────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  State Storage  │
                  │ (Progress Track)│
                  └─────────────────┘
```

### 核心组件

- **Scheduler（调度器）**：负责任务调度、时间窗口管理和优先级控制
- **SyncTask（同步任务）**：管理多表并发同步，协调 TableSyncJob
- **TableSyncJob（表同步作业）**：单表同步执行单元，管理 Pipeline
- **Pipeline（数据管道）**：异步数据处理管道，支持多 Writer 并发
- **Reader/Writer Factory（工厂模式）**：动态创建不同协议的读写器
- **Storage（状态存储）**：任务状态和进度持久化

## 🚀 快速开始

### 前置要求

- Go 1.25.1+
- ClickHouse 19.0+
- StarRocks 2.0+
- Docker（可选）

### 安装

#### 从源码构建

```bash
git clone https://github.com/sunkaimr/ck2sr.git
cd ck2sr
go build -o ck2sr cmd/ck2sr/main.go
```

#### 使用 Docker

```bash
docker build -t ck2sr:v2.0 .
```

### 配置

创建配置文件 `config.yaml`：

```yaml
# ClickHouse 数据库配置
clickhouse:
  - name: myck-1
    mysql:
      host: "localhost"
      port: 9004
      username: "default"
      password: ""
      timeout: "30s"
    http:
      host: "localhost"
      port: 8123
      username: "default"
      password: ""
      timeout: "30s"

# StarRocks 数据库配置
starrocks:
  - name: mysr-1
    mysql:
      host: "localhost"
      port: 9030
      username: "root"
      password: ""
      timeout: "30s"
    http:
      host: "localhost"
      port: 8030
      username: "root"
      password: ""
      timeout: "30s"

# 同步任务配置
sync_tasks:
  - task_id: "demo_sync_001"
    name: "演示同步任务"
    enabled: true
    priority: 1
    reader:
      name: "myck-1"
      vendor: "clickhouse"
      protocol: "mysql"
      database: "test"
      tables:
        - "orders"
    writer:
      name: "mysr-1"
      vendor: "starrocks"
      protocol: "http"
      database: "test"
      tables:
        - "orders_sync"
    settings:
      batch_size: 10000
      parallel_tables: 1

# 全局策略配置
policy:
  transfer:
    batch_size: 10000
    batch_interval: "5s"
    progress_report_every: 10
    writer_concurrency: 3
  schedule:
    check_interval: "1m"
    max_concurrent_task: 2

# 日志配置
log:
  level: "info"
  format: "text"
  output: "stdout"
```

### 运行

```bash
# 直接运行
./ck2sr --config config.yaml

# 使用 Docker
docker run -d \
  --name ck2sr \
  -v $(pwd)/config.yaml:/app/config.yaml \
  ck2sr:v2.0 --config /app/config.yaml
```

## 📖 配置指南

### 数据库连接配置

#### ClickHouse 配置

```yaml
clickhouse:
  - name: "myck-1"              # 实例名称，用于引用
    mysql:                       # MySQL 协议配置（用于数据读取）
      host: "localhost"
      port: 9004
      username: "default"
      password: ""
      timeout: "30s"
    http:                        # HTTP 协议配置（用于 Stream Load）
      host: "localhost"
      port: 8123
      username: "default"
      password: ""
      timeout: "30s"
```

#### StarRocks 配置

```yaml
starrocks:
  - name: "mysr-1"              # 实例名称，用于引用
    mysql:                       # MySQL 协议配置（用于元数据查询）
      host: "localhost"
      port: 9030
      username: "root"
      password: ""
      timeout: "30s"
    flightsql:                   # FlightSQL 协议配置（高性能读取，可选）
      host: "localhost"
      port: 9408
      username: "root"
      password: ""
      timeout: "30s"
      tls:
        enabled: false
    http:                        # HTTP 协议配置（用于 Stream Load 写入）
      host: "localhost"
      port: 8030
      username: "root"
      password: ""
      timeout: "30s"
```

### 同步任务配置

```yaml
sync_tasks:
  - task_id: "task_001"          # 任务唯一标识
    name: "用户数据同步"           # 任务名称
    enabled: true                 # 是否启用
    priority: 1                   # 优先级（数字越小优先级越高）

    # 数据源配置
    reader:
      name: "myck-1"              # 引用 ClickHouse 实例名称
      vendor: "clickhouse"        # 数据库厂商
      protocol: "mysql"           # 使用的协议：mysql/http
      database: "test"            # 数据库名
      tables:                     # 源表列表
        - "users"
        - "orders"

    # 数据目标配置
    writer:
      name: "mysr-1"              # 引用 StarRocks 实例名称
      vendor: "starrocks"         # 数据库厂商
      protocol: "http"            # 使用的协议：mysql/http/flightsql
      database: "test"            # 数据库名
      tables:                     # 目标表列表（与源表一一对应）
        - "users_sync"
        - "orders_sync"

    # 同步设置
    settings:
      # 数据范围
      data_range:
        time_column: "created_at"           # 时间列名
        start_time: "2024-01-01 00:00:00"  # 起始时间
        end_time: ""                        # 结束时间（空表示同步到最新）

      # 时间窗口
      time_window:
        start_time: "01:00"      # 允许同步的起始时间
        end_time: "05:00"        # 允许同步的结束时间

      # 速率限制
      rate_limit:
        max_bytes_per_second: 52428800    # 最大字节/秒（50MB/s）
        max_rows_per_second: 50000        # 最大行数/秒
        burst_size: 1000                  # 突发大小

      # 重试配置
      retry:
        max_retries: 3
        initial_delay: "2s"
        max_delay: "60s"
        backoff_factor: 2.0

      # 列映射
      column_mapping:
        user_id: "id"              # 源列名: 目标列名
        create_time: "created_at"

      # 批处理设置
      batch_size: 10000            # 批处理大小
      batch_interval: "5s"         # 批次间隔时间
      parallel_tables: 2           # 并行处理表的数量
```

### 全局策略配置

```yaml
policy:
  # 数据传输策略
  transfer:
    batch_size: 10000                     # 默认批次大小
    batch_interval: "5s"                  # 批次间隔时间
    progress_report_every: 10             # 每N批次输出进度
    progress_report_timeout: "10s"        # 或超过N秒强制输出进度
    rate_limit_sleep: "10ms"              # 速率控制休眠时间
    writer_concurrency: 3                 # Writer 并发数量

  # 调度策略
  schedule:
    check_interval: "1m"          # 时间窗口检查间隔
    retry_interval: "5m"          # 失败重试间隔
    max_concurrent_task: 2        # 最大并发任务数

  # HTTP 客户端策略
  http:
    timeout: "30s"                        # 请求超时
    idle_conn_timeout: "60s"              # 空闲连接超时
    tls_handshake_timeout: "10s"          # TLS 握手超时
    max_idle_conns: 100                   # 最大空闲连接数
    max_conns_per_host: 10                # 每个主机最大连接数

  # 数据过滤策略
  filter:
    exclude_columns: []           # 需要排除的列
    fixed_values: {}              # 固定值列（临时方案）

  # 重试策略（全局默认）
  retry:
    max_retries: 3
    initial_delay: "2s"
    max_delay: "60s"
    backoff_factor: 2.0
```

### 日志配置

```yaml
log:
  level: "info"                  # 日志级别：debug, info, warn, error
  format: "text"                 # 日志格式：json, text
  output: "stdout"               # 输出目标：stdout, file
  file_path: "/var/log/ck2sr/ck2sr.log"
  max_size: 100                  # 单个日志文件最大大小（MB）
  max_backups: 10                # 保留的日志文件数量
  max_age: 30                    # 日志文件保留天数
  compress: true                 # 是否压缩归档日志
```

## 💡 使用示例

### 单表同步

```yaml
sync_tasks:
  - task_id: "single_table_sync"
    name: "单表同步示例"
    enabled: true
    reader:
      name: "myck-1"
      vendor: "clickhouse"
      protocol: "mysql"
      database: "test"
      tables: ["users"]
    writer:
      name: "mysr-1"
      vendor: "starrocks"
      protocol: "http"
      database: "test"
      tables: ["users_sync"]
    settings:
      batch_size: 10000
```

### 多表并发同步

```yaml
sync_tasks:
  - task_id: "multi_table_sync"
    name: "多表并发同步示例"
    enabled: true
    reader:
      name: "myck-1"
      vendor: "clickhouse"
      protocol: "mysql"
      database: "test"
      tables: ["users", "orders", "products"]
    writer:
      name: "mysr-1"
      vendor: "starrocks"
      protocol: "http"
      database: "test"
      tables: ["users_sync", "orders_sync", "products_sync"]
    settings:
      batch_size: 10000
      parallel_tables: 3     # 3个表并发同步
```

### 数据过滤和映射

```yaml
sync_tasks:
  - task_id: "filter_and_mapping"
    name: "数据过滤和映射示例"
    enabled: true
    reader:
      name: "myck-1"
      vendor: "clickhouse"
      protocol: "mysql"
      database: "test"
      tables: ["users"]
    writer:
      name: "mysr-1"
      vendor: "starrocks"
      protocol: "http"
      database: "test"
      tables: ["users_sync"]
    settings:
      # 数据范围过滤
      data_range:
        time_column: "created_at"
        start_time: "2024-01-01 00:00:00"

      # 列映射
      column_mapping:
        user_id: "id"
        user_name: "name"
        create_time: "created_at"

      batch_size: 10000

# 全局过滤配置
policy:
  filter:
    exclude_columns:
      - "internal_field"
      - "temp_data"
    fixed_values:
      sync_timestamp: 1704067200
```

### 时间范围同步

```yaml
sync_tasks:
  - task_id: "time_range_sync"
    name: "时间范围同步示例"
    enabled: true
    reader:
      name: "myck-1"
      vendor: "clickhouse"
      protocol: "mysql"
      database: "test"
      tables: ["events"]
    writer:
      name: "mysr-1"
      vendor: "starrocks"
      protocol: "http"
      database: "test"
      tables: ["events_sync"]
    settings:
      data_range:
        time_column: "event_time"
        start_time: "2024-01-01 00:00:00"
        end_time: "2024-12-31 23:59:59"

      # 时间窗口限制（仅在凌晨1点到5点执行）
      time_window:
        start_time: "01:00"
        end_time: "05:00"

      batch_size: 10000
```

## 🔧 开发指南

### 项目结构

```
ck2sr/
├── cmd/                        # 应用程序入口
│   ├── ck2sr/                  # 主程序
│   │   └── main.go
│   └── dtool/                  # 数据生成工具
│       └── main.go
├── internal/                   # 内部业务逻辑
│   ├── client/                 # 客户端管理器
│   ├── config/                 # 配置管理
│   ├── logging/                # 日志管理
│   ├── reader/                 # 数据读取器工厂
│   ├── scheduler/              # 任务调度器
│   ├── storage/                # 状态存储
│   ├── sync/                   # 同步核心逻辑
│   │   ├── task.go             # 同步任务
│   │   ├── table_sync_job.go   # 表同步作业
│   │   ├── pipeline.go         # 数据处理管道
│   │   ├── monitor.go          # 进度监控
│   │   └── converter.go        # 数据转换器
│   └── writer/                 # 数据写入器工厂
├── pkg/                        # 可复用的公共库
│   ├── protocol/               # 协议抽象
│   │   ├── reader.go           # Reader 接口定义
│   │   ├── writer.go           # Writer 接口定义
│   │   └── types.go            # 通用类型定义
│   ├── clickhouse/             # ClickHouse 客户端
│   │   ├── client.go
│   │   ├── mysql.go
│   │   └── http.go
│   ├── starrocks/              # StarRocks 客户端
│   │   ├── client.go
│   │   ├── mysql.go
│   │   ├── flightsql.go
│   │   └── http.go
│   └── utils/                  # 工具函数
│       ├── retry.go
│       ├── array.go
│       ├── json.go
│       └── time.go
├── configs/                    # 配置文件
│   ├── config.yaml
│   └── k8s-deployment.yaml
├── docs/                       # 文档
└── scripts/                    # 脚本
```

### 添加新协议支持

1. **实现 Reader 接口**（在 `internal/reader/` 中）：

```go
type MyCustomReader struct {
    // 实现 protocol.DataReader 接口
}

func (r *MyCustomReader) Next() bool { ... }
func (r *MyCustomReader) GetRecord() (interface{}, error) { ... }
func (r *MyCustomReader) Close() error { ... }
```

2. **实现 Writer 接口**（在 `internal/writer/` 中）：

```go
type MyCustomWriter struct {
    // 实现 protocol.DataWriter 接口
}

func (w *MyCustomWriter) Write(ctx context.Context, records interface{}) error { ... }
func (w *MyCustomWriter) Flush(ctx context.Context) error { ... }
func (w *MyCustomWriter) Close() error { ... }
```

3. **在工厂中注册**：

```go
// 在 reader/factory.go 中
func (f *ReaderFactory) CreateExecutable(cfg *config.ReaderConfig, ...) (ExecutableReader, error) {
    switch cfg.Protocol {
    case "mycustom":
        return NewMyCustomReader(...)
    // ...
    }
}

// 在 writer/factory.go 中
func (f *WriterFactory) Create(cfg *config.WriterConfig, ...) (ExecutableWriter, error) {
    switch cfg.Protocol {
    case "mycustom":
        return NewMyCustomWriter(...)
    // ...
    }
}
```

### 运行测试

```bash
# 运行所有测试
go test ./...

# 运行带覆盖率的测试
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# 运行特定包的测试
go test ./internal/sync/...

# 运行基准测试
go test -bench=. -benchmem ./...
```

## 🔍 运维指南

### 性能调优

#### 批处理大小优化

```yaml
policy:
  transfer:
    batch_size: 10000      # 小数据集：5000-10000
                           # 中等数据集：10000-20000
                           # 大数据集：20000-50000
```

#### Writer 并发优化

```yaml
policy:
  transfer:
    writer_concurrency: 3  # 根据目标数据库性能调整
                           # 建议值：2-5
                           # 过高可能导致目标数据库压力过大
```

#### 连接池优化

```yaml
policy:
  http:
    max_idle_conns: 100          # 最大空闲连接数
    max_conns_per_host: 10       # 每个主机最大连接数
    idle_conn_timeout: "60s"     # 空闲连接超时
```

### 监控指标

当前版本通过日志输出监控信息，包括：

- 任务执行状态
- 数据同步进度（批次数、行数、速率）
- 表级别统计信息
- 错误和异常信息

日志示例：

```
INFO: Starting sync task: demo_sync_001
INFO: Progress [orders]: 10 batches, 100000 rows, 5234.56 rows/sec
INFO: Successfully completed sync for table: orders
INFO: === Task Summary ===
INFO:   Success Rate: 1/1 tables
INFO:   Total Rows: 100000
INFO:   Total Bytes: 10485760
INFO:   Total Duration: 19.123s
```

### 常见问题排查

#### 1. 连接失败

**问题**：无法连接到 ClickHouse 或 StarRocks

**排查步骤**：
- 检查网络连通性：`ping <host>`
- 验证端口是否开放：`telnet <host> <port>`
- 检查用户名和密码是否正确
- 查看数据库日志

#### 2. 同步速度慢

**问题**：数据同步速度不符合预期

**优化建议**：
- 增加 `batch_size`
- 提高 `writer_concurrency`
- 检查网络带宽
- 调整 `rate_limit_sleep`

#### 3. 内存占用过高

**问题**：程序内存占用过高

**解决方案**：
- 减小 `batch_size`
- 降低 `writer_concurrency`
- 减少 `parallel_tables` 数量
- 检查是否有内存泄漏

#### 4. 断点续传不生效

**问题**：重启后未从断点继续

**排查步骤**：
- 检查 `storage_path` 配置是否正确
- 验证进度文件是否存在：`ls ./data/`
- 查看日志中的进度保存信息
- 确认任务 ID 未更改

### 日志分析

#### 调整日志级别

```yaml
log:
  level: "debug"    # 开发调试时使用
  level: "info"     # 生产环境推荐
  level: "warn"     # 仅关注警告和错误
  level: "error"    # 仅记录错误
```

#### 日志格式

```yaml
log:
  format: "json"    # JSON 格式，便于日志收集和分析
  format: "text"    # 文本格式，便于人工阅读
```

## 📋 版本历史

### v2.0（当前版本）

**架构重构**：
- 采用清晰的四层架构设计（Application → Business → Protocol → Infrastructure）
- 实现工厂模式的 Reader/Writer，支持多协议动态切换
- 引入 Scheduler 任务调度器，支持时间窗口和优先级管理
- 实现异步 Pipeline 数据处理管道，支持多 Writer 并发

**核心特性**：
- 多协议支持：ClickHouse（MySQL/HTTP）、StarRocks（MySQL/HTTP/FlightSQL）
- 策略驱动配置：全局策略与任务级配置分离
- 并发处理：多表并发同步、多 Writer 并发写入
- 状态持久化：支持断点续传和进度跟踪
- 数据处理：列过滤、列映射、固定值列

**技术栈**：
- Go 1.25.1
- ClickHouse Client v2
- MySQL Driver
- Apache Arrow ADBC（FlightSQL 支持）

## 🤝 贡献指南

我们欢迎社区贡献！请遵循以下步骤：

1. Fork 本仓库
2. 创建特性分支：`git checkout -b feature/amazing-feature`
3. 提交更改：`git commit -m 'Add some amazing feature'`
4. 推送到分支：`git push origin feature/amazing-feature`
5. 创建 Pull Request

### 开发规范

- 遵循 Go 官方编码规范
- 编写单元测试，确保测试覆盖率 > 70%
- 使用中文注释说明函数和重要逻辑
- 提交前运行 `go fmt` 和 `go vet`
- 保持单文件行数在 500 行以内
- 遵循分层架构，避免跨层调用

### 提交信息规范

```
<type>(<scope>): <subject>

<body>

<footer>
```

**类型（type）**：
- `feat`: 新功能
- `fix`: Bug 修复
- `docs`: 文档更新
- `style`: 代码格式调整
- `refactor`: 重构
- `perf`: 性能优化
- `test`: 测试相关
- `chore`: 构建/工具相关

## 📜 许可证

本项目采用 Apache 2.0 许可证。详见 [LICENSE](LICENSE) 文件。

## 🙏 致谢

感谢以下开源项目的支持：

- [ClickHouse](https://clickhouse.com/) - 高性能列式数据库
- [StarRocks](https://www.starrocks.io/) - 高性能分析数据库
- [Apache Arrow](https://arrow.apache.org/) - 列式数据格式和 FlightSQL 协议
- [Logrus](https://github.com/sirupsen/logrus) - 结构化日志库
- [YAML](https://github.com/go-yaml/yaml) - YAML 解析库

## 📞 联系方式

- 问题反馈：[GitHub Issues](https://github.com/sunkaimr/ck2sr/issues)
- 项目主页：[GitHub Repository](https://github.com/sunkaimr/ck2sr)

---

**如果这个项目对您有帮助，请给我们一个 Star ⭐️**
