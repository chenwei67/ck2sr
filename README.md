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
- **📅 两阶段调度算法**：Phase 1 执行未完成任务，Phase 2 自动重试失败任务，确保数据完整性
- **🔄 统一重试机制**：Reader/Writer/Scheduler 三级重试，支持指数退避和随机抖动
- **💾 断点续传**：任务状态持久化，支持服务重启后从断点继续同步，避免重复传输
- **🛡️ 幂等性保障**：已完成表不重复同步，失败任务支持断点续传（基于 synced_rows offset）
- **⏱️ 超时保护机制**：轮询超时保护（1小时），避免任务挂起导致进程阻塞
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
      port: 9000
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
      batch_interval: "5s"
      parallel_tables: 1

# 全局策略配置
policy:
  schedule:
    check_interval: "1m"
    retry_interval: "5m"
    retry_times: 3
    max_concurrent_task: 2
  retry:
    max_attempts: 3
    initial_backoff: "1s"
    max_backoff: "60s"
    backoff_multiplier: 2.0
    jitter: true

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
      port: 9000
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

      # 数据过滤策略
      filter:
        # 排除列列表（在所有任务中排除这些列）
        exclude_columns:
        - "internal_field"
        - "temp_column"
        # 固定值列映射（将指定列替换为固定值）
        fixed_values:
          sync_timestamp: 1740924169
          sync_flag: "manual"

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
  # 调度策略
  schedule:
    # 时间窗口策略（全局配置，所有任务遵循统一时间窗口）
    time_window:
      enabled: false                 # 是否启用时间窗口策略
      start_time: ""                 # 窗口起始时间（格式：HH:MM，如"01:00"）
      end_time: ""                   # 窗口结束时间（格式：HH:MM，如"05:00"）
    check_interval: "1m"                # 时间窗口检查间隔（用于轮询）
    retry_interval: "5m"                # 失败任务重试等待时间
    retry_times: 3                      # 失败重试次数：0=不重试，-1=无限重试，N=最多重试N次
    max_concurrent_task: 2              # 最大并发任务数（串行执行时设置为1）

  # HTTP 客户端策略
  http:
    timeout: "30s"                      # 请求超时
    idle_conn_timeout: "60s"            # 空闲连接超时
    tls_handshake_timeout: "10s"        # TLS握手超时
    max_idle_conns: 100                 # 最大空闲连接数
    max_conns_per_host: 10              # 每个主机最大连接数

  # 重试策略（统一Reader、Writer、Scheduler的重试策略）
  retry:
    max_attempts: 3                     # 最大重试次数（包含首次尝试）
    initial_backoff: "1s"               # 初始退避时间（首次重试等待时间）
    max_backoff: "60s"                  # 最大退避时间（防止退避时间过长）
    backoff_multiplier: 2.0             # 退避倍数（指数退避）
    jitter: true                        # 随机抖动（±25%，避免惊群效应）
```

**关键配置说明**：

#### schedule 配置
- `time_window`：时间窗口策略（全局配置）
  - `enabled`：是否启用时间窗口策略（默认 false）
  - `start_time`：窗口起始时间（HH:MM格式，如 "01:00"）
  - `end_time`：窗口结束时间（HH:MM格式，如 "05:00"）
  - 支持跨日窗口：如 `start_time="23:00", end_time="03:00"` 表示晚上11点到凌晨3点
  - 空值表示全天候执行，无时间限制
- `check_interval`：时间窗口检查间隔，窗口外时按此间隔轮询等待
- `retry_times`：任务级别重试次数
  - `0`：不重试，任务失败后直接退出
  - `-1`：无限重试，直到成功或手动停止
  - `N`：最多重试N次（N > 0）
- `retry_interval`：每次重试之间的等待时间
- `max_concurrent_task`：最大并发任务数，串行执行时设置为 `1`

#### retry 配置（统一重试策略）
- `max_attempts`：最大尝试次数（包含首次尝试，实际重试次数为 `max_attempts-1`）
- `initial_backoff`：首次重试等待时间（例如：1s）
- `max_backoff`：最大等待时间上限（例如：60s）
- `backoff_multiplier`：指数退避倍数
  - 第1次重试等待：`initial_backoff * backoff_multiplier^0 = 1s`
  - 第2次重试等待：`initial_backoff * backoff_multiplier^1 = 2s`
  - 第3次重试等待：`initial_backoff * backoff_multiplier^2 = 4s`
  - 以此类推，直到达到 `max_backoff`
- `jitter`：随机抖动开关
  - `true`：在退避时间上增加 ±25% 随机浮动
  - `false`：使用固定的退避时间
  - 用途：避免多个失败任务同时重试导致的惊群效应

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

**日志级别说明**：
- `debug`：调试模式，输出详细的轮询日志、状态变更日志，用于故障排查
- `info`：生产环境推荐，输出任务执行、进度、统计等关键信息
- `warn`：仅输出警告和错误信息
- `error`：仅输出错误信息

### 状态文件说明

ck2sr 使用文件系统持久化任务状态和进度信息，支持断点续传和任务恢复。状态文件存储在 `service.storage_path` 配置的目录中（默认 `./data`）。

#### 任务状态文件

**文件命名**：`task_${task_id}.json`

**示例**：`task_ck2sr_demo_001.json`

```json
{
  "task_id": "ck2sr_demo_001",
  "status": "success",
  "schedule_times": 1,
  "failed_times": 0,
  "started_at": "2025-10-10T21:51:45.495767435+08:00",
  "last_run_time": "2025-10-10T21:51:45.495766009+08:00",
  "updated_at": "2025-10-10T21:52:15.123456789+08:00",
  "finished_at": "2025-10-10T21:52:15.123456789+08:00",
  "last_error": ""
}
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `task_id` | string | 任务唯一标识 |
| `status` | string | 任务状态：`idle`（未执行）、`running`（执行中）、`success`（成功）、`failed`（失败）、`paused`（时间窗口外暂停） |
| `schedule_times` | int64 | 调度次数（包含首次执行和所有重试） |
| `failed_times` | int64 | 失败次数 |
| `started_at` | timestamp | 首次启动时间 |
| `last_run_time` | timestamp | 最后一次运行时间 |
| `updated_at` | timestamp | 状态最后更新时间 |
| `finished_at` | timestamp | 任务完成时间 |
| `last_error` | string | 最后一次错误信息（成功时为空） |

**状态转换图**：

```
     ┌──────┐
     │ idle │ (初始状态/未执行)
     └──┬───┘
        │ Start()
        ▼
   ┌─────────┐
   │ running │ (执行中)
   └────┬────┘
        │
    ┌───┴────┐
    ▼        ▼
┌─────────┐ ┌────────┐
│ success │ │ failed │
└─────────┘ └───┬────┘
                │
          Phase 2 Retry
                │
                ▼
          ┌─────────┐
          │ running │
          └─────────┘
```

#### 进度文件

**文件命名**：`progress_${task_id}_${table}.json`

**示例**：`progress_ck2sr_demo_001_orders.json`

```json
{
  "task_id": "ck2sr_demo_001",
  "source_db": "test",
  "source_table": "orders",
  "target_db": "test_sync",
  "target_table": "orders_sync",
  "total_rows": 100000,
  "synced_rows": 100000,
  "synced_bytes": 10485760,
  "start_sync_time": "2025-10-10T21:51:45.496063445+08:00",
  "last_sync_time": "2025-10-10T21:52:15.576051898+08:00",
  "progress": 100.0
}
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `task_id` | string | 所属任务ID |
| `source_db` | string | 源数据库名 |
| `source_table` | string | 源表名 |
| `target_db` | string | 目标数据库名 |
| `target_table` | string | 目标表名 |
| `total_rows` | int64 | 总行数（估算值，基于 COUNT(*) 查询）|
| `synced_rows` | int64 | 已同步行数（用于断点续传的 OFFSET）|
| `synced_bytes` | int64 | 已同步字节数（用于速率统计）|
| `start_sync_time` | timestamp | 同步开始时间 |
| `last_sync_time` | timestamp | 最后同步时间 |
| `progress` | float64 | 进度百分比（0.0-100.0）|

**断点续传逻辑**：

1. 任务启动时，读取 `progress_${task_id}_${table}.json` 文件
2. 如果 `progress >= 100.0` 或 `synced_rows >= total_rows`，跳过该表（幂等性）
3. 如果 `0 < synced_rows < total_rows`，从 `OFFSET synced_rows` 继续同步
4. 每个批次写入成功后，更新 `synced_rows` 和 `synced_bytes`

**状态文件路径配置**：

```yaml
service:
  storage_path: "./data"  # 修改此路径可自定义状态文件存储位置
```

**清理状态文件**：

如需重新开始同步（不使用断点续传），删除对应的状态文件即可：

```bash
# 删除指定任务的所有状态文件
rm -f ./data/task_ck2sr_demo_001.json
rm -f ./data/progress_ck2sr_demo_001_*.json

# 清空所有状态文件
rm -f ./data/*.json
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

      # 数据过滤
      filter:
        exclude_columns:
          - "internal_field"
          - "temp_data"
        fixed_values:
          sync_timestamp: 1704067200
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
- 验证防火墙规则

**常见错误**：
```
ERROR: failed to connect to mysql://localhost:9004: dial tcp: connection refused
```

**解决方案**：
```yaml
# 确认配置正确
clickhouse:
  - name: myck-1
    mysql:
      host: "192.168.1.100"  # 确保 host 可达
      port: 9004              # 确认端口正确
      username: "default"
      password: "your_password"
```

#### 2. 任务挂起（Task Hang）

**问题**：任务执行后无响应，无法继续执行后续任务

**症状**：
- 日志停留在某个表同步完成后
- 任务状态文件显示 `"status": "running"`
- 进度文件显示 `"progress": 100.0`

**排查步骤**：

1. **检查任务状态文件** (`./data/task_${task_id}.json`)：
```bash
cat ./data/task_ck2sr_demo_001.json
```

如果看到 `"status": "running"` 且已过去很长时间，说明任务可能挂起。

2. **检查进度文件** (`./data/progress_${task_id}_${table}.json`)：
```bash
cat ./data/progress_ck2sr_demo_001_table3.json
```

如果 `"progress": 100.0` 但任务状态仍为 `running`，说明状态同步异常。

3. **检查日志中的轮询信息**（需设置 `log.level: "debug"`）：
```
DEBUG Polling progress for table table3 (poll #1): status=completed, processed=10000/10000 rows
INFO Successfully completed sync for table: table3
```

如果看不到 `Successfully completed` 日志，说明轮询未能正常退出。

**根因**：
- ✅ **已修复**：v2.0 版本已修复此问题（同步状态更新和轮询超时保护）
- 状态异步更新导致轮询无法读取到 `completed` 状态
- 轮询循环缺少超时保护机制

**解决方案**：

1. **临时解决**：手动更新状态文件并重启
```bash
# 停止进程
kill <pid>

# 手动修改任务状态为 success
vi ./data/task_ck2sr_demo_001.json
# 将 "status": "running" 改为 "status": "success"

# 重启进程
./ck2sr --config config.yaml
```

2. **永久解决**：升级到 v2.0+ 版本（已内置修复）

**预防措施**：
- 设置 `log.level: "debug"` 用于故障排查
- 配置 `policy.schedule.max_concurrent_task: 1` 串行执行任务，便于定位问题

#### 3. 断点续传不生效

**问题**：重启后未从断点继续，而是从头开始同步

**排查步骤**：

1. **检查存储路径配置**：
```yaml
service:
  storage_path: "./data"  # 确保路径存在且可写
```

2. **验证进度文件是否存在**：
```bash
ls -lh ./data/progress_*.json
```

3. **查看进度文件内容**：
```bash
cat ./data/progress_ck2sr_demo_001_orders.json
```

确认 `synced_rows` 字段是否正确记录。

4. **检查日志中的偏移量信息**：
```
INFO Loaded progress for table orders: offset=50000, processed_rows=50000, total_rows=100000
INFO Starting table synchronization for orders from offset 50000
```

**常见原因**：
- 存储路径不正确或无写入权限
- 进度文件被误删除
- 任务 ID (`task_id`) 发生变化
- 表名发生变化

**解决方案**：
```bash
# 检查目录权限
ls -ld ./data
# 应输出类似：drwxr-xr-x ... ./data

# 检查文件权限
ls -l ./data/*.json
# 应有读写权限

# 如需重新开始，删除进度文件
rm -f ./data/progress_ck2sr_demo_001_orders.json
```

#### 4. 同步速度慢

**问题**：数据同步速度不符合预期

**诊断**：

1. **查看进度日志**：
```
INFO Progress [orders]: 10 batches, 100000 rows, 5234.56 rows/sec, 10.5 MB, 1.05 MB/sec
```

2. **计算期望速率**：
- 网络带宽：1 Gbps = 125 MB/s
- 实际速率：1.05 MB/s（远低于预期）

**优化建议**：

1. **增加批次大小**：
```yaml
settings:
  batch_size: 20000      # 从 10000 增加到 20000
  batch_bytes: 10485760  # 10MB
```

2. **提高 Writer 并发**：
```yaml
policy:
  transfer:
    writer_concurrency: 5  # 从 3 增加到 5
```

3. **减少批次间隔**：
```yaml
settings:
  batch_interval: "100ms"  # 从 5s 减少到 100ms
```

4. **检查网络带宽**：
```bash
# 测试网络速度
iperf3 -c <target_host>
```

5. **检查目标数据库性能**：
- 查看 StarRocks BE 节点的 CPU/内存/磁盘 IO
- 检查是否有慢查询

#### 5. 内存占用过高

**问题**：程序内存占用过高，可能导致 OOM

**诊断**：
```bash
# 查看内存占用
ps aux | grep ck2sr
# USER       PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
# root     12345  5.0 15.2 2048000 1024000 ?    Ssl  10:00   0:30 ./ck2sr
```

**优化方案**：

1. **减小批次大小**：
```yaml
settings:
  batch_size: 5000       # 从 20000 减少到 5000
  batch_bytes: 5242880   # 5MB
```

2. **降低 Writer 并发**：
```yaml
policy:
  transfer:
    writer_concurrency: 2  # 从 5 减少到 2
```

3. **减少并行表数量**：
```yaml
settings:
  parallel_tables: 1  # 串行处理表
```

4. **检查是否有内存泄漏**：
```bash
# 使用 pprof 分析内存
go tool pprof http://localhost:6060/debug/pprof/heap
```

#### 6. 重试不生效

**问题**：任务失败后未自动重试

**排查步骤**：

1. **检查 `retry_times` 配置**：
```yaml
policy:
  schedule:
    retry_times: 3  # 确保不为 0
```

如果 `retry_times: 0`，则不会重试。

2. **检查重试日志**：
```
INFO Phase 1: Task ck2sr_demo_001 will be executed (status: running)
INFO Phase 1: Completed 1 tasks
INFO Phase 2: Retry attempt 1/3 for 0 failed tasks
INFO Phase 2: No failed tasks to retry
```

3. **检查任务状态**：
```bash
cat ./data/task_ck2sr_demo_001.json
```

如果 `"status": "failed"`，应该在 Phase 2 被重试。

**常见原因**：
- `retry_times` 设置为 0
- 任务在 Phase 1 已成功，无需重试
- `retry_interval` 设置过长，仍在等待

**解决方案**：
```yaml
policy:
  schedule:
    retry_times: 3          # 或 -1（无限重试）
    retry_interval: "1m"    # 减少等待时间
```

#### 7. 数据不一致

**问题**：源表和目标表数据行数不一致

**诊断**：

1. **检查同步进度**：
```bash
cat ./data/progress_ck2sr_demo_001_orders.json
# "synced_rows": 100000, "total_rows": 100000
```

2. **验证数据行数**：
```sql
-- 源表
SELECT COUNT(*) FROM clickhouse.test.orders;
-- 100000

-- 目标表
SELECT COUNT(*) FROM starrocks.test.orders_sync;
-- 99500 (不一致！)
```

**可能原因**：
- 数据写入过程中发生错误但未正确处理
- 目标表有唯一键约束，部分数据被忽略
- Stream Load 失败但未报错

**排查方法**：

1. **检查写入日志**：
```
INFO HTTP insert 10000 records, 1048576 bytes data to test.orders_sync
ERROR failed to insert data: stream load failed with status 500: duplicate key
```

2. **检查 StarRocks Stream Load 结果**：
```
INFO Stream load response: {Status:Success NumberLoadedRows:9950 NumberFilteredRows:50 ...}
```

如果 `NumberFilteredRows > 0`，说明有数据被过滤。

**解决方案**：
- 检查目标表约束（主键、唯一键）
- 启用 `log.level: "debug"` 查看详细写入日志
- 对比源表和目标表的数据差异：
```sql
-- 找出缺失的数据
SELECT * FROM clickhouse.test.orders
WHERE id NOT IN (SELECT id FROM starrocks.test.orders_sync);
```

### 日志分析

#### 调整日志级别

```yaml
log:
  level: "debug"    # 开发调试时使用，输出轮询日志、状态变更日志
  level: "info"     # 生产环境推荐，输出任务执行、进度、统计信息
  level: "warn"     # 仅关注警告和错误
  level: "error"    # 仅记录错误
```

#### 关键日志示例

**正常执行日志**：
```
INFO Starting sync task: ck2sr_demo_001
INFO Starting sync for table: orders
INFO Progress [orders]: 10 batches, 100000/100000 rows (100.0%), 5234.56 rows/sec, 10.5 MB, 1.05 MB/sec
INFO Successfully completed sync for table: orders
INFO === Task Summary ===
INFO   Success Rate: 1/1 tables
INFO   Total Rows: 100000
INFO   Total Bytes: 10485760
INFO   Total Duration: 19.123s
INFO All tasks completed successfully
```

**异常执行日志**：
```
INFO Starting sync task: ck2sr_demo_001
INFO Starting sync for table: orders
DEBUG Polling progress for table orders (poll #1): status=running, processed=50000/100000 rows
ERROR failed to write batch: stream load failed with status 500
ERROR Table orders sync failed
WARN Partial success: 0/1 tables completed
ERROR Scheduler execution failed: all tables failed, last error: table orders: stream load failed
```

#### 日志格式

```yaml
log:
  format: "json"    # JSON 格式，便于日志收集和分析（推荐用于生产环境）
  format: "text"    # 文本格式，便于人工阅读（推荐用于开发调试）
```

**JSON 格式示例**：
```json
{"level":"info","msg":"Starting sync task: ck2sr_demo_001","time":"2025-10-10T21:51:45+08:00"}
{"level":"info","msg":"Progress [orders]: 10 batches, 100000 rows","table":"orders","time":"2025-10-10T21:52:00+08:00"}
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
