# ck2sr - ClickHouse to StarRocks 数据同步服务

[![Go Version](https://img.shields.io/badge/Go-1.25.1-blue.svg)](https://golang.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/Build-Passing-brightgreen.svg)]()

**ck2sr** 是一个高效、可靠、完整的 ClickHouse 到 StarRocks 数据同步服务，采用 **ClickHouse ArrowStream 原生格式** 和 **StarRocks Arrow Flight SQL** 实现**真正的零拷贝**数据传输，支持分布式部署，能够在生产环境中稳定运行。

## 🌟 核心特性

- **🚀 真正的零拷贝架构**：ClickHouse 使用原生 `FORMAT ArrowStream` 直接输出，StarRocks 使用 Arrow Flight SQL 流式写入，实现端到端零拷贝传输
- **⚡ 极致性能优化**：基于 Apache Arrow 列式内存格式，避免数据序列化/反序列化开销，显著提升同步性能
- **🔄 流式传输架构**：支持大数据集的实时流式传输，内存占用低，支持TB级数据同步
- **🆕 智能数据转换引擎**：实时数据转换、类型转换、数据验证和清洗 (v2.2.0+)
- **🆕 灵活表名控制**：直接使用配置的目标表名，无需自动后缀 (v2.2.0+)
- **策略驱动**：通过配置文件定义数据同步任务，支持灵活的同步策略
- **流量控制**：支持全局和任务级别的速率限制，防止对源系统造成压力
- **并发控制**：支持多工作单元并发同步，提高同步效率
- **时间窗口**：支持在指定时间段内执行同步任务
- **断点续传**：任务状态持久化，支持服务重启后的断点续传
- **数据完整性**：提供数据校验能力，确保同步的数据完整性
- **分布式协同**：支持 Kubernetes 环境下的分布式部署
- **监控告警**：内置 Prometheus 指标和健康检查接口
- **数据处理管道**：支持数据过滤、转换和验证
- **压缩传输**：支持多种压缩算法 (LZ4、ZSTD、GZIP) 降低网络带宽占用
- **灵活日志控制**：命令行选项控制日志格式，支持生产环境优化

## 🏗️ 系统架构

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ClickHouse    │    │      ck2sr      │    │    StarRocks    │
│   (数据源)      │───▶│   (同步服务)    │───▶│   (目标库)      │
│                 │    │                 │    │                 │
│ FORMAT          │    │ ArrowStream     │    │ Arrow Flight    │
│ ArrowStream     │    │ Zero-Copy       │    │ SQL Streaming   │
│ 原生输出        │    │ Streaming       │    │ Direct Write    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                               │
                               ▼
                    ┌─────────────────┐
                    │  状态存储       │
                    │ (File/K8s CRD)  │
                    └─────────────────┘

数据流: ClickHouse ArrowStream → ArrowStreamWriter → StarRocks Flight SQL
协议: TCP Native ArrowStream + gRPC Arrow Flight SQL + 零拷贝内存传输
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
- ClickHouse 21.12+ (需支持 ArrowStream 格式输出)
- StarRocks 4.0+ (需支持 Arrow Flight SQL)
- Docker (可选)
- Kubernetes (可选，用于分布式部署)

**注意**:
- 确保 ClickHouse 支持 `FORMAT ArrowStream` 查询语法（ClickHouse 21.12+ 版本支持）
- StarRocks 实例已开启 Arrow Flight SQL 支持
- 如遇到连接问题，请参考 [故障排除文档](docs/troubleshooting-clickhouse-connection.md)

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

  # ArrowStream 配置
  batch_size: 10000
  compression_type: "lz4"  # lz4, gzip, none
  max_block_size: 100000
  read_timeout: "30s"
  write_timeout: "30s"
  max_idle_conns: 10
  max_open_conns: 100
  conn_max_lifetime: "1h"

starrocks:
  host: "localhost"
  port: 9030
  username: "root"
  password: ""
  database: "test"

  # Arrow Flight SQL 配置
  flight_sql_endpoint: "localhost"
  flight_sql_port: 9090
  flight_sql_auth:
    username: "flight_user"
    password: "flight_pass"
  use_tls: false
  flight_timeout: "30s"
  batch_size: 10000
  compression_type: "lz4"  # none, gzip, lz4, zstd

sync_tasks:
  - task_id: "user_data_sync"
    name: "用户数据同步"
    enabled: true
    source_table: "users"
    target_table: "users_sync"    # v2.2.0+: 直接使用此表名，不再自动添加后缀

    # 数据范围
    data_range:
      time_column: "created_at"
      where: "status = 'active'"

    # 🆕 数据转换配置 (v2.2.0+)
    data_transform:
      enabled: true
      field_transforms:
        - column: "username"
          type_conversion:
            source_type: "string"
            target_type: "varchar"
            expression: "trim"      # 去除首尾空格
          validation:
            pattern: "^[a-zA-Z0-9_]+$"
          required: true
        - column: "age"
          type_conversion:
            source_type: "int64"
            target_type: "int32"
          validation:
            min_value: 0
            max_value: 150
          default_value: 0
      global_rules:
        - source_type: "float64"
          target_type: "double"

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

### 🆕 v2.2.0 新功能文档
- [数据转换功能完整指南](docs/data-transformation.md) - 字段级转换、验证、表达式处理
- [更新日志](docs/CHANGELOG.md) - 详细的版本更新记录
- [配置文件示例](configs/config.yaml) - 包含数据转换功能的完整配置示例

### 配置说明

#### 混合高性能数据传输架构

ck2sr v2.1+ 采用混合架构进行高性能数据传输，相比传统的 Stream Load 方式具有以下优势：

- **零拷贝传输**: ClickHouse ArrowStream 直接传输到 StarRocks Arrow Flight SQL
- **高性能**: 基于列式存储格式，端到端 Arrow 格式传输
- **标准化**: ClickHouse 使用原生 TCP 协议，StarRocks 使用 Arrow Flight SQL 标准协议
- **压缩传输**: 支持多种压缩算法降低网络开销
- **流式处理**: 支持大数据集的流式传输

##### ClickHouse TCP + ArrowStream 配置

```yaml
clickhouse:
  # 基本数据库连接配置
  host: "localhost"
  port: 9000
  username: "default"
  password: ""
  database: "default"

  # ArrowStream 配置
  batch_size: 10000                     # Arrow 批次大小
  compression_type: "lz4"               # 压缩算法: lz4, gzip, none
  max_block_size: 100000                # ClickHouse 块大小
  read_timeout: "30s"                   # TCP 读取超时
  write_timeout: "30s"                  # TCP 写入超时
  max_idle_conns: 10                    # 最大空闲连接数
  max_open_conns: 100                   # 最大连接数
  conn_max_lifetime: "1h"               # 连接最大生命周期
```

##### StarRocks Arrow Flight SQL 配置

```yaml
starrocks:
  # 基本数据库连接配置 (MySQL 协议用于元数据)
  host: "localhost"
  port: 9030
  username: "root"
  password: ""
  database: "test"

  # Arrow Flight SQL 配置
  flight_sql_endpoint: "localhost"      # Flight SQL 服务地址
  flight_sql_port: 9090                 # Flight SQL 服务端口
  flight_sql_auth:                      # Flight SQL 独立认证
    username: "flight_user"             # Flight SQL 用户名
    password: "flight_pass"             # Flight SQL 密码
  use_tls: false                        # 是否启用 TLS 加密
  flight_timeout: "30s"                 # Flight SQL 写入超时时间
  batch_size: 10000                     # Arrow 批次大小
  compression_type: "lz4"               # 压缩算法: none, gzip, lz4, zstd
  max_message_size: 100                 # gRPC 最大消息大小 (MB)
```

#### 从旧版本迁移到混合架构

如果您正在从旧版本的 ck2sr 升级，需要进行以下配置更新：

1. **更新 ClickHouse 配置 (从 Flight SQL 到 TCP + ArrowStream)**:
   ```yaml
   # 旧配置 (需要删除)
   clickhouse:
     flight_sql_endpoint: "localhost"
     flight_sql_port: 9090
     use_tls: false

   # 新配置
   clickhouse:
     host: "localhost"
     port: 9000  # TCP 端口
     batch_size: 10000
     compression_type: "lz4"
     max_block_size: 100000
     # ... 其他 TCP 配置
   ```

2. **保持 StarRocks Arrow Flight SQL 配置**:
   ```yaml
   # StarRocks 配置保持不变，继续使用 Arrow Flight SQL
   starrocks:
     flight_sql_endpoint: "localhost"
     flight_sql_port: 9090
     flight_sql_auth:
       username: "flight_user"
       password: "flight_pass"
     # ... 其他 Flight SQL 配置
   ```

3. **确保数据库支持**:
   - 验证 ClickHouse 支持 `FORMAT ArrowStream` 查询语法
   - 验证 StarRocks 实例已启用 Arrow Flight SQL 支持

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

#### 🆕 数据转换处理器 (Data Transform) - v2.2.0+

```yaml
data_transform:
  enabled: true
  field_transforms:
    - column: "username"
      type_conversion:
        source_type: "string"
        target_type: "varchar"
        expression: "trim"        # 去除首尾空格
      validation:
        pattern: "^[a-zA-Z0-9_]+$"  # 正则验证
        allow_null: false
      required: true
    - column: "price"
      type_conversion:
        source_type: "float64"
        target_type: "decimal(15,2)"
      validation:
        min_value: 0.01
        max_value: 999999.99
      default_value: 0.00
  global_rules:
    - source_type: "string"
      target_type: "varchar"
    - source_type: "int64"
      target_type: "bigint"
```

**支持的转换**:
- **类型转换**: int64→int32, string→varchar, float64→decimal 等
- **表达式转换**: upper, lower, trim, sprintf格式化
- **数据验证**: 正则表达式、数值范围、空值检查
- **默认值**: 处理缺失或无效数据
- **必需字段**: 确保关键数据完整性

详细配置请参考: [数据转换功能文档](docs/data-transformation.md)

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

#### 混合架构性能优化

- **批次大小调优**: 调整 `batch_size` 平衡内存使用和传输效率
  ```yaml
  # ClickHouse ArrowStream 推荐配置
  clickhouse:
    batch_size: 5000     # 小数据集
    batch_size: 10000    # 中等数据集
    batch_size: 20000    # 大数据集

  # StarRocks Flight SQL 推荐配置
  starrocks:
    batch_size: 5000     # 小数据集
    batch_size: 10000    # 中等数据集
    batch_size: 20000    # 大数据集
  ```

- **压缩算法选择**: 根据网络和 CPU 资源选择合适的压缩算法
  ```yaml
  compression_type: "lz4"    # 高压缩速度，适合 CPU 密集场景
  compression_type: "gzip"   # 通用压缩，兼容性好
  compression_type: "none"   # 无压缩，适合高速内网环境
  ```

- **ClickHouse TCP 连接优化**: 调整连接池和超时配置
  ```yaml
  clickhouse:
    max_open_conns: 100      # 最大连接数
    max_idle_conns: 10       # 最大空闲连接数
    conn_max_lifetime: "1h"  # 连接生命周期
    read_timeout: "30s"      # 读取超时
    write_timeout: "30s"     # 写入超时
  ```

- **StarRocks Flight SQL 超时配置**: 根据数据量调整合理的超时时间
  ```yaml
  starrocks:
    flight_timeout: "30s"   # 小批次数据
    flight_timeout: "60s"   # 中等批次数据
    flight_timeout: "120s"  # 大批次数据
    max_message_size: 100   # gRPC 最大消息大小 (MB)
  ```

#### 内存优化

- 调整 `batch_size` 控制批处理大小
- 设置合适的 `max_workers` 数量
- 使用 `rate_limit` 控制内存使用
- Arrow 零拷贝传输减少内存分配和释放开销
- ClickHouse TCP 连接池复用减少内存占用

#### 网络优化

- 配置合适的连接池大小
- 调整读写超时时间
- 使用 `burst_size` 控制突发流量
- 选择合适的压缩算法降低网络传输量
- ClickHouse TCP 连接复用减少连接开销
- StarRocks gRPC 连接池优化

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

- **v2.3.0** (2024-09-18) - **重大架构重构**
  - **🚀 真正零拷贝流式传输**: 重构为 ClickHouse ArrowStream → StarRocks Flight SQL 直接流式传输
  - **⚡ 性能革命性提升**: 消除了数据行级处理和Arrow重构的性能瓶颈
  - **🔄 流式写入器**: 新增 ArrowStreamWriter 支持连续流式写入，避免重复连接开销
  - **🛠️ 重构同步工作器**: 实现端到端零拷贝数据传输架构
  - **✅ 完整测试验证**: 所有模块编译测试通过，确保生产就绪
  - **📚 文档全面更新**: 反映新的零拷贝架构和性能优势

- **v2.2.0** (2024-09-17) - 数据转换与目标表优化
  - **🎉 数据转换引擎**: 支持字段级数据转换、表达式转换和验证
  - **🐛 目标表命名优化**: 直接使用配置的 `target_table` 字段，不再自动添加后缀
  - **🚀 性能提升**: Arrow记录内存管理优化
  - **🔧 新增组件**: pkg/transformer/ 数据转换引擎包

- **v2.1.0** (2024-09-18)
  - **架构重构**: 采用混合高性能传输架构
  - ClickHouse: 从 Flight SQL 切换到 TCP 连接 + ArrowStream 格式查询
  - StarRocks: 保持 Arrow Flight SQL 协议进行数据导入
  - 实现零拷贝流式传输：ClickHouse ArrowStream → StarRocks Flight SQL
  - 移除 ClickHouse Flight SQL 依赖，简化配置和代码结构
  - 优化性能：原生 Arrow 格式端到端传输
  - 启用 ClickHouse 原生协议版本和 LZ4 压缩
  - 完整的单元测试覆盖和编译验证

- **v2.0.0** (2024-09-17)
  - **重大更新**: 全面采用 Apache Arrow Flight SQL 协议
  - 统一 ClickHouse 数据读取和 StarRocks 数据写入为 Arrow Flight SQL
  - 实现零拷贝列式数据传输，显著提升性能
  - 支持多种压缩算法 (LZ4、ZSTD、GZIP)
  - 基于 gRPC/HTTP2 的高性能网络通信
  - 完整的单元测试覆盖
  - 向后兼容 Stream Load 接口

- **v1.0.0** (2024-01-xx)
  - 初始版本发布
  - 支持基本的数据同步功能
  - 支持配置化的同步任务
  - 支持数据完整性验证

## 📜 许可证

本项目采用 Apache 2.0 许可证。详见 [LICENSE](LICENSE) 文件。

## 🙏 致谢

感谢以下开源项目的支持：

- [Apache Arrow](https://arrow.apache.org/) - 内存中列式数据格式和 Flight SQL 协议
- [ClickHouse](https://clickhouse.com/) - 高性能列式数据库
- [StarRocks](https://www.starrocks.io/) - 高性能分析数据库
- [Prometheus](https://prometheus.io/) - 监控系统
- [Kubernetes](https://kubernetes.io/) - 容器编排平台

---

如果您觉得这个项目有用，请给我们一个 ⭐️！