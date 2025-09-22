# ck2sr 更新日志

## v2.3.4 - srtool工具重大增强

### 🎉 新功能重构

#### 1. 子命令架构
- **全新设计**: 重构为子命令架构，支持多种操作模式
- **命令结构**:
  - `srtool sync`: 数据同步操作
  - `srtool generate_data`: 测试数据生成
  - `srtool help`: 帮助信息

#### 2. 增强的数据同步功能
- **独立源和目标配置**: 支持不同的StarRocks实例之间的同步
- **命令语法**:
```bash
srtool sync --sql_endpoint "10.192.31.3" --sql_port 9408 \
    --sql_auth_username "root" --sql_auth_password "StarRocks!@2025#." \
    --db hcy --table a \
    --dst_sql_endpoint "10.192.31.3" --dst_sql_port 9408 \
    --dst_sql_auth_username "root" --dst_sql_auth_password "StarRocks!@2025#." \
    --dst_db hcy --dst_table b
```

#### 3. 完整的测试数据生成
- **数据类型覆盖**: 支持StarRocks所有基本数据类型的测试数据生成
- **包含数据类型**:
  - **整数类型**: TINYINT, SMALLINT, INT, BIGINT
  - **浮点类型**: FLOAT, DOUBLE
  - **布尔类型**: BOOLEAN
  - **字符串类型**: VARCHAR, CHAR, TEXT
  - **日期时间类型**: DATE, DATETIME, TIMESTAMP
  - **特殊类型**: JSON, DECIMAL
- **命令语法**:
```bash
srtool generate_data --sql_endpoint "10.192.31.3" --sql_port 9408 \
    --sql_auth_username "root" --sql_auth_password "StarRocks!@2025#." \
    --db testdb --table test_table --row_count 100000
```

### 🚀 技术特点

#### 1. 自动表创建
- **同步操作**: 目标表不存在时自动根据源表架构创建
- **数据生成**: 自动创建包含所有数据类型的综合测试表
- **架构转换**: 智能的StarRocks到Arrow Schema转换

#### 2. 数据生成算法
- **随机数据**: 使用时间种子的高质量随机数据生成
- **NULL值处理**: 10%概率生成NULL值，模拟真实数据
- **批量处理**: 10,000行批次处理，优化内存使用
- **数据范围**: 合理的数据范围和格式（如日期范围2020-2025）

#### 3. 数据类型映射
```go
// StarRocks -> Arrow类型映射
TINYINT    -> Int8
SMALLINT   -> Int16
INT        -> Int32
BIGINT     -> Int64
FLOAT      -> Float32
DOUBLE     -> Float64
BOOLEAN    -> Boolean
VARCHAR/CHAR/TEXT -> String
DATE       -> Date32
DATETIME/TIMESTAMP -> Timestamp_us
DECIMAL    -> Float64
JSON       -> String
```

### 📋 参数说明

#### sync子命令参数
- **源配置**:
  - `--sql_endpoint`: 源StarRocks Flight SQL端点
  - `--sql_port`: 源StarRocks Flight SQL端口 (默认9408)
  - `--sql_auth_username`: 源认证用户名
  - `--sql_auth_password`: 源认证密码
  - `--db`: 源数据库名
  - `--table`: 源表名

- **目标配置**:
  - `--dst_sql_endpoint`: 目标StarRocks Flight SQL端点
  - `--dst_sql_port`: 目标StarRocks Flight SQL端口 (默认9408)
  - `--dst_sql_auth_username`: 目标认证用户名
  - `--dst_sql_auth_password`: 目标认证密码
  - `--dst_db`: 目标数据库名
  - `--dst_table`: 目标表名

- **同步配置**:
  - `--batch_size`: 批次大小 (默认1000)
  - `--verbose`: 启用详细日志

#### generate_data子命令参数
- `--sql_endpoint`: StarRocks Flight SQL端点
- `--sql_port`: StarRocks Flight SQL端口 (默认9408)
- `--sql_auth_username`: 认证用户名
- `--sql_auth_password`: 认证密码
- `--db`: 数据库名 (默认"testdb")
- `--table`: 表名 (默认"test_table")
- `--row_count`: 生成行数 (默认100000)
- `--verbose`: 启用详细日志

### 🔧 技术改进

#### 1. Arrow数据生成优化
- **内存管理**: 正确的Arrow Builder生命周期管理
- **数据完整性**: 完整的Arrow Record构建和释放
- **类型安全**: 严格的类型转换和验证

#### 2. 错误处理增强
- **参数验证**: 完整的必需参数验证
- **连接测试**: 操作前的连接可用性验证
- **数据验证**: 同步后的数据完整性检查

#### 3. 日志系统
- **分级日志**: INFO/DEBUG日志级别控制
- **详细统计**: 同步行数、字节数、耗时统计
- **进度跟踪**: 批次处理进度显示

### 📚 使用场景

#### 1. 开发测试
```bash
# 生成测试数据
srtool generate_data --sql_endpoint "dev.starrocks.com" --sql_port 9408 \
    --sql_auth_username "dev_user" --sql_auth_password "dev_pass" \
    --db testdb --table comprehensive_test --row_count 50000

# 同步测试数据到另一个环境
srtool sync --sql_endpoint "dev.starrocks.com" --sql_port 9408 \
    --sql_auth_username "dev_user" --sql_auth_password "dev_pass" \
    --db testdb --table comprehensive_test \
    --dst_sql_endpoint "staging.starrocks.com" --dst_sql_port 9408 \
    --dst_sql_auth_username "stage_user" --dst_sql_auth_password "stage_pass" \
    --dst_db stagingdb --dst_table test_copy
```

#### 2. 数据迁移
```bash
# 跨环境数据同步
srtool sync --sql_endpoint "prod.starrocks.com" --sql_port 9408 \
    --sql_auth_username "prod_reader" --sql_auth_password "prod_pass" \
    --db production --table user_data \
    --dst_sql_endpoint "backup.starrocks.com" --dst_sql_port 9408 \
    --dst_sql_auth_username "backup_writer" --dst_sql_auth_password "backup_pass" \
    --dst_db backup --dst_table user_data_backup \
    --batch_size 5000 --verbose
```

#### 3. 性能测试
```bash
# 生成大量测试数据进行性能测试
srtool generate_data --sql_endpoint "perf.starrocks.com" --sql_port 9408 \
    --sql_auth_username "perf_user" --sql_auth_password "perf_pass" \
    --db perftest --table large_dataset --row_count 10000000 --verbose
```

### ⚠️ 使用限制和注意事项

#### 1. 权限要求
- **源数据库**: 需要对源表的SELECT权限
- **目标数据库**: 需要CREATE TABLE和INSERT权限
- **Flight SQL**: 确保Flight SQL端口可访问且认证正确

#### 2. 数据类型限制
- **DECIMAL精度**: 当前简化为Float64处理
- **复杂类型**: ARRAY、MAP等复杂类型暂不支持
- **大字段**: 超大TEXT/BLOB字段可能需要调整批次大小

#### 3. 性能考虑
- **网络带宽**: 大数据量同步受网络带宽限制
- **内存使用**: 批次大小影响内存使用，建议根据可用内存调整
- **并发限制**: 当前为单线程处理，大表同步较慢

### 🔄 向后兼容性

- **完全不兼容**: v2.3.3的单一命令模式不再支持
- **迁移指南**:
  - 原`srtool`参数 -> `srtool sync`参数
  - `--flight_sql_endpoint` -> `--sql_endpoint`
  - `--src_table` -> `--table`
  - `--target_table` -> `--dst_table`

### 🎯 测试状态

- ✅ 编译测试通过
- ✅ 子命令结构验证
- ✅ 参数解析和验证
- ✅ 帮助信息显示正确
- ✅ 数据类型覆盖完整
- ✅ Arrow数据生成测试

### 📖 后续计划

1. **性能优化**:
   - 并行数据处理
   - 流水线数据传输
   - 压缩传输支持

2. **功能扩展**:
   - 数据过滤条件支持
   - 增量同步功能
   - 配置文件支持

3. **监控增强**:
   - 实时进度条
   - 性能指标监控
   - 错误恢复机制

### 📋 相关文件

- `cmd/srtool/main.go`: 完全重写的srtool主程序
- `pkg/starrocks/client.go`: StarRocks客户端（增强Flight SQL支持）

---

## v2.3.3 - 新增srtool调试工具

### 🎉 新功能

#### 1. srtool - StarRocks到StarRocks同步调试工具
- **功能**: 专门用于StarRocks表之间的Arrow Flight SQL同步测试的快速调试工具
- **技术特点**:
  - 使用原生Arrow Flight SQL协议进行StarRocks到StarRocks的数据同步
  - 支持零拷贝数据传输，直接通过Arrow IPC格式传输数据
  - 自动创建目标表，支持架构复制
  - 提供详细的同步统计信息和性能指标
  - 支持批量数据传输配置

#### 2. 命令行接口
```bash
srtool --flight_sql_endpoint "10.192.31.3" \
       --flight_sql_port 9408 \
       --flight_sql_auth_username "root" \
       --flight_sql_auth_password "StarRocks!@2025#." \
       --src_table source_table \
       --target_table target_table \
       --batch_size 1000 \
       --verbose
```

#### 3. 参数说明
- `--flight_sql_endpoint`: StarRocks Flight SQL服务端点（必需）
- `--flight_sql_port`: Flight SQL端口（默认9408）
- `--flight_sql_auth_username`: Flight SQL认证用户名（必需）
- `--flight_sql_auth_password`: Flight SQL认证密码（必需）
- `--src_table`: 源表名（必需）
- `--target_table`: 目标表名（必需）
- `--batch_size`: 批次大小（默认1000）
- `--verbose`: 启用详细日志

### 🚀 技术改进

#### 1. StarRocks客户端增强
- **新增**: `ExecuteQuery` 方法 - 执行Flight SQL查询并返回FlightInfo
- **新增**: `DoGet` 方法 - 从指定ticket获取Arrow数据流
- **优化**: 完善了Arrow Flight SQL的查询和数据获取功能

#### 2. 数据传输流程
1. **连接测试**: 验证源和目标StarRocks连接
2. **表信息获取**: 获取源表结构和数据统计
3. **Flight SQL查询**: 执行SELECT查询获取源数据
4. **Arrow流传输**: 直接传输Arrow记录到目标表
5. **结果验证**: 统计传输结果和性能指标

#### 3. 错误处理优化
- 详细的连接失败提示
- Flight SQL查询错误处理
- Arrow数据传输异常恢复
- 完整的统计信息输出

### 📚 使用场景

#### 1. 开发测试
- StarRocks环境的Flight SQL功能验证
- 表结构和数据同步测试
- 性能基准测试

#### 2. 故障诊断
- Flight SQL连接问题排查
- Arrow数据传输问题调试
- 同步性能分析

#### 3. 批量数据迁移
- 小规模数据表的快速同步
- 数据架构验证
- 同步策略测试

### ⚠️ 使用限制

#### 1. 环境要求
- **StarRocks版本**: 建议2.5+版本以获得最佳Flight SQL支持
- **网络要求**: 源和目标StarRocks需要支持Flight SQL端口访问
- **权限要求**: 需要对源表有SELECT权限，对目标表有CREATE/INSERT权限

#### 2. 数据限制
- 当前版本主要用于调试和小规模同步
- 对于大规模数据同步，建议使用完整的ck2sr工具
- 不支持复杂的数据转换和验证（使用基本传输）

### 🔄 向后兼容性

- **完全兼容**: srtool是独立工具，不影响现有ck2sr功能
- **配置独立**: 使用命令行参数，不依赖配置文件
- **库兼容**: 复用现有StarRocks客户端库，保持一致性

### 🎯 测试状态

- ✅ 编译测试通过
- ✅ 命令行参数解析验证
- ✅ 帮助信息显示正确
- ✅ 参数验证逻辑完整
- ✅ StarRocks客户端增强功能测试

### 📋 后续计划

1. **功能增强**:
   - 支持WHERE条件过滤
   - 添加数据转换支持
   - 支持分区表同步

2. **性能优化**:
   - 并行端点处理
   - 自适应批次大小
   - 压缩传输支持

3. **监控改进**:
   - 实时进度显示
   - 详细性能指标
   - 错误统计分析

### 📖 相关文档

- `cmd/srtool/main.go`: srtool主程序
- `pkg/starrocks/client.go`: 增强的StarRocks客户端
- 使用示例详见工具帮助信息

---

## v2.3.2 - ClickHouse ArrowStream 空数据问题修复

### 🐛 BUG修复

#### 1. ClickHouse ArrowStream 8字节空数据问题
- **问题**: ClickHouse 查询返回多个 8 字节无效数据块，导致 0 条记录被处理
- **根因**:
  - 查询条件匹配不到数据（如使用未来时间）
  - ClickHouse 版本对 ArrowStream 支持不完整
  - 返回空结果时的数据格式问题
- **修复**:
  - 新增查询前数据存在性验证
  - 增加 ClickHouse ArrowStream 格式支持检查
  - 智能识别 8 字节空结果和错误标识
  - 提供详细的调试信息和错误提示

#### 2. 数据验证增强
- **新增**: `validateQueryHasData()` 查询前数据验证
- **新增**: `validateArrowStreamSupport()` ArrowStream 格式支持检查
- **新增**: `isClickHouseEmptyResult()` 空结果识别
- **新增**: `isClickHouseError()` 错误标识检测
- **优化**: 增强的数据块内容调试信息

#### 3. 错误提示改进
- 明确提示数据不存在的原因
- 提供 ClickHouse 版本升级建议
- 详细的配置修正指导
- 十六进制数据内容调试输出

### 🚀 用户体验改进

#### 1. 早期错误发现
- 在执行 ArrowStream 查询前验证数据存在性
- 避免处理大量无效的 8 字节数据块
- 提供清晰的错误信息和解决建议

#### 2. 配置验证
- 自动检测常见的配置错误（如未来时间）
- 提供具体的修正建议
- 智能识别 ClickHouse 版本兼容性问题

### 📚 文档更新

#### 1. 新增BUG修复文档
- `docs/bug-fixes/arrowstream-empty-data-fix.md`: 详细的问题分析和修复说明
- 包含配置修正示例和验证方法
- 提供完整的故障排除指导

#### 2. 常见问题解决
- 时间范围配置错误的识别和修正
- ClickHouse 版本兼容性检查
- ArrowStream 格式支持验证方法

### ⚠️ 重要提示

#### 1. 配置检查
如果遇到类似问题，请检查以下配置：

```yaml
# ❌ 错误：使用未来时间
data_range:
  time_column: "recordTime"
  start_time: "2025-03-04 23:59:59"  # 未来时间导致无数据

# ✅ 正确：使用历史时间
data_range:
  time_column: "recordTime"
  start_time: "2023-01-01 00:00:00"  # 确保有数据的时间范围
```

#### 2. ClickHouse 版本要求
- **最低版本**: ClickHouse 21.12+
- **推荐版本**: ClickHouse 22.x 或更高版本
- **验证命令**: `SELECT version();`

#### 3. 调试方法
```yaml
log:
  level: "debug"  # 启用详细调试信息查看具体问题
```

### 🔄 向后兼容性

- **配置兼容**: 完全兼容现有配置文件
- **行为改进**: 增加验证步骤但不改变核心逻辑
- **错误处理**: 提供更友好的错误信息

### 🎯 测试状态

- ✅ 编译测试通过
- ✅ 数据验证逻辑测试
- ✅ 空数据场景处理验证
- ✅ ClickHouse 版本兼容性检查

### 📋 相关修复

此修复与以下问题相关：
- [v2.3.1 ArrowStream EOF 错误修复](arrowstream-eof-fix.md)
- ClickHouse 连接和兼容性问题
- 数据查询范围配置问题

---

## v2.3.1 - ClickHouse ArrowStream 错误处理增强

### 🐛 BUG修复

#### 1. ClickHouse ArrowStream EOF 错误修复
- **问题**: 在实际测试中出现 "unexpected EOF" 错误导致批处理失败
- **根因**: ClickHouse 返回的 ArrowStream 数据块可能不完整或损坏
- **修复**:
  - 增强 Arrow IPC 数据验证逻辑
  - 实现数据恢复策略（头部/尾部字节修复）
  - 智能跳过有问题的数据块
  - 增强错误处理和重试机制

#### 2. 错误处理优化
- **新增**: `processArrowDataBlockWithRetry` 带重试的数据块处理
- **新增**: `shouldSkipBlock` 智能错误跳过判断
- **新增**: `isValidArrowData` Arrow IPC 格式验证
- **优化**: 同步工作器的错误容错能力

#### 3. 相关代码文件
- `pkg/clickhouse/client.go`: 核心修复逻辑
- `internal/worker/sync_worker.go`: 错误处理增强
- `docs/bug-fixes/arrowstream-eof-fix.md`: 详细修复文档

### 🚀 性能改进

#### 1. 错误恢复策略
- 自动尝试跳过损坏的数据头部/尾部字节
- 减少因单个损坏块导致的整体同步失败
- 提高数据同步的可靠性和成功率

#### 2. 智能重试机制
- 区分可重试和不可重试错误
- 避免无效重试，提升整体性能
- 详细的错误日志便于问题诊断

### 📚 文档更新

#### 1. BUG修复文档
- 新增详细的问题分析和修复说明
- 包含配置建议和验证方法
- 提供性能影响评估和测试指南

#### 2. 配置建议
- 针对问题环境的配置优化建议
- 批次大小调整指导
- 日志级别和超时配置建议

### ⚠️ 使用建议

#### 1. 配置优化
```yaml
clickhouse:
  batch_size: 100  # 如遇到频繁 EOF 错误，可降低批次大小
  read_timeout: "60s"  # 增加读取超时
log:
  level: "debug"  # 启用详细日志便于问题排查
```

#### 2. 监控要点
- 观察 "Skipping problematic Arrow block" 日志频率
- 监控数据同步成功率和完整性
- 关注 "Successfully recovered Arrow data" 恢复成功消息

### 🔄 向后兼容性

- **配置兼容**: 完全兼容现有配置文件
- **API兼容**: 所有接口保持不变
- **行为兼容**: 正常数据处理流程无变化

### 🎯 测试状态

- ✅ 编译测试通过
- ✅ 单元测试通过
- ✅ 错误处理逻辑验证
- ✅ 恢复策略测试

---

## v2.2.0 - 数据转换与目标表优化

### 🎉 新功能

#### 1. 数据转换引擎
- **字段级数据转换**: 支持对特定字段进行类型转换、表达式转换和验证
- **全局转换规则**: 统一的类型映射规则，减少配置重复
- **表达式支持**: 内置 `upper`、`lower`、`trim` 等常用转换表达式
- **数据验证**:
  - 正则表达式模式验证
  - 数值范围验证 (min_value, max_value)
  - 空值控制 (allow_null)
- **默认值处理**: 为缺失或无效数据设置默认值
- **必需字段检查**: 确保关键字段的数据完整性

#### 2. 配置增强
```yaml
data_transform:
  enabled: true
  field_transforms:
    - column: "username"
      type_conversion:
        source_type: "string"
        target_type: "varchar"
        expression: "trim"
      validation:
        pattern: "^[a-zA-Z0-9_]+$"
      required: true
  global_rules:
    - source_type: "float64"
      target_type: "double"
```

### 🐛 BUG修复

#### 1. 目标表命名逻辑优化
- **问题**: 之前版本自动为目标表添加 "_ck2sr" 后缀
- **修复**: 直接使用配置文件中的 `target_table` 字段，不再自动添加后缀
- **影响**: 提供更灵活的表名控制，避免表名冲突

**修复前**:
```yaml
target_table: "users"           # 实际创建表名: users_ck2sr
```

**修复后**:
```yaml
target_table: "users_sync"      # 实际创建表名: users_sync
```

#### 2. 相关代码优化
- 移除了 `generateTargetTableName()` 函数
- 简化了 `writeArrowToTarget()` 方法的表名处理逻辑
- 更新了自动建表功能以使用正确的表名

### 🚀 性能提升

#### 1. Arrow记录内存管理优化
- 改进了Arrow Record的引用计数管理
- 添加了适当的 `Retain()` 和 `Release()` 调用
- 避免了内存泄漏和重复释放问题

#### 2. 数据转换性能
- 使用高效的Apache Arrow列式操作
- 零拷贝数据传输（当未启用转换时）
- 批量处理提高整体吞吐量

### 🔧 技术改进

#### 1. 新增组件
- `pkg/transformer/`: 数据转换引擎包
  - `DataTransformer`: 核心转换器
  - 支持复杂的类型映射和验证逻辑
  - 高效的Arrow Record处理

#### 2. 配置结构扩展
- `internal/config/types.go`: 新增数据转换相关配置结构
  - `TypeConversionRule`: 类型转换规则
  - `FieldTransformConfig`: 字段转换配置
  - `ValidationRule`: 验证规则
  - `DataTransformConfig`: 数据转换总配置

#### 3. 集成优化
- `internal/worker/sync_worker.go`: 集成数据转换器
  - 在数据写入前自动应用转换
  - 支持转换开关控制
  - 完善的错误处理和日志记录

### 📚 文档更新

#### 1. 新增文档
- `docs/data-transformation.md`: 数据转换功能完整说明
- 包含详细的配置示例和最佳实践
- 故障排除指南

#### 2. 配置示例更新
- `configs/config.yaml`: 添加数据转换配置示例
- 展示了字段级和全局转换规则的使用方法
- 包含实际使用场景的配置示例

### ⚠️ 破坏性变更

#### 1. 目标表命名变更
- **影响**: 现有配置中的 `target_table` 将直接作为目标表名使用
- **迁移**: 如果之前依赖自动添加的 "_ck2sr" 后缀，需要在配置中显式添加

**迁移示例**:
```yaml
# 旧配置 (v2.1.x 及之前)
target_table: "users"           # 实际表名会是 users_ck2sr

# 新配置 (v2.2.0+)
target_table: "users_ck2sr"     # 需要显式指定完整表名
```

### 🔄 向后兼容性

- **数据转换**: 默认禁用 (`enabled: false`)，不影响现有同步任务
- **配置结构**: 所有新配置项都是可选的
- **API接口**: 保持完全兼容

### 🎯 使用建议

#### 1. 数据转换功能
```yaml
# 推荐的渐进式启用方式
data_transform:
  enabled: true
  # 先设置简单的全局规则
  global_rules:
    - source_type: "string"
      target_type: "varchar"
  # 再为特殊字段添加专门配置
  field_transforms:
    - column: "important_field"
      validation:
        allow_null: false
      required: true
```

#### 2. 目标表命名
```yaml
# 推荐使用描述性的表名
target_table: "users_from_clickhouse"    # 清晰表明数据来源
target_table: "orders_daily_sync"        # 表明同步频率
target_table: "products_replica"         # 表明表的用途
```

### 🐞 已知问题

无已知问题。

### 📋 下个版本计划

- 更多转换表达式支持
- 图形化配置界面
- 数据转换性能监控指标
- 支持自定义转换函数

---

## v2.1.2 - 日志格式控制

### 新功能
- 添加命令行选项控制日志中的 file 和 func 字段显示
- `-log-with-file`: 控制文件名和行号显示
- `-log-with-func`: 控制函数名显示

### BUG修复
- 修复 ClickHouse 连接参数兼容性问题
- 移除不兼容的 `native_protocol_version` 参数
- 移除不兼容的 `read_timeout`、`write_timeout` 参数

### 技术改进
- 重写 ArrowStream 读取器，提高兼容性
- 添加自定义日志格式化器
- 改进错误处理和日志记录

详细信息请参考 `docs/log-format-control.md`