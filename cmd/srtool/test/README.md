# srtool Mock单元测试文档

## 概述

本文档描述了srtool工具的Mock单元测试实现，使用Go的testing框架和自定义Mock技术来验证数据生成(generate_data)和数据同步(sync)功能。

## 测试架构

### 核心组件

1. **Mock接口层** (`mock_interfaces.go`)
   - `MockStarRocksClient`: StarRocks客户端Mock接口
   - `MockArrowDataWriter`: Arrow数据写入器Mock
   - `MockArrowStreamWriter`: Arrow流式写入器Mock
   - `MockLogger`: 日志记录器Mock

2. **Mock实现层** (`mock_client.go`)
   - `MockStarRocksClientImpl`: StarRocks客户端的完整Mock实现
   - 支持表信息管理、连接测试、数据查询等功能

3. **测试用例层**
   - `generate_data_test.go`: 数据生成功能测试
   - `sync_test.go`: 数据同步功能测试
   - `boolean_fix_test.go`: 布尔类型修复测试
   - `datetime_fix_test.go`: 日期时间类型修复测试
   - `panic_fix_test.go`: ArrowDataWriter空指针修复测试
   - `benchmark_test.go`: 性能基准测试

## 测试功能覆盖

### 数据生成测试 (generate_data_test.go)

| 测试用例 | 测试场景 | 验证内容 |
|---------|----------|----------|
| 成功生成测试数据 | 正常数据生成流程 | 表创建、数据写入、类型正确性 |
| 连接失败 | 无效主机连接 | 错误处理、无表创建 |
| 表已存在场景 | 目标表已存在 | 复用现有表、数据写入 |
| 小批量数据生成 | 小量数据处理 | 数据完整性、ID递增 |

**测试数据类型覆盖**:
- 整数类型: TINYINT, SMALLINT, INT, BIGINT
- 浮点类型: FLOAT, DOUBLE
- 布尔类型: BOOLEAN (包含tinyint(1)修复)
- 字符串类型: VARCHAR, CHAR, TEXT, JSON
- 日期时间类型: DATE, DATETIME, TIMESTAMP
- 数值类型: DECIMAL

### 数据同步测试 (sync_test.go)

| 测试用例 | 测试场景 | 验证内容 |
|---------|----------|----------|
| 成功同步数据 | 正常同步流程 | 源表读取、目标表创建、数据传输 |
| 源连接失败 | 源端连接异常 | 错误处理、同步中止 |
| 目标连接失败 | 目标端连接异常 | 错误处理、源表信息获取 |
| 源表为空 | 空表同步 | 空数据处理、无错误 |
| 大批量同步 | 大量数据同步 | 批处理、多端点处理 |

**同步流程覆盖**:
1. 连接测试 (源端 + 目标端)
2. 源表信息获取
3. 行数统计
4. Schema转换
5. 目标表自动创建
6. Flight SQL查询执行
7. Arrow数据流处理
8. 数据写入和验证

## Mock设计特点

### 1. 完整的生命周期模拟

```go
// 连接测试
func (m *MockStarRocksClientImpl) TestConnection(ctx context.Context) error

// 表信息管理
func (m *MockStarRocksClientImpl) GetTableInfo(ctx context.Context, tableName string) (*starrocks.TableInfo, error)
func (m *MockStarRocksClientImpl) autoCreateTable(tableName string, schema *arrow.Schema)

// 数据查询和传输
func (m *MockStarRocksClientImpl) ExecuteQuery(ctx context.Context, query string) (*flight.FlightInfo, error)
func (m *MockStarRocksClientImpl) DoGet(ctx context.Context, ticket *flight.Ticket) (flight.FlightService_DoGetClient, error)
```

### 2. 状态跟踪和验证

```go
type MockStarRocksClientImpl struct {
    CreatedTables []string              // 跟踪创建的表
    DataWriters   map[string]*MockArrowDataWriter   // 跟踪数据写入器
    StreamWriters map[string]*MockArrowStreamWriter // 跟踪流式写入器
    CloseCalled   bool                  // 跟踪关闭调用
}
```

### 3. 灵活的错误注入

```go
// 支持各种错误场景
ConnectionError      error  // 连接错误
TableNotFoundError   error  // 表不存在错误
QueryError          error  // 查询错误
CreateTableError    error  // 建表错误
```

### 4. 详细的日志捕获

```go
type MockLogger struct {
    InfoMessages    []string
    WarningMessages []string
    ErrorMessages   []string
    DebugMessages   []string
}
```

## 运行测试

### 命令行运行

```bash
# Linux/Mac
chmod +x run_tests.sh
./run_tests.sh

# Windows
run_tests.bat
```

### 单独运行测试

```bash
# 数据生成测试
go test -v -run TestGenerateDataCommand ./...

# 数据同步测试
go test -v -run TestSyncCommand ./...

# 布尔类型修复测试
go test -v -run TestBoolean ./...

# ArrowDataWriter空指针修复测试
go test -v -run TestArrowDataWriter ./...
go test -v -run TestExtract ./...
go test -v -run TestFlightClient ./...

# 性能基准测试
go test -bench=. -benchmem ./...

# 测试覆盖率
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html
```

## 布尔类型修复功能

### 问题描述
StarRocks在某些版本中会将`BOOLEAN`类型实际存储为`tinyint`或`tinyint(1)`，这会导致以下问题：
- 查询表结构时获取到`tinyint`类型而不是`BOOLEAN`
- 数据类型转换时发生类型不匹配错误
- 布尔值数据无法正确处理

### 修复方案
我们实现了智能的布尔类型识别机制：

**识别规则**:
1. 列名包含`boolean`或`bool`关键字
2. 列名以`_flag`或`_active`结尾
3. 数据类型为`tinyint(1)`

**代码实现**:
```go
// 处理StarRocks中BOOLEAN类型可能被存储为tinyint(1)的情况
if (col.Type == "tinyint" && (strings.Contains(strings.ToLower(col.Name), "boolean") ||
    strings.Contains(strings.ToLower(col.Name), "bool") ||
    strings.HasSuffix(strings.ToLower(col.Name), "_flag") ||
    strings.HasSuffix(strings.ToLower(col.Name), "_active"))) ||
    strings.Contains(col.Type, "tinyint(1)") {
    col.Type = "BOOLEAN"
}
```

**修复效果**:
```
原始类型 -> 修复后类型
boolean_col (tinyint) -> BOOLEAN ✅
active_flag (tinyint) -> BOOLEAN ✅
is_active (tinyint) -> BOOLEAN ✅
status (tinyint(1)) -> BOOLEAN ✅
user_id (tinyint) -> tinyint (保持不变) ✅
```

### 测试验证
- `TestBooleanTypeFixing`: 验证完整的表信息处理流程
- `TestBooleanTypePattern`: 测试各种命名模式的识别准确性

## 日期时间类型修复功能

### 问题描述
在数据写入时出现以下错误：
```
invalid value type for date32: string, error: invalid date format: 2025-09-22 10:07:54
```

**根本原因**: StarRocks类型到Arrow类型转换的优先级问题，`DATETIME`类型被错误匹配为`DATE`类型，导致：
- `timestamp_col (datetime)` 被映射为 `Date32` 而不是 `Timestamp`
- 时间字符串 `"2025-09-22 10:07:54"` 无法转换为只支持日期格式的 `Date32`

### 修复方案
调整类型检查优先级，确保`DATETIME`和`TIMESTAMP`在`DATE`之前检查：

**修复前**:
```go
case strings.Contains(upperType, "DATE"):
    return arrow.FixedWidthTypes.Date32, nil
case strings.Contains(upperType, "DATETIME") || strings.Contains(upperType, "TIMESTAMP"):
    return arrow.FixedWidthTypes.Timestamp_us, nil
```

**修复后**:
```go
// 先检查DATETIME和TIMESTAMP，再检查DATE，避免DATE匹配到DATETIME
case strings.Contains(upperType, "DATETIME") || strings.Contains(upperType, "TIMESTAMP"):
    return arrow.FixedWidthTypes.Timestamp_us, nil
case strings.Contains(upperType, "DATE"):
    return arrow.FixedWidthTypes.Date32, nil
```

### 修复效果
```
原始类型 -> 修复后Arrow类型
date (date) -> Date32 ✅
datetime (datetime) -> Timestamp ✅
timestamp_col (datetime) -> Timestamp ✅ (用户报告的情况)
```

### 数据格式支持
- **Date32**: 支持 `"2006-01-02"` 格式
- **Timestamp**: 支持 `"2006-01-02 15:04:05"` 格式和 `time.Time` 对象

### 测试验证
- `TestDateTimeTypeMapping`: 验证类型映射的正确性
- `TestTimeDataConversion`: 测试数据格式转换的完整性

## ArrowDataWriter空指针修复功能

### 问题描述
在数据写入时出现panic错误：
```
panic: runtime error: invalid memory address or nil pointer dereference [recovered]
[signal SIGSEGV: segmentation violation code=0x1 addr=0x50 pc=0xba09dc]

goroutine 1 [running]:
github.com/ck2sr/ck2sr/pkg/starrocks.(*ArrowDataWriter).WriteRecord(0xc0002905c0, {0xf16c70, 0xc000289a70})
        D:/Claude/ck2sr/pkg/starrocks/client.go:829 +0x15c
```

**根本原因**: `ArrowDataWriter`在`WriteRecord`方法中直接访问`dw.client.flightClient.DoPut(ctx)`，但`flightClient`为`nil`：
- 用户配置中没有提供`FlightSQLEndpoint`，导致`flightClient`未初始化
- 代码注释说明使用MySQL协议写入，但实际使用了未初始化的Flight协议

### 修复方案
实现了智能协议选择机制，支持MySQL协议回退：

**1. 修复前** - 直接使用Flight协议：
```go
func (dw *ArrowDataWriter) WriteRecord(record arrow.Record) error {
    // 直接访问 flightClient，可能为 nil
    flightStream, err := dw.client.flightClient.DoPut(ctx)
    // ... panic!
}
```

**2. 修复后** - 智能协议选择：
```go
func (dw *ArrowDataWriter) WriteRecord(record arrow.Record) error {
    // 检查是否有可用的 Flight 客户端
    if dw.client.flightClient == nil {
        // 使用 MySQL 协议进行数据写入
        return dw.writeRecordViaMySQL(record)
    }
    // 使用 Arrow Flight 协议进行数据写入
    return dw.writeRecordViaFlight(record)
}
```

### 修复特性

**MySQL协议写入** (`writeRecordViaMySQL`):
- 构建批量`INSERT`语句
- 支持所有Arrow数据类型到SQL值的转换
- 自动处理NULL值
- 使用参数化查询防止SQL注入

**Arrow类型转换** (`extractValueFromArrowArray`):
- 支持所有基本数据类型（Boolean, Int8-64, Float32/64, String）
- 自动转换Date32为`"2006-01-02"`格式
- 自动转换Timestamp为`"2006-01-02 15:04:05"`格式
- 未知类型使用字符串格式化

**Flight协议保留** (`writeRecordViaFlight`):
- 保持原有Flight协议功能不变
- 高性能零拷贝数据传输
- 二进制Arrow格式传输

### 修复效果
```
配置场景 -> 使用协议
有FlightSQLEndpoint -> Arrow Flight 协议 ✅
无FlightSQLEndpoint -> MySQL 协议回退 ✅
MySQL模式数据写入 -> 无panic，正常写入 ✅
Flight模式数据写入 -> 保持原有性能 ✅
```

### 测试验证
- `TestArrowDataWriterMySQLFallback`: 验证MySQL协议回退功能
- `TestExtractValueFromArrowArray`: 测试Arrow数组值提取
- `TestFlightClientNilHandling`: 测试Flight客户端为nil的处理
```

## 测试结果文件

| 文件名 | 内容 |
|-------|------|
| `generate_data_test_output.log` | 数据生成测试详细日志 |
| `sync_test_output.log` | 数据同步测试详细日志 |
| `full_test_output.log` | 完整测试套件日志 |
| `benchmark_output.log` | 性能基准测试结果 |
| `coverage.out` | 测试覆盖率数据文件 |
| `coverage.html` | 测试覆盖率HTML报告 |

## 测试验证要点

### 1. 数据生成验证

- ✅ 表自动创建功能
- ✅ 15种数据类型正确生成
- ✅ 批量处理正确性
- ✅ 数据类型精度匹配
- ✅ NULL值处理
- ✅ ID字段递增逻辑

### 2. 数据同步验证

- ✅ 双端连接测试
- ✅ Schema转换正确性
- ✅ Flight SQL查询执行
- ✅ 多端点数据处理
- ✅ Arrow数据流解析
- ✅ 批量同步性能

### 3. 错误处理验证

- ✅ 连接失败场景
- ✅ 表不存在处理
- ✅ 权限错误处理
- ✅ 数据格式错误
- ✅ 网络异常恢复

## Mock技术优势

1. **无依赖测试**: 不需要真实的StarRocks环境
2. **确定性结果**: 测试结果可重现，不受外部环境影响
3. **全面覆盖**: 可以模拟各种异常场景
4. **快速执行**: 无网络IO，测试执行速度快
5. **并行安全**: 多个测试可以并行执行

## 扩展测试

### 添加新测试用例

1. 在对应的测试文件中添加测试用例
2. 设置Mock行为和预期结果
3. 实现验证逻辑
4. 更新测试文档

### Mock功能扩展

1. 在`mock_interfaces.go`中扩展接口定义
2. 在`mock_client.go`中实现具体Mock逻辑
3. 添加状态跟踪和验证方法
4. 更新测试用例使用新功能

## 注意事项

1. **类型适配**: 由于Go的类型系统限制，某些返回具体类型的方法需要通过接口抽象或依赖注入解决
2. **内存管理**: Arrow相关对象需要正确的引用计数管理
3. **上下文处理**: 所有方法都应该正确处理context.Context
4. **并发安全**: Mock对象的状态修改需要考虑并发安全

## 性能基准

性能基准测试提供以下指标：

- **数据生成性能**: 每秒生成的行数
- **数据同步性能**: 每秒同步的行数
- **内存使用情况**: 操作过程中的内存分配
- **CPU使用效率**: 处理效率和资源消耗

通过这些指标可以评估srtool在不同数据量下的性能表现。