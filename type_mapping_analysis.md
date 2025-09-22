# srtool 类型映射兼容性分析

## 完整类型映射流程

### 1. 初始Arrow Schema定义 (createTestTableArrowSchema)
| 字段名 | Arrow类型 | 可空 |
|--------|-----------|------|
| id | Int64 | false |
| tiny_int_col | Int8 | true |
| small_int_col | Int16 | true |
| int_col | Int32 | true |
| big_int_col | Int64 | true |
| float_col | Float32 | true |
| double_col | Float64 | true |
| boolean_col | Boolean | true |
| varchar_col | String | true |
| char_col | String | true |
| text_col | String | true |
| date_col | Date32 | true |
| datetime_col | Timestamp_us | true |
| timestamp_col | Timestamp_us | true |
| json_col | String | true |
| decimal_col | Float64 | true |

### 2. Arrow到StarRocks类型转换 (convertArrowTypeToStarRocksType)
| Arrow类型 | StarRocks类型 |
|-----------|----------------|
| BOOL | BOOLEAN |
| INT8 | TINYINT |
| INT16 | SMALLINT |
| INT32 | INT |
| INT64 | BIGINT |
| FLOAT32 | FLOAT |
| FLOAT64 | DOUBLE |
| STRING/BINARY | VARCHAR(65533) |
| DATE32 | DATE |
| TIMESTAMP | DATETIME |
| DECIMAL128/256 | DECIMAL(27, 9) |

### 3. StarRocks到Arrow类型转换 (convertStarRocksTypeToArrow)
| StarRocks类型 | Arrow类型 |
|---------------|-----------|
| BOOLEAN/BOOL | Boolean |
| TINYINT | Int8 |
| SMALLINT | Int16 |
| INT/INTEGER | Int32 |
| BIGINT | Int64 |
| FLOAT | Float32 |
| DOUBLE | Float64 |
| VARCHAR/CHAR/TEXT | String |
| DATE | Date32 |
| DATETIME/TIMESTAMP | Timestamp_us |

### 4. 数据生成 (generateTestDataRow)
| 字段名 | 生成的Go类型 | 对应Arrow类型 |
|--------|-------------|---------------|
| id | int64 | Int64 ✅ |
| tiny_int_col | int8 | Int8 ✅ |
| small_int_col | int16 | Int16 ✅ |
| int_col | int32 | Int32 ✅ |
| big_int_col | int64 | Int64 ✅ |
| float_col | float32 | Float32 ✅ |
| double_col | float64 | Float64 ✅ |
| boolean_col | bool | Boolean ✅ |
| varchar_col | string | String ✅ |
| char_col | string | String ✅ |
| text_col | string | String ✅ |
| date_col | string (date format) | Date32 ⚠️ |
| datetime_col | string (datetime format) | Timestamp_us ⚠️ |
| timestamp_col | string (datetime format) | Timestamp_us ⚠️ |
| json_col | string | String ✅ |
| decimal_col | float64 | Float64 ✅ |

## 潜在问题分析

### ✅ 问题1修复: Date32Builder和TimestampBuilder支持
- **修复**: 在StarRocks客户端`appendValueToBuilder`方法中添加了Date32Builder和TimestampBuilder的case处理
- **新增**: `convertToDate32`和`convertToTimestamp`类型转换函数
- **支持格式**:
  - Date32: string("YYYY-MM-DD"), time.Time, arrow.Date32
  - Timestamp: string("YYYY-MM-DD HH:MM:SS"), time.Time, int64(微秒), arrow.Timestamp
- **状态**: ✅ 已修复

### ⚠️ 问题2: 日期时间类型的字符串转换
- **问题**: 生成日期时间数据为字符串格式，但Arrow期望原生类型
- **影响字段**: date_col, datetime_col, timestamp_col
- **风险等级**: 中等
- **状态**: ✅ 通过类型转换函数支持字符串格式

### ⚠️ 问题3: Timestamp类型精度不匹配
- **问题**: Arrow使用Timestamp_us（微秒），但创建表时转换为DATETIME（可能丢失精度）
- **影响字段**: datetime_col, timestamp_col
- **风险等级**: 低

### ⚠️ 问题4: DECIMAL类型简化处理
- **问题**: Arrow DECIMAL转换为StarRocks DECIMAL(27, 9)，但数据生成为float64
- **影响字段**: decimal_col
- **风险等级**: 低

## 兼容性评估

### ✅ 完全兼容
- 所有整数类型 (TINYINT, SMALLINT, INT, BIGINT)
- 所有浮点类型 (FLOAT, DOUBLE)
- 布尔类型 (BOOLEAN)
- 字符串类型 (VARCHAR, CHAR, TEXT, JSON)

### ⚠️ 需要注意
- 日期时间类型的字符串格式转换
- DECIMAL精度处理

### 🔧 推荐修复
1. ✅ **已修复**: 添加Date32Builder和TimestampBuilder支持到StarRocks客户端
2. ✅ **已修复**: 实现convertToDate32和convertToTimestamp类型转换函数
3. 🔄 **可选优化**: 优化日期时间数据生成为原生类型（当前字符串格式通过转换函数也能正常工作）
4. 🔄 **可选优化**: 统一DECIMAL类型处理，使用更精确的Decimal128类型

## 修复详情

### 1. StarRocks客户端Date/Time类型支持 (pkg/starrocks/client.go)

**修复位置**: `appendValueToBuilder`方法和类型转换函数
**修复代码**:
```go
// 添加的Builder支持
case *array.Date32Builder:
    if v, err := convertToDate32(value); err == nil {
        builder.Append(v)
    } else {
        return fmt.Errorf("invalid value type for date32: %T, error: %w", value, err)
    }
case *array.TimestampBuilder:
    if v, err := convertToTimestamp(value); err == nil {
        builder.Append(v)
    } else {
        return fmt.Errorf("invalid value type for timestamp: %T, error: %w", value, err)
    }

// 添加的转换函数
func convertToDate32(value interface{}) (arrow.Date32, error)
func convertToTimestamp(value interface{}) (arrow.Timestamp, error)
```

### 2. 支持的输入格式

**Date32类型**:
- `string`: "YYYY-MM-DD" 格式
- `time.Time`: Go标准时间类型
- `arrow.Date32`: 原生Arrow类型

**Timestamp类型**:
- `string`: "YYYY-MM-DD HH:MM:SS" 格式
- `time.Time`: Go标准时间类型
- `int64`: Unix微秒时间戳
- `arrow.Timestamp`: 原生Arrow类型

## 测试状态

- ✅ 编译测试通过
- ✅ 类型转换函数实现完整
- ✅ 支持多种输入格式
- ✅ 错误处理完善
- 🔄 实际数据写入测试待验证（需要StarRocks环境）

## 总结

经过全面分析和修复，srtool工具的类型兼容性问题已基本解决：

1. **重大修复**: 添加了关键的Date32Builder和TimestampBuilder支持，解决了日期时间类型数据写入失败的问题
2. **类型转换**: 实现了灵活的类型转换机制，支持多种输入格式
3. **兼容性**: 保持向后兼容，不影响现有功能
4. **错误处理**: 提供清晰的错误信息，便于问题诊断

**建议**: 在实际StarRocks环境中进行完整测试，验证所有数据类型的正确写入和读取。