# 数据转换功能

## 功能概述

ck2sr v2.2.0 新增了灵活的数据转换功能，允许在数据从 ClickHouse 同步到 StarRocks 的过程中进行实时数据转换、类型转换、数据验证和清洗。

## 核心特性

### 1. 字段级别转换
- **类型转换**: 支持不同数据类型之间的转换（如 int64 → int32, string → varchar）
- **表达式转换**: 支持内置表达式如 upper、lower、trim 等
- **数据验证**: 支持正则表达式、数值范围验证
- **默认值**: 为空值或无效值设置默认值
- **必需字段**: 标记必需字段，确保数据完整性

### 2. 全局转换规则
- **统一类型映射**: 对所有字段应用统一的类型转换规则
- **批量处理**: 减少配置重复，提高配置效率

### 3. 数据验证
- **正则表达式验证**: 验证字符串格式（如邮箱、电话号码）
- **数值范围验证**: 确保数值在合理范围内
- **空值控制**: 控制字段是否允许为空

## 配置结构

```yaml
data_transform:
  enabled: true                    # 是否启用数据转换
  field_transforms:               # 字段级转换配置
    - column: "字段名"
      type_conversion:            # 类型转换
        source_type: "源类型"
        target_type: "目标类型"
        expression: "转换表达式"   # 可选
      validation:                 # 数据验证
        pattern: "正则表达式"      # 可选
        min_value: 最小值         # 可选
        max_value: 最大值         # 可选
        allow_null: true/false    # 是否允许空值
      default_value: "默认值"     # 可选
      required: true/false        # 是否必需
  global_rules:                   # 全局转换规则
    - source_type: "源类型"
      target_type: "目标类型"
      expression: "转换表达式"     # 可选
```

## 支持的数据类型

### ClickHouse → Arrow → StarRocks 类型映射

| ClickHouse 类型 | Arrow 类型 | StarRocks 类型 | 说明 |
|----------------|-----------|---------------|------|
| Int8 | Int8 | TINYINT | 8位整数 |
| Int16 | Int16 | SMALLINT | 16位整数 |
| Int32 | Int32 | INT | 32位整数 |
| Int64 | Int64 | BIGINT | 64位整数 |
| Float32 | Float32 | FLOAT | 32位浮点数 |
| Float64 | Float64 | DOUBLE | 64位浮点数 |
| String | String | VARCHAR(65533) | 字符串 |
| DateTime | Timestamp | DATETIME | 时间戳 |
| Date | Date32 | DATE | 日期 |
| Boolean | Boolean | BOOLEAN | 布尔值 |
| Decimal | Decimal | DECIMAL(p,s) | 定点数 |

## 支持的转换表达式

### 字符串转换
- `upper` / `uppercase`: 转换为大写
- `lower` / `lowercase`: 转换为小写
- `trim`: 去除首尾空格

### 格式化转换
- 支持 `sprintf` 风格的格式化字符串，如 `prefix_%s_suffix`

### 自定义表达式
未来版本将支持更复杂的表达式引擎。

## 使用示例

### 1. 基础类型转换

```yaml
data_transform:
  enabled: true
  field_transforms:
    - column: "user_id"
      type_conversion:
        source_type: "int64"
        target_type: "int32"
```

### 2. 字符串处理和验证

```yaml
data_transform:
  enabled: true
  field_transforms:
    - column: "username"
      type_conversion:
        source_type: "string"
        target_type: "varchar"
        expression: "trim"        # 去除空格
      required: true
      validation:
        pattern: "^[a-zA-Z0-9_]+$"  # 只允许字母数字下划线
        allow_null: false
    - column: "email"
      type_conversion:
        source_type: "string"
        target_type: "varchar"
        expression: "lower"       # 转为小写
      validation:
        pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
      required: true
```

### 3. 数值验证和默认值

```yaml
data_transform:
  enabled: true
  field_transforms:
    - column: "age"
      type_conversion:
        source_type: "int64"
        target_type: "int32"
      validation:
        min_value: 0
        max_value: 150
      default_value: 0
    - column: "price"
      type_conversion:
        source_type: "float64"
        target_type: "decimal(15,2)"
      validation:
        min_value: 0.01
        max_value: 999999.99
      default_value: 0.00
```

### 4. 状态字段标准化

```yaml
data_transform:
  enabled: true
  field_transforms:
    - column: "status"
      type_conversion:
        source_type: "string"
        target_type: "varchar"
        expression: "upper"       # 转为大写
      validation:
        pattern: "^(ACTIVE|INACTIVE|PENDING|COMPLETED)$"
      default_value: "PENDING"
```

### 5. 全局转换规则

```yaml
data_transform:
  enabled: true
  global_rules:
    - source_type: "float64"
      target_type: "double"
    - source_type: "int64"
      target_type: "bigint"
    - source_type: "timestamp"
      target_type: "datetime"
  field_transforms:
    # 字段级配置会覆盖全局规则
    - column: "special_id"
      type_conversion:
        source_type: "int64"      # 即使全局规则是bigint
        target_type: "int32"      # 这个字段仍然使用int32
```

## 执行顺序

1. **字段级转换优先**: 如果字段有专门的转换配置，优先使用字段级配置
2. **全局规则**: 如果字段没有专门配置，应用全局转换规则
3. **类型转换**: 执行数据类型转换
4. **表达式转换**: 应用转换表达式（如 upper、lower）
5. **数据验证**: 验证转换后的数据
6. **默认值处理**: 对无效或缺失数据应用默认值

## 错误处理

### 转换失败处理
- **数据类型转换失败**: 记录警告日志，使用 NULL 值或默认值
- **验证失败**:
  - 如果设置了默认值，使用默认值
  - 如果字段为必需且无默认值，返回错误
  - 否则使用 NULL 值

### 日志记录
所有转换过程都会记录详细的日志信息：
```
DEBUG: Applying data transformation to record with 1000 rows
DEBUG: Data transformation completed: 1000 rows processed
WARN: Failed to convert value 'invalid_age' in column age: cannot convert string to int32
```

## 性能影响

### 内存管理
- 使用 Apache Arrow 的内存分配器进行高效内存管理
- 自动管理 Arrow Record 的引用计数和释放
- 支持零拷贝操作以最小化内存占用

### 处理性能
- **启用转换**: 会增加一定的处理时间，但采用高效的 Arrow 列式操作
- **禁用转换**: 数据直接传递，性能影响最小
- **批处理**: 按批次处理数据，提高整体吞吐量

## 最佳实践

### 1. 配置设计
```yaml
# 推荐：先设置全局规则，再配置特殊字段
data_transform:
  enabled: true
  global_rules:
    - source_type: "string"
      target_type: "varchar"
    - source_type: "int64"
      target_type: "bigint"
  field_transforms:
    # 只配置需要特殊处理的字段
    - column: "id"
      type_conversion:
        source_type: "int64"
        target_type: "int32"    # 覆盖全局规则
```

### 2. 验证策略
```yaml
# 关键字段严格验证
- column: "email"
  validation:
    pattern: "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    allow_null: false
  required: true

# 可选字段宽松验证
- column: "phone"
  validation:
    pattern: "^[0-9-+()\\s]*$"
    allow_null: true
  default_value: ""
```

### 3. 表达式使用
```yaml
# 数据清洗
- column: "name"
  type_conversion:
    expression: "trim"          # 去除空格
- column: "email"
  type_conversion:
    expression: "lower"         # 统一小写
- column: "status"
  type_conversion:
    expression: "upper"         # 统一大写
```

## 版本兼容性

- **引入版本**: ck2sr v2.2.0
- **向后兼容**: 完全兼容，`data_transform.enabled: false` 时行为与旧版本相同
- **配置要求**: 可选配置，不添加不影响现有功能

## 故障排除

### 常见问题

1. **转换失败**
   - 检查源类型和目标类型是否兼容
   - 查看日志中的详细错误信息
   - 验证转换表达式是否正确

2. **验证失败**
   - 检查正则表达式是否正确
   - 确认数值范围设置是否合理
   - 检查是否需要设置默认值

3. **性能问题**
   - 考虑简化复杂的验证规则
   - 使用全局规则减少配置复杂度
   - 适当调整批次大小

### 调试技巧

1. **启用详细日志**
   ```yaml
   log:
     level: "debug"
   ```

2. **分步测试**
   - 先测试简单的类型转换
   - 再逐步添加验证和表达式
   - 最后启用复杂的转换逻辑

3. **监控指标**
   - 观察转换成功率
   - 监控处理性能
   - 跟踪错误率变化