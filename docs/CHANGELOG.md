# ck2sr 更新日志

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