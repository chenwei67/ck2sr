# StarRocks 自动建表功能

## 功能概述

ck2sr v2.1.1 新增了自动建表功能。当数据同步到 StarRocks 时，如果发现目标表不存在，系统将根据源表的 Arrow Schema 自动创建一个合理的 StarRocks 表。

## 主要特性

### 1. 自动创建目的表

### 2. 智能类型映射

系统自动将 Apache Arrow 数据类型映射到对应的 StarRocks 数据类型：

| Arrow 类型 | StarRocks 类型 | 说明 |
|-----------|---------------|------|
| BOOL | BOOLEAN | 布尔类型 |
| INT8 | TINYINT | 8位整数 |
| INT16 | SMALLINT | 16位整数 |
| INT32 | INT | 32位整数 |
| INT64 | BIGINT | 64位整数 |
| UINT8 | SMALLINT | 无符号8位→16位有符号 |
| UINT16 | INT | 无符号16位→32位有符号 |
| UINT32 | BIGINT | 无符号32位→64位有符号 |
| UINT64 | BIGINT | 无符号64位→64位有符号 |
| FLOAT32 | FLOAT | 单精度浮点 |
| FLOAT64 | DOUBLE | 双精度浮点 |
| STRING/BINARY | VARCHAR(65533) | 变长字符串 |
| DATE32 | DATE | 日期类型 |
| TIMESTAMP | DATETIME | 时间戳类型 |
| DECIMAL128/256 | DECIMAL(27, 9) | 高精度小数 |

### 3. 智能主键识别

- 自动识别名为 `id` 或以 `_id` 结尾且不可为空的字段作为主键
- 支持复合主键
- 如果没有主键字段，使用 DUPLICATE KEY 模型

### 4. StarRocks 优化配置

自动创建的表包含以下 StarRocks 优化配置：

```sql
ENGINE=OLAP
DISTRIBUTED BY HASH(`主键字段`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1",
  "storage_format" = "DEFAULT",
  "compression" = "LZ4"
)
```

## 使用方法

### 1. 配置文件设置

在 `sync_tasks` 配置中，可以有以下几种配置方式：

#### 方式1: 使用源表名（推荐）
```yaml
sync_tasks:
  - task_id: "user_sync"
    source_table: "users"
    target_table: "users"  # 系统自动生成 users_ck2sr
```

#### 方式2: 指定完整目标表名
```yaml
sync_tasks:
  - task_id: "user_sync"
    source_table: "users"
    target_table: "users_ck2sr"  # 直接指定完整表名
```

#### 方式3: 自定义目标表名
```yaml
sync_tasks:
  - task_id: "user_sync"
    source_table: "users"
    target_table: "my_users"  # 系统自动生成 my_users_ck2sr
```

### 2. 程序中使用

#### 启用自动建表的 ArrowDataWriter
```go
// 创建支持自动建表的数据写入器
writer, err := starRocksClient.NewArrowDataWriterWithAutoCreate(
    targetTableName,
    batchSize,
    sourceSchema  // 传入源表的 Arrow Schema
)
```

#### 向后兼容的使用方式
```go
// 传统方式（不支持自动建表）
writer, err := starRocksClient.NewArrowDataWriter(targetTableName, batchSize)
```

## 自动建表示例

### 源表 Schema
```
ClickHouse Table: users
- id: Int64 (NOT NULL)
- name: String (NOT NULL)
- email: String (NULL)
- age: Int32 (NULL)
- created_at: DateTime (NOT NULL)
- is_active: Boolean (NOT NULL)
```

### 自动生成的 StarRocks 表
```sql
CREATE TABLE IF NOT EXISTS `users_ck2sr` (
  `id` BIGINT NOT NULL,
  `name` VARCHAR(65533) NOT NULL,
  `email` VARCHAR(65533),
  `age` INT,
  `created_at` DATETIME NOT NULL,
  `is_active` BOOLEAN NOT NULL,
  PRIMARY KEY (`id`)
)
ENGINE=OLAP
DISTRIBUTED BY HASH(`id`) BUCKETS 10
PROPERTIES (
  "replication_num" = "1",
  "storage_format" = "DEFAULT",
  "compression" = "LZ4"
)
```

## 日志输出

启用自动建表功能后，日志中会显示相关信息：

```json
{
  "level": "info",
  "msg": "Table users_ck2sr not found, attempting to create it automatically",
  "time": "2025-09-18T19:30:00+08:00"
}

{
  "level": "info",
  "msg": "Creating table with SQL: CREATE TABLE IF NOT EXISTS `users_ck2sr` ...",
  "time": "2025-09-18T19:30:01+08:00"
}

{
  "level": "info",
  "msg": "Successfully auto-created table: users_ck2sr",
  "time": "2025-09-18T19:30:02+08:00"
}
```

## 错误处理

### 常见错误及解决方案

1. **权限不足**
   ```
   Error: failed to execute CREATE TABLE: Access denied
   ```
   解决：确保 StarRocks 用户具有 CREATE TABLE 权限

2. **表名冲突**
   ```
   Error: Table 'users_ck2sr' already exists
   ```
   解决：检查表是否已存在，或配置不同的目标表名

3. **不支持的数据类型**
   ```
   Warning: Failed to convert field xxx type yyy, using VARCHAR(255)
   ```
   解决：系统会自动降级为 VARCHAR 类型，通常不影响使用

## 最佳实践

1. **测试环境验证**: 在生产环境使用前，先在测试环境验证自动生成的表结构
2. **权限配置**: 确保 StarRocks 连接用户具有足够的 CREATE TABLE 权限
3. **监控日志**: 关注自动建表的日志输出，及时发现问题
4. **表结构调优**: 根据数据特点，可能需要手动调整自动生成的表结构

## 兼容性

- **ck2sr 版本**: v2.1.1+
- **StarRocks 版本**: 4.0+
- **ClickHouse 版本**: 21.12+（支持 ArrowStream 格式）

## 相关配置

参考 [配置文档](../README.md#配置说明) 了解完整的同步任务配置选项。