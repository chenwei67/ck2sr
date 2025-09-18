# Frame Too Large 错误排除指南

## 错误描述
```
rpc error: code = Unavailable desc = connection error: desc = "error reading server preface: http2: frame too large"
```

## 解决步骤

### 1. 立即修复：使用小批次配置

复制 `config-small-batch.yaml` 配置：

```yaml
# 临时解决方案：极小批次
clickhouse:
  batch_size: 500           # 从默认 10000 降低到 500
  max_message_size: 10      # 从默认 100MB 降低到 10MB

starrocks:
  batch_size: 500           # 从默认 10000 降低到 500
  max_message_size: 10      # 从默认 100MB 降低到 10MB
```

### 2. 逐步调优

逐步增加批次大小，找到最佳配置：

```yaml
# 测试步骤1: 极小批次
batch_size: 500
max_message_size: 10

# 测试步骤2: 小批次
batch_size: 1000
max_message_size: 20

# 测试步骤3: 中等批次
batch_size: 2000
max_message_size: 40

# 测试步骤4: 较大批次
batch_size: 5000
max_message_size: 80
```

### 3. 检查服务器端配置

确保 ClickHouse/StarRocks 的 Flight SQL 服务也配置了适当的消息大小限制：

```sql
-- ClickHouse 检查 Flight SQL 配置
SELECT * FROM system.server_settings WHERE name LIKE '%flight%';

-- StarRocks 检查配置
SHOW VARIABLES LIKE '%grpc%';
SHOW VARIABLES LIKE '%flight%';
```

### 4. 网络环境检查

- 检查是否有代理或负载均衡器限制
- 测试直连是否工作正常
- 确认网络带宽和延迟

### 5. 数据类型优化

某些数据类型可能导致更大的序列化大小：

```yaml
# 对于包含大文本字段的表，使用更小的批次
tasks:
  - name: "large_text_table"
    source_table: "articles"  # 包含大文本字段
    target_table: "articles"
    concurrency:
      batch_size: 100         # 非常小的批次
```

### 6. 日志监控

启用详细日志查看具体错误：

```yaml
log:
  level: "debug"              # 启用调试日志
  format: "json"
```

查看日志中的提示信息：
- "Frame too large error detected. Consider reducing batch_size..."
- "Flight SQL timeout: ..., batch size: ..."

## 成功示例配置

### 小型环境
```yaml
batch_size: 1000
max_message_size: 50
max_workers: 5
```

### 中型环境
```yaml
batch_size: 5000
max_message_size: 200
max_workers: 10
```

### 大型环境
```yaml
batch_size: 20000
max_message_size: 1000
max_workers: 20
```

## 注意事项

1. **批次大小与消息大小的关系**：
   - 批次大小越大，单个消息可能越大
   - 文本字段多的表需要更小的批次

2. **压缩的影响**：
   - `compression_type: "lz4"` 可以减少传输大小
   - 但解压缩也需要额外的处理时间

3. **超时设置**：
   - 小批次可以使用较短的超时时间
   - 大批次需要更长的超时时间

## 联系支持

如果问题仍然存在，请提供：
- 完整的错误日志
- 当前配置文件
- 数据表结构信息
- 网络环境描述