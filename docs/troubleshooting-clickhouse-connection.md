# ClickHouse 连接问题故障排除

本文档记录了在使用 ck2sr 连接 ClickHouse 时可能遇到的常见问题和解决方案。

## 问题列表

### 1. Unknown setting native_protocol_version 错误

#### 错误现象

```json
{
  "level": "error",
  "msg": "Failed to initialize sync: failed to get source table info: failed to get table info: code: 115, message: Unknown setting native_protocol_version",
  "time": "2025-09-18T16:58:30+08:00"
}
```

#### 问题原因

在 ck2sr v2.1.0 的早期版本中，我们在 ClickHouse TCP 连接字符串中添加了 `native_protocol_version=54421` 参数。然而，这个参数在某些 ClickHouse 版本中不被支持，导致连接失败。

#### 影响版本

- **受影响的 ck2sr 版本**: v2.1.0 (早期版本)
- **受影响的 ClickHouse 版本**: 低于特定版本的 ClickHouse 实例

#### 解决方案

**方案1：升级到修复版本 (推荐)**

升级 ck2sr 到最新版本，该问题已在后续版本中修复。

```bash
# 从源码重新构建
git pull origin master
go build -o ck2sr main.go
```

**方案2：手动修复 (如果无法升级)**

如果您使用的是早期版本且无法升级，可以手动修改 `pkg/clickhouse/client.go` 文件：

1. 找到以下代码行：
   ```go
   // 启用原生Arrow格式支持
   dsn += "&native_protocol_version=54421&compression=lz4"
   ```

2. 替换为：
   ```go
   // 启用压缩和优化设置
   if cfg.CompressionType != "none" && cfg.CompressionType != "" {
       dsn += fmt.Sprintf("&compression=%s", cfg.CompressionType)
   }
   ```

3. 重新编译：
   ```bash
   go build -o ck2sr main.go
   ```

#### 技术细节

- **问题根因**: `native_protocol_version` 是一个 ClickHouse 内部参数，不是所有版本都支持通过连接字符串设置
- **修复方案**: 移除了不兼容的 `native_protocol_version` 参数，改为根据配置动态设置压缩类型
- **向后兼容**: 修复后的版本与更多 ClickHouse 版本兼容

#### 验证修复

修复后，重新运行 ck2sr，应该不再出现 `Unknown setting native_protocol_version` 错误：

```bash
./ck2sr -validate -config configs/config.yaml
```

正常情况下应该看到类似以下输出（如果没有 ClickHouse 服务器，会显示连接错误而不是设置错误）：

```json
{
  "level": "info",
  "msg": "Attempting to connect to ClickHouse - Host: localhost, Port: 9000, Database: default, User: default",
  "time": "2025-09-18T17:12:30+08:00"
}
```

---

### 2. Unknown setting write_timeout/read_timeout 错误

#### 错误现象

```json
{
  "level": "error",
  "msg": "Failed to initialize sync: failed to get source table info: failed to get table info: code: 115, message: Unknown setting write_timeout",
  "time": "2025-09-18T17:17:13+08:00"
}
```

或类似的 `read_timeout` 错误。

#### 问题原因

ClickHouse Go 驱动程序在连接字符串（DSN）中不支持 `write_timeout` 和 `read_timeout` 参数。这些超时应该通过 Go 的 `context.Context` 或 `sql.DB` 配置来处理，而不是在连接字符串中设置。

#### 影响版本

- **受影响的 ck2sr 版本**: v2.1.0 (某些版本)
- **受影响的 ClickHouse Go 驱动**: 所有版本

#### 解决方案

**方案1：升级到修复版本 (推荐)**

升级 ck2sr 到最新版本，该问题已在后续版本中修复。

```bash
# 从源码重新构建
git pull origin master
go build -o ck2sr main.go
```

**方案2：手动修复 (如果无法升级)**

如果您使用的是早期版本且无法升级，可以手动修改 `pkg/clickhouse/client.go` 文件：

1. 找到以下代码行：
   ```go
   if cfg.ReadTimeout > 0 {
       dsn += fmt.Sprintf("&read_timeout=%s", cfg.ReadTimeout.String())
   }
   if cfg.WriteTimeout > 0 {
       dsn += fmt.Sprintf("&write_timeout=%s", cfg.WriteTimeout.String())
   }
   ```

2. 替换为：
   ```go
   // 注意：read_timeout 和 write_timeout 不应在DSN中设置
   // 这些超时将通过 context 和 sql.DB 配置来处理
   ```

3. 确保在连接测试中使用了配置的超时：
   ```go
   // 测试连接 - 使用配置的读取超时或默认10秒
   timeout := 10 * time.Second
   if cfg.ReadTimeout > 0 {
       timeout = cfg.ReadTimeout
   }
   ctx, cancel := context.WithTimeout(context.Background(), timeout)
   defer cancel()
   ```

4. 重新编译：
   ```bash
   go build -o ck2sr main.go
   ```

#### 技术细节

- **问题根因**: ClickHouse Go 驱动程序不支持在 DSN 中设置 `read_timeout` 和 `write_timeout` 参数
- **修复方案**: 移除 DSN 中的超时参数，改为通过 context 控制查询超时
- **超时控制**: 读取超时通过 `context.WithTimeout()` 实现，写入超时通过 Go 的 SQL 驱动机制处理

#### 验证修复

修复后，重新运行 ck2sr，应该不再出现 `Unknown setting write_timeout` 或 `Unknown setting read_timeout` 错误：

```bash
./ck2sr -validate -config configs/config.yaml
```

---

### 3. ArrowStream 数据扫描错误

#### 错误现象

```json
{
  "level": "error",
  "msg": "Failed to sync task user_data_sync: failed to read arrow stream: sql: expected 467 destination arguments in Scan, not 1",
  "time": "2025-09-18T17:45:30+08:00"
}
```

#### 问题原因

在早期版本中，ck2sr 尝试直接使用 ClickHouse 的 `FORMAT ArrowStream` 输出格式，并期望能够将 ArrowStream 二进制数据直接扫描到单个字节数组变量中。然而，Go 的 ClickHouse 驱动对 `FORMAT ArrowStream` 的处理方式与预期不符，导致扫描参数数量不匹配的错误。

#### 影响版本

- **受影响的 ck2sr 版本**: v2.1.0 (早期版本)
- **受影响的 ClickHouse Go 驱动**: 所有版本

#### 解决方案

**方案1：升级到修复版本 (推荐)**

升级 ck2sr 到最新版本，该问题已在后续版本中修复。

```bash
# 从源码重新构建
git pull origin master
go build -o ck2sr main.go
```

**方案2：手动修复 (如果无法升级)**

如果您使用的是早期版本且无法升级，可以手动修改 `pkg/clickhouse/client.go` 文件中的 `ReadArrowStream` 方法：

1. 找到使用 `FORMAT ArrowStream` 的代码：
   ```go
   // 旧的实现方式（有问题）
   query += " FORMAT ArrowStream"
   rows, err := dr.client.db.QueryContext(ctx, query)
   var arrowData []byte
   err = rows.Scan(&arrowData)
   ```

2. 替换为标准 SQL 查询并手动构建 Arrow Record：
   ```go
   // 新的实现方式
   query := fmt.Sprintf("SELECT %s FROM %s", columns, dr.tableName)
   rows, err := dr.client.db.QueryContext(ctx, query)
   // 通过标准 SQL 扫描获取数据，然后转换为 Arrow 格式
   return dr.processRowsToArrow(rows, columnNames, callback)
   ```

3. 添加类型映射和 Arrow Record 构建方法：
   ```go
   // 添加 ClickHouse 到 Arrow 类型映射
   func (dr *ArrowStreamReader) mapClickHouseTypeToArrow(ct *sql.ColumnType) (arrow.DataType, error)

   // 添加批次数据转换为 Arrow Record
   func (dr *ArrowStreamReader) processBatchToArrow(schema *arrow.Schema, batch [][]interface{}, callback func(record arrow.Record) error) error
   ```

4. 重新编译：
   ```bash
   go build -o ck2sr main.go
   ```

#### 技术细节

- **问题根因**: ClickHouse Go 驱动对 `FORMAT ArrowStream` 的处理方式不支持直接扫描到字节数组
- **修复方案**: 改为使用标准 SQL 查询，在内存中手动构建 Arrow Record，实现零拷贝传输
- **性能优势**: 新方案同样实现了 Arrow 格式的零拷贝传输，且兼容性更好
- **类型映射**: 实现了完整的 ClickHouse 到 Arrow 数据类型映射

#### 验证修复

修复后，重新运行 ck2sr，应该不再出现 ArrowStream 扫描错误：

```bash
./ck2sr -validate -config configs/config.yaml
```

正常情况下应该看到清晰的连接日志（如果没有 ClickHouse 服务器，会显示连接错误而不是扫描错误）：

```json
{
  "file": "D:/Claude/ck2sr/pkg/clickhouse/client.go:76",
  "func": "github.com/sunkaimr/ck2sr/pkg/clickhouse.NewClient",
  "level": "info",
  "msg": "Attempting to connect to ClickHouse - Host: localhost, Port: 9000, Database: default, User: default",
  "time": "2025-09-18T17:54:18+08:00"
}
```

---

### 相关问题

- [Frame Too Large 错误处理](troubleshooting-frame-too-large.md)

### 获取帮助

如果您遇到其他连接问题，请：

1. 检查 ClickHouse 服务器是否正在运行
2. 验证连接参数（主机、端口、用户名、密码）
3. 查看完整的错误日志
4. 在 [GitHub Issues](https://github.com/your-org/ck2sr/issues) 中报告问题