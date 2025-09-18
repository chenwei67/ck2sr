# 日志输出格式控制

## 功能概述

ck2sr v2.1.2 新增了灵活的日志输出格式控制功能，允许通过命令行选项控制日志中的 `file` 和 `func` 字段的显示，让日志输出更加灵活和简洁。

## 命令行选项

### 新增参数

- `-log-with-file=<boolean>`: 控制是否在日志中包含文件名和行号 (默认: true)
- `-log-with-func=<boolean>`: 控制是否在日志中包含函数名 (默认: true)

### 参数说明

- **`-log-with-file`**: 控制 `file` 字段的显示
  - `true` (默认): 显示文件路径和行号，如 `"file":"D:/Claude/ck2sr/main.go:195"`
  - `false`: 不显示文件信息

- **`-log-with-func`**: 控制 `func` 字段的显示
  - `true` (默认): 显示函数名，如 `"func":"main.NewApplication"`
  - `false`: 不显示函数信息

## 使用示例

### 1. 默认输出（包含文件名和函数名）

```bash
./ck2sr -validate -config config.yaml
```

**JSON 格式输出**:
```json
{
  "file": "D:/Claude/ck2sr/main.go:195",
  "func": "main.NewApplication",
  "level": "info",
  "msg": "Starting ck2sr v1.0.0",
  "time": "2025-09-18T19:46:07+08:00"
}
```

**文本格式输出**:
```
2025-09-18T19:47:23+08:00 [INFO] (D:/Claude/ck2sr/main.go:195 main.NewApplication) Starting ck2sr v1.0.0
```

### 2. 只显示文件信息，不显示函数名

```bash
./ck2sr -validate -config config.yaml -log-with-func=false
```

**JSON 格式输出**:
```json
{
  "file": "D:/Claude/ck2sr/main.go:195",
  "level": "info",
  "msg": "Starting ck2sr v1.0.0",
  "time": "2025-09-18T19:46:24+08:00"
}
```

**文本格式输出**:
```
2025-09-18T19:47:31+08:00 [INFO] (D:/Claude/ck2sr/main.go:195) Starting ck2sr v1.0.0
```

### 3. 只显示函数名，不显示文件信息

```bash
./ck2sr -validate -config config.yaml -log-with-file=false
```

**JSON 格式输出**:
```json
{
  "func": "main.NewApplication",
  "level": "info",
  "msg": "Starting ck2sr v1.0.0",
  "time": "2025-09-18T19:46:15+08:00"
}
```

**文本格式输出**:
```
2025-09-18T19:47:31+08:00 [INFO] (main.NewApplication) Starting ck2sr v1.0.0
```

### 4. 简洁输出（不显示文件和函数信息）

```bash
./ck2sr -validate -config config.yaml -log-with-file=false -log-with-func=false
```

**JSON 格式输出**:
```json
{
  "level": "info",
  "msg": "Starting ck2sr v1.0.0",
  "time": "2025-09-18T19:46:32+08:00"
}
```

**文本格式输出**:
```
2025-09-18T19:47:39+08:00 [INFO] Starting ck2sr v1.0.0
```

## 配置优先级

命令行参数会覆盖配置文件中的设置：

1. **命令行参数** (最高优先级)
2. **配置文件设置**
3. **默认值**

## 使用场景

### 开发环境
```bash
# 开发调试时显示完整信息
./ck2sr -config dev-config.yaml
```

### 生产环境
```bash
# 生产环境简化输出，减少日志大小
./ck2sr -config prod-config.yaml -log-with-file=false -log-with-func=false
```

### 日志分析
```bash
# 保留文件信息便于定位问题，但去掉函数名减少噪音
./ck2sr -config config.yaml -log-with-func=false
```

### 容器环境
```bash
# 容器中运行时简化输出
docker run ck2sr:latest -log-with-file=false -log-with-func=false
```

## 性能影响

- **启用 file/func**: 略微增加日志输出时间和存储空间
- **禁用 file/func**: 提升日志性能，减少存储空间，特别适合高频日志场景

## 技术实现

### 自定义格式化器

系统使用自定义的日志格式化器：
- `CustomJSONFormatter`: 处理 JSON 格式日志
- `CustomTextFormatter`: 处理文本格式日志

### 调用者信息获取

- 根据命令行参数动态启用/禁用 `logrus.SetReportCaller()`
- 只在需要文件或函数信息时才获取调用者信息，优化性能

## 最佳实践

1. **开发阶段**: 启用所有字段便于调试
   ```bash
   ./ck2sr -log-with-file=true -log-with-func=true
   ```

2. **生产环境**: 根据需要选择性启用
   ```bash
   # 高性能场景
   ./ck2sr -log-with-file=false -log-with-func=false

   # 平衡场景（保留文件信息便于定位问题）
   ./ck2sr -log-with-func=false
   ```

3. **日志聚合**: 在使用 ELK、Prometheus 等日志聚合系统时，可以禁用这些字段以减少数据量

4. **容器化部署**: Kubernetes 等容器环境中建议使用简洁格式

## 兼容性

- **版本要求**: ck2sr v2.1.2+
- **向后兼容**: 完全兼容现有配置和使用方式
- **默认行为**: 保持与之前版本相同的默认输出格式