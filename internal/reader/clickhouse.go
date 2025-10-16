package reader

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/clickhouse"
)

// ClickHouseMySQLReader ClickHouse MySQL协议读取器
// 实现了OffsetReader接口，支持偏移量和断点续传
type ClickHouseMySQLReader struct {
	config         *config.DataSourceConfig
	logger         *logrus.Logger
	client         *clickhouse.MySQLClient
	rows           *sql.Rows
	ctx            context.Context
	query          string                 // SQL查询语句
	excludeColumns []string               // 排除的列
	fixedValues    map[string]interface{} // 固定值列

	// P0 优化：引入强类型记录和对象池
	columnMetadata *ColumnMetadata // 列元数据（所有记录共享）
	recordPool     *RecordPool     // 记录对象池
	initialized    bool            // 是否已初始化列元数据
}

// NewClickHouseMySQLReader 创建新的ClickHouse MySQL读取器
func NewClickHouseMySQLReader(cfg *config.DataSourceConfig, cli *clickhouse.MySQLClient, logger *logrus.Logger) (*ClickHouseMySQLReader, error) {
	reader := &ClickHouseMySQLReader{
		config:         cfg,
		logger:         logger,
		ctx:            context.Background(),
		query:          "",
		excludeColumns: []string{},
		fixedValues:    make(map[string]interface{}),
	}
	reader.SetClient(cli)

	return reader, nil
}

// SetClient 设置ClickHouse MySQL客户端
func (r *ClickHouseMySQLReader) SetClient(client *clickhouse.MySQLClient) ExecutableReader {
	r.client = client
	return r
}

// SetColumnFilter 设置列过滤配置
// excludeColumns: 需要排除的列名列表
// fixedValues: 固定值列配置
func (r *ClickHouseMySQLReader) SetColumnFilter(excludeColumns []string, fixedValues map[string]interface{}) ExecutableReader {
	r.excludeColumns = excludeColumns
	r.fixedValues = fixedValues
	return r
}

// Execute 执行SQL查询，支持OFFSET优化
func (r *ClickHouseMySQLReader) SetQuery(query string) ExecutableReader {
	r.query = query
	return r
}

// Execute 执行SQL查询
func (r *ClickHouseMySQLReader) Execute(ctx context.Context) error {
	r.ctx = ctx
	r.logger.Infof("Executing query: %s", r.query)
	rows, err := r.client.Query(ctx, r.query)
	if err != nil {
		return fmt.Errorf("failed to execute query [%s]: %w", r.query, err)
	}
	r.rows = rows
	return nil
}

func (r *ClickHouseMySQLReader) Next() bool {
	if r.rows == nil {
		return false
	}
	return r.rows.Next()
}

// GetRecord 获取当前记录
// P0 优化：使用强类型 Record + 对象池 + 延迟 JSON 解析
func (r *ClickHouseMySQLReader) GetRecord() (interface{}, error) {
	if r.rows == nil {
		return nil, fmt.Errorf("no active rows")
	}

	// 延迟初始化列元数据（仅第一次调用时）
	if !r.initialized {
		if err := r.initializeColumnMetadata(); err != nil {
			return nil, fmt.Errorf("failed to initialize column metadata: %w", err)
		}
		r.initialized = true
	}

	// 从对象池获取 Record
	record := r.recordPool.Get()

	// 准备 Scan 目标
	valuePtrs := make([]interface{}, len(record.Values))
	for i := range record.Values {
		valuePtrs[i] = &record.Values[i]
	}

	// 扫描行数据
	if err := r.rows.Scan(valuePtrs...); err != nil {
		r.recordPool.Put(record) // 归还对象池
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	// 处理列过滤和类型转换
	for i, val := range record.Values {
		columnName := r.columnMetadata.Names[i]

		// NULL 值处理
		if val == nil {
			r.logger.Debugf("Column %s is NULL, skipping", columnName)
			continue
		}

		// 检查是否在排除列表中
		if r.isColumnExcluded(columnName) {
			r.logger.Debugf("Column %s is filtered, skipping", columnName)
			record.Values[i] = nil
			continue
		}

		// P0 优化方案2+3：类型转换优化
		switch v := val.(type) {
		case []byte:
			// P3 优化：JSON/Array 延迟解析
			strVal := string(v)
			if r.isArray(strVal) {
				// 保存为 RawValue，延迟到 Writer 端处理
				record.Values[i] = RawValue{
					IsJSON: true,
					Data:   v, // 直接使用字节数组，避免字符串拷贝
				}
			} else {
				record.Values[i] = strVal
			}

		case string:
			// P3 优化：JSON/Array 延迟解析
			if r.isArray(v) {
				record.Values[i] = RawValue{
					IsJSON: true,
					Data:   []byte(v),
				}
			} else {
				record.Values[i] = v
			}

		case int64, int32, int16, int8, int, uint64, uint32, uint16, uint8, uint, float32, float64, bool:
			// 数值类型直接使用
			record.Values[i] = v

		default:
			// 其他类型转换为字符串
			record.Values[i] = fmt.Sprintf("%v", v)
		}
	}

	// 应用固定值列（如果有）
	for colName, fixedVal := range r.fixedValues {
		if err := record.Set(colName, fixedVal); err != nil {
			r.logger.Warnf("Failed to set fixed value for column %s: %v", colName, err)
		}
	}

	return record, nil
}

// initializeColumnMetadata 初始化列元数据（延迟初始化，仅调用一次）
func (r *ClickHouseMySQLReader) initializeColumnMetadata() error {
	if r.rows == nil {
		return fmt.Errorf("no active rows to initialize metadata")
	}

	// 获取列名
	columns, err := r.rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %w", err)
	}

	// 获取列类型
	columnTypes, err := r.rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("failed to get column types: %w", err)
	}

	// 构建列类型名称列表
	dataTypes := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		dataTypes[i] = ct.DatabaseTypeName()
	}

	// 创建共享的列元数据
	r.columnMetadata = NewColumnMetadata(columns, dataTypes)

	// 创建记录对象池
	r.recordPool = NewRecordPool(r.columnMetadata)

	r.logger.Infof("Initialized column metadata: %d columns", len(columns))
	return nil
}

// isArray 检查字符串是否为  Array 格式
func (r *ClickHouseMySQLReader) isArray(value string) bool {
	if len(value) == 0 {
		return false
	}

	// 去除前后空白
	value = strings.TrimSpace(value)
	firstChar := value[0]
	endChar := value[len(value)-1]

	// JSON数组必须以[开头并以]结尾
	isArray := firstChar == '[' && endChar == ']'

	return isArray
}

// Close 关闭读取器和相关资源
func (r *ClickHouseMySQLReader) Close() error {
	if r.rows != nil {
		return r.rows.Close()
	}
	return nil
}

// isColumnExcluded 检查列是否在排除列表中
func (r *ClickHouseMySQLReader) isColumnExcluded(columnName string) bool {
	for _, excludeCol := range r.excludeColumns {
		if excludeCol == columnName {
			return true
		}
	}
	return false
}

// parseArrayOrJSON 解析字符串为数组或JSON对象
// 支持ClickHouse数组格式：['item1','item2'] 和标准JSON格式
func (r *ClickHouseMySQLReader) parseArrayOrJSON(value string) (interface{}, error) {
	if value == "" {
		return nil, fmt.Errorf("empty value")
	}

	// 去除前后空白字符
	value = strings.TrimSpace(value)

	// 尝试解析为标准JSON
	var jsonResult interface{}
	if err := json.Unmarshal([]byte(value), &jsonResult); err == nil {
		return jsonResult, nil
	}

	// 如果不是标准JSON，尝试解析ClickHouse数组格式
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		return r.parseClickHouseArray(value)
	}

	// 如果都不是，返回错误
	return nil, fmt.Errorf("not a valid array or JSON format")
}

// parseClickHouseArray 解析ClickHouse数组格式
// 支持格式：['item1','item2'] 或 [1,2,3] 等
// P0 优化方案2：预分配切片容量，避免动态扩容
func (r *ClickHouseMySQLReader) parseClickHouseArray(value string) (interface{}, error) {
	// 移除外层的方括号
	inner := strings.TrimSpace(value[1 : len(value)-1])
	if inner == "" {
		return []interface{}{}, nil // 空数组
	}

	// P0 方案2优化：预估数组元素个数，预分配容量
	// 通过统计逗号数量估算元素数量（逗号数 + 1）
	estimatedLen := strings.Count(inner, ",") + 1
	items := r.splitArrayItems(inner)

	// 使用预估容量初始化结果切片，避免 bytes.growSlice 开销
	result := make([]interface{}, 0, estimatedLen)
	result = result[:len(items)] // 设置实际长度

	for i, item := range items {
		item = strings.TrimSpace(item)

		// 移除引号（如果有）
		if (strings.HasPrefix(item, "'") && strings.HasSuffix(item, "'")) ||
			(strings.HasPrefix(item, "\"") && strings.HasSuffix(item, "\"")) {
			item = item[1 : len(item)-1]
			result[i] = item
		} else {
			// 尝试解析为数字
			if intVal, err := strconv.ParseInt(item, 10, 64); err == nil {
				result[i] = intVal
			} else if floatVal, err := strconv.ParseFloat(item, 64); err == nil {
				result[i] = floatVal
			} else if boolVal, err := strconv.ParseBool(item); err == nil {
				result[i] = boolVal
			} else {
				result[i] = item // 作为字符串
			}
		}
	}

	return result, nil
}

// splitArrayItems 智能分割数组项（考虑引号内的逗号）
func (r *ClickHouseMySQLReader) splitArrayItems(s string) []string {
	var items []string
	var current strings.Builder
	inQuotes := false
	quoteChar := byte(0)

	for i := 0; i < len(s); i++ {
		char := s[i]

		if !inQuotes && (char == '\'' || char == '"') {
			inQuotes = true
			quoteChar = char
			current.WriteByte(char)
		} else if inQuotes && char == quoteChar {
			inQuotes = false
			current.WriteByte(char)
		} else if !inQuotes && char == ',' {
			items = append(items, current.String())
			current.Reset()
		} else {
			current.WriteByte(char)
		}
	}

	// 添加最后一项
	if current.Len() > 0 {
		items = append(items, current.String())
	}

	return items
}

type ClickHouseHTTPReader struct {
	config *config.DataSourceConfig
	client *clickhouse.HTTPClient
	query  string
	data   []byte
	index  int
	ctx    context.Context
	logger *logrus.Logger
}

func NewClickHouseHTTPReader(cfg *config.DataSourceConfig, cli *clickhouse.HTTPClient, logger *logrus.Logger) (*ClickHouseHTTPReader, error) {
	reader := &ClickHouseHTTPReader{
		config: cfg,
		ctx:    context.Background(),
		logger: logger,
	}
	reader.SetClient(cli)

	return reader, nil
}

func (r *ClickHouseHTTPReader) SetClient(client *clickhouse.HTTPClient) ExecutableReader {
	r.client = client
	return r
}

func (r *ClickHouseHTTPReader) SetQuery(query string) ExecutableReader {
	r.query = query
	return r
}

func (r *ClickHouseHTTPReader) SetColumnFilter(excludeColumns []string, fixedValues map[string]interface{}) ExecutableReader {
	// HTTP Reader暂不支持列过滤
	r.logger.Warn("SetColumnFilter is not supported for ClickHouse HTTP Reader")
	return r
}

func (r *ClickHouseHTTPReader) Execute(ctx context.Context) error {
	r.ctx = ctx
	data, err := r.client.Query(ctx, r.query, "JSONEachRow")
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	r.data = data
	r.index = 0
	return nil
}

func (r *ClickHouseHTTPReader) Next() bool {
	return r.index < len(r.data)
}

// GetRecord 获取当前记录（HTTP Reader需要自定义JSON解析）
func (r *ClickHouseHTTPReader) GetRecord() (interface{}, error) {
	return nil, fmt.Errorf("HTTP reader requires custom JSON parsing")
}

func (r *ClickHouseHTTPReader) Close() error {
	return nil
}
