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
	"github.com/sunkaimr/ck2sr/pkg/starrocks"
)

// StarRocksMySQLReader StarRocks MySQL协议读取器
// 实现了ExecutableReader接口，支持列过滤和复杂数据类型处理
type StarRocksMySQLReader struct {
	config         *config.DataSourceConfig
	logger         *logrus.Logger
	client         *starrocks.MySQLClient
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

func NewStarRocksMySQLReader(cfg *config.DataSourceConfig, cli *starrocks.MySQLClient, logger *logrus.Logger) (*StarRocksMySQLReader, error) {
	reader := &StarRocksMySQLReader{
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

func (r *StarRocksMySQLReader) SetClient(client *starrocks.MySQLClient) ExecutableReader {
	r.client = client
	return r
}

// SetColumnFilter 设置列过滤配置
// excludeColumns: 需要排除的列名列表
// fixedValues: 固定值列配置
func (r *StarRocksMySQLReader) SetColumnFilter(excludeColumns []string, fixedValues map[string]interface{}) ExecutableReader {
	r.excludeColumns = excludeColumns
	r.fixedValues = fixedValues
	return r
}

func (r *StarRocksMySQLReader) SetQuery(query string) ExecutableReader {
	r.query = query
	return r
}

// Execute 执行SQL查询
func (r *StarRocksMySQLReader) Execute(ctx context.Context) error {
	r.ctx = ctx
	r.logger.Infof("Executing query: %s", r.query)
	rows, err := r.client.Query(ctx, r.query)
	if err != nil {
		return fmt.Errorf("failed to execute query [%s]: %w", r.query, err)
	}
	r.rows = rows
	return nil
}

func (r *StarRocksMySQLReader) Next() bool {
	if r.rows == nil {
		return false
	}
	return r.rows.Next()
}

func (r *StarRocksMySQLReader) Err() error {
	return r.rows.Err()
}

func (r *StarRocksMySQLReader) ReleaseRecords(records []interface{}) {
	for _, rec := range records {
		if record, ok := rec.(*Record); ok {
			r.recordPool.Put(record)
		}
	}
}

// GetRecord 获取当前记录
// P0 优化：使用强类型 Record + 对象池 + 延迟 JSON 解析
func (r *StarRocksMySQLReader) GetRecord() (interface{}, error) {
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
			// strVal := string(v)
			// if r.isJSONOrArray(strVal) {
			// 	// 保存为 RawValue，延迟到 Writer 端处理
			// 	record.Values[i] = RawValue{
			// 		IsJSON: r.isJSON(strVal),
			// 		Data:   v, // 直接使用字节数组，避免字符串拷贝
			// 	}
			// } else {
			// 	record.Values[i] = strVal
			// }
			record.Values[i] = string(v)
		case string:
			// P3 优化：JSON/Array 延迟解析
			// if r.isJSONOrArray(v) {
			// 	record.Values[i] = RawValue{
			// 		// IsJSON: r.isJSON(v),
			// 		IsJSON: false, //TODO: [DEBUG] 先全部标记为非JSON
			// 		Data:   []byte(v),
			// 	}
			// } else {
			// 	record.Values[i] = v
			// }
			record.Values[i] = v

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

func (r *StarRocksMySQLReader) Close() error {
	if r.rows != nil {
		return r.rows.Close()
	}
	return nil
}

// initializeColumnMetadata 初始化列元数据（延迟初始化，仅调用一次）
func (r *StarRocksMySQLReader) initializeColumnMetadata() error {
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
		r.logger.Debugf("[DEBUG] Column %s has type %s", columns[i], dataTypes[i])
	}

	// 创建共享的列元数据
	r.columnMetadata = NewColumnMetadata(columns, dataTypes)

	// 创建记录对象池
	r.recordPool = NewRecordPool(r.columnMetadata)

	r.logger.Infof("Initialized column metadata: %d columns", len(columns))
	return nil
}

// isJSONOrArray 检查字符串是否为 JSON 或 Array 格式
func (r *StarRocksMySQLReader) isJSONOrArray(value string) bool {
	if len(value) == 0 {
		return false
	}

	// 去除前后空白
	value = strings.TrimSpace(value)

	// 检查是否以 { 或 [ 开头（JSON object/array 或 StarRocks array）
	firstChar := value[0]
	return firstChar == '{' || firstChar == '['
}

// isJSON 检查字符串是否为标准 JSON 格式（而非 StarRocks 数组格式）
func (r *StarRocksMySQLReader) isJSON(value string) bool {
	if len(value) == 0 {
		return false
	}

	value = strings.TrimSpace(value)

	// JSON object: 以 { 开头
	if value[0] == '{' {
		return true
	}

	// JSON array vs StarRocks array:
	// StarRocks: ['item1','item2'] or [1,2,3]
	// JSON: ["item1","item2"] or [1,2,3]
	// 简单启发式：如果包含双引号，判定为 JSON；如果包含单引号，判定为 StarRocks array
	if value[0] == '[' {
		// 尝试快速解析判断
		// JSON 标准使用双引号，StarRocks 数组通常使用单引号
		if strings.Contains(value, "\"") {
			return true
		}
		// 如果没有引号（纯数字数组），尝试 JSON 解析验证
		if !strings.Contains(value, "'") {
			var temp interface{}
			return json.Unmarshal([]byte(value), &temp) == nil
		}
		return false
	}

	return false
}

// isColumnExcluded 检查列是否在排除列表中
func (r *StarRocksMySQLReader) isColumnExcluded(columnName string) bool {
	for _, excludeCol := range r.excludeColumns {
		if excludeCol == columnName {
			return true
		}
	}
	return false
}

// parseArrayOrJSON 解析字符串为数组或JSON对象
// 支持StarRocks数组格式：['item1','item2'] 和标准JSON格式
func (r *StarRocksMySQLReader) parseArrayOrJSON(value string) (interface{}, error) {
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

	// 如果不是标准JSON，尝试解析StarRocks数组格式
	if strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]") {
		return r.parseStarRocksArray(value)
	}

	// 如果都不是，返回错误
	return nil, fmt.Errorf("not a valid array or JSON format")
}

// parseStarRocksArray 解析StarRocks数组格式
// 支持格式：['item1','item2'] 或 [1,2,3] 等
// P0 优化方案2：预分配切片容量，避免动态扩容
func (r *StarRocksMySQLReader) parseStarRocksArray(value string) (interface{}, error) {
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
func (r *StarRocksMySQLReader) splitArrayItems(s string) []string {
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

type StarRocksFlightSQLReader struct {
	config  *config.DataSourceConfig
	client  *starrocks.FlightSQLClient
	query   string
	records []interface{}
	index   int
	ctx     context.Context
}

func NewStarRocksFlightSQLReader(cfg *config.DataSourceConfig, cli *starrocks.FlightSQLClient, logger *logrus.Logger) (*StarRocksFlightSQLReader, error) {
	ret := &StarRocksFlightSQLReader{
		config: cfg,
		ctx:    context.Background(),
	}
	ret.SetClient(cli)
	return ret, nil
}

func (r *StarRocksFlightSQLReader) SetClient(client *starrocks.FlightSQLClient) ExecutableReader {
	r.client = client
	return r
}

func (r *StarRocksFlightSQLReader) SetColumnFilter(excludeColumns []string, fixedValues map[string]interface{}) ExecutableReader {
	// FlightSQL Reader暂不支持列过滤
	return r
}

func (r *StarRocksFlightSQLReader) SetQuery(query string) ExecutableReader {
	r.query = query
	return r
}

func (r *StarRocksFlightSQLReader) Execute(ctx context.Context) error {
	r.ctx = ctx
	records, err := r.client.QueryRecords(ctx, r.query)
	if err != nil {
		return fmt.Errorf("failed to execute query(%s): %w", r.query, err)
	}
	r.records = records
	r.index = 0
	return nil
}

func (r *StarRocksFlightSQLReader) Next() bool {
	return r.index < len(r.records)
}

func (r *StarRocksFlightSQLReader) Err() error {
	return nil
}

// GetRecord 获取当前记录（FlightSQL Reader）
func (r *StarRocksFlightSQLReader) GetRecord() (interface{}, error) {
	if r.index >= len(r.records) {
		return nil, fmt.Errorf("no more records")
	}
	record := r.records[r.index]
	r.index++
	return record, nil
}

func (r *StarRocksFlightSQLReader) ReleaseRecords(records []interface{}) {
	// FlightSQL Reader不使用对象池，无需释放
}

func (r *StarRocksFlightSQLReader) Close() error {
	r.records = nil
	return nil
}
