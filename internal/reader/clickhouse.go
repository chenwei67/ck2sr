package reader

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/clickhouse"

	// ClickHouse官方SDK

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

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

// ClickHouseReader 基于官方SDK的ClickHouse读取器
// 使用 github.com/ClickHouse/clickhouse-go/v2 原生协议
// 性能优化：
// 1. 使用原生协议，避免MySQL协议的兼容性和性能问题
// 2. 对象池减少内存分配
// 3. 延迟JSON解析，减少序列化开销
type ClickHouseReader struct {
	config         *config.DataSourceConfig
	logger         *logrus.Logger
	client         *clickhouse.MySQLClient
	rows           driver.Rows // 查询结果集
	ctx            context.Context
	query          string                 // SQL查询语句
	excludeColumns []string               // 排除的列
	fixedValues    map[string]interface{} // 固定值列

	// 性能优化：引入强类型记录和对象池
	columnMetadata *ColumnMetadata // 列元数据（所有记录共享）
	recordPool     *RecordPool     // 记录对象池
	initialized    bool            // 是否已初始化列元数据
	scanDest       []interface{}   // 扫描目标缓冲区（复用）
}

// NewClickHouseReader 创建基于官方SDK的ClickHouse读取器
// 复用MySQLConfig配置，但使用ClickHouse原生协议
func NewClickHouseReader(cfg *config.DataSourceConfig, cli *clickhouse.MySQLClient, logger *logrus.Logger) (*ClickHouseReader, error) {
	reader := &ClickHouseReader{
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

func (r *ClickHouseReader) SetClient(client *clickhouse.MySQLClient) ExecutableReader {
	r.client = client
	return r
}

// SetColumnFilter 设置列过滤配置
func (r *ClickHouseReader) SetColumnFilter(excludeColumns []string, fixedValues map[string]interface{}) ExecutableReader {
	r.excludeColumns = excludeColumns
	r.fixedValues = fixedValues
	return r
}

// SetQuery 设置SQL查询语句
func (r *ClickHouseReader) SetQuery(query string) ExecutableReader {
	r.query = query
	return r
}

// Execute 执行SQL查询
func (r *ClickHouseReader) Execute(ctx context.Context) error {
	r.ctx = ctx
	r.logger.Infof("Executing ClickHouse native query: %s", r.query)

	rows, err := r.client.Query(ctx, r.query)
	if err != nil {
		return fmt.Errorf("failed to execute query [%s]: %w", r.query, err)
	}

	r.rows = rows
	r.initialized = false // 重置初始化状态
	return nil
}

// Next 移动到下一条记录
func (r *ClickHouseReader) Next() bool {
	if r.rows == nil {
		return false
	}
	return r.rows.Next()
}

// GetRecord 获取当前记录
// 性能优化：使用强类型 Record + 对象池 + 延迟 JSON 解析
func (r *ClickHouseReader) GetRecord() (interface{}, error) {
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

	// 扫描行数据 - 使用复用的scanDest缓冲区
	if err := r.rows.Scan(r.scanDest...); err != nil {
		r.recordPool.Put(record) // 归还对象池
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	// 处理列过滤和类型转换
	for i, val := range r.scanDest {
		columnName := r.columnMetadata.Names[i]

		// NULL 值处理
		if val == nil {
			record.Values[i] = nil
			continue
		}

		// 检查是否在排除列表中
		if r.isColumnExcluded(columnName) {
			record.Values[i] = nil
			continue
		}

		// 类型转换和优化
		converted, err := r.convertClickHouseType(val, r.columnMetadata.DataTypes[i])
		if err != nil {
			r.logger.Warnf("Failed to convert column %s: %v", columnName, err)
			record.Values[i] = val // 使用原始值
		} else {
			record.Values[i] = converted
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

// initializeColumnMetadata 初始化列元数据
func (r *ClickHouseReader) initializeColumnMetadata() error {
	if r.rows == nil {
		return fmt.Errorf("no active rows to initialize metadata")
	}

	// 获取列类型信息（ClickHouse原生接口）
	columnTypes := r.rows.ColumnTypes()
	columnCount := len(columnTypes)

	// 构建列名和类型列表
	columns := make([]string, columnCount)
	dataTypes := make([]string, columnCount)

	for i, ct := range columnTypes {
		columns[i] = ct.Name()
		dataTypes[i] = ct.DatabaseTypeName()
	}

	// 创建共享的列元数据
	r.columnMetadata = NewColumnMetadata(columns, dataTypes)

	// 创建记录对象池
	r.recordPool = NewRecordPool(r.columnMetadata)

	// 初始化扫描目标缓冲区（根据列类型创建正确的目标变量）
	r.scanDest = make([]interface{}, columnCount)
	for i, ct := range columnTypes {
		r.scanDest[i] = r.createScanDestination(ct)
	}

	r.logger.Infof("Initialized ClickHouse column metadata: %d columns", columnCount)
	return nil
}

// createScanDestination 根据列类型创建正确的扫描目标变量
// ClickHouse SDK 要求使用具体类型的指针，不支持 *interface{}
func (r *ClickHouseReader) createScanDestination(columnType driver.ColumnType) interface{} {
	typeName := columnType.DatabaseTypeName()

	// 根据 ClickHouse 类型名称创建对应的 Go 类型指针
	switch {
	// 整数类型
	case typeName == "Int8":
		return new(int8)
	case typeName == "Int16":
		return new(int16)
	case typeName == "Int32":
		return new(int32)
	case typeName == "Int64":
		return new(int64)
	case typeName == "UInt8":
		return new(uint8)
	case typeName == "UInt16":
		return new(uint16)
	case typeName == "UInt32":
		return new(uint32)
	case typeName == "UInt64":
		return new(uint64)

	// 浮点类型
	case typeName == "Float32":
		return new(float32)
	case typeName == "Float64":
		return new(float64)

	// 布尔类型
	case typeName == "Bool":
		return new(bool)

	// 字符串类型
	case typeName == "String", strings.HasPrefix(typeName, "FixedString"):
		return new(string)

	// 日期时间类型
	case typeName == "Date", typeName == "Date32":
		return new(time.Time)
	case strings.HasPrefix(typeName, "DateTime"):
		return new(time.Time)

	// UUID
	case typeName == "UUID":
		return new(string) // ClickHouse SDK 返回 UUID 为 string

	// IPv4/IPv6
	case typeName == "IPv4", typeName == "IPv6":
		return new(string)

	// Enum 类型
	case strings.HasPrefix(typeName, "Enum"):
		return new(string) // Enum 会被扫描为字符串

	// Decimal 类型
	case strings.HasPrefix(typeName, "Decimal"):
		return new(float64) // Decimal 通常扫描为 float64

	// Array 类型
	case strings.HasPrefix(typeName, "Array"):
		return new([]interface{}) // Array 扫描为切片

	// Map 类型
	case strings.HasPrefix(typeName, "Map"):
		return new(map[string]interface{}) // Map 扫描为 map

	// Tuple 类型
	case strings.HasPrefix(typeName, "Tuple"):
		return new([]interface{}) // Tuple 扫描为切片

	// JSON 类型
	case typeName == "JSON":
		return new(string) // JSON 扫描为字符串

	// Nullable 类型 - 递归处理内部类型
	case strings.HasPrefix(typeName, "Nullable"):
		// 提取内部类型，例如 "Nullable(Int32)" -> "Int32"
		innerType := extractInnerType(typeName, "Nullable")
		// 创建一个模拟的 ColumnType 用于递归
		mockColumnType := &mockColumnType{typeName: innerType}
		return r.createScanDestination(mockColumnType)

	// LowCardinality 类型 - 递归处理内部类型
	case strings.HasPrefix(typeName, "LowCardinality"):
		innerType := extractInnerType(typeName, "LowCardinality")
		mockColumnType := &mockColumnType{typeName: innerType}
		return r.createScanDestination(mockColumnType)

	// 默认情况：使用 string 作为安全的回退
	default:
		r.logger.Warnf("Unknown ClickHouse type: %s, using string as fallback", typeName)
		return new(string)
	}
}

// mockColumnType 模拟 ColumnType 接口，用于递归处理嵌套类型
type mockColumnType struct {
	typeName string
}

func (m *mockColumnType) Name() string {
	return ""
}

func (m *mockColumnType) DatabaseTypeName() string {
	return m.typeName
}

func (m *mockColumnType) ScanType() reflect.Type {
	return nil
}

func (m *mockColumnType) Nullable() bool {
	return strings.HasPrefix(m.typeName, "Nullable")
}

// extractInnerType 从包装类型中提取内部类型
// 例如: "Nullable(Int32)" -> "Int32"
//
//	"LowCardinality(String)" -> "String"
func extractInnerType(typeName, wrapper string) string {
	prefix := wrapper + "("
	if !strings.HasPrefix(typeName, prefix) {
		return typeName
	}

	// 移除前缀和后缀括号
	inner := strings.TrimPrefix(typeName, prefix)
	inner = strings.TrimSuffix(inner, ")")
	return strings.TrimSpace(inner)
}

// convertClickHouseType 转换ClickHouse类型到通用类型
// 针对ClickHouse特有类型进行优化处理
func (r *ClickHouseReader) convertClickHouseType(val interface{}, dataType string) (interface{}, error) {
	if val == nil {
		return nil, nil
	}

	// 统一解引用所有类型的指针
	val = derefPointer(val)
	if val == nil {
		return nil, nil
	}

	// 处理ClickHouse特有类型
	switch {
	// Array类型
	case strings.HasPrefix(dataType, "Array"):
		return r.convertArray(val)

	// Map类型
	case strings.HasPrefix(dataType, "Map"):
		return r.convertMap(val)

	// Tuple类型
	case strings.HasPrefix(dataType, "Tuple"):
		return r.convertTuple(val)

	// DateTime系列
	case strings.HasPrefix(dataType, "DateTime"):
		return r.convertDateTime(val)

	// Date系列
	case strings.HasPrefix(dataType, "Date"):
		return r.convertDate(val)

	// Decimal系列
	case strings.HasPrefix(dataType, "Decimal"):
		return r.convertDecimal(val)

	// UUID
	case dataType == "UUID":
		return fmt.Sprintf("%v", val), nil

	// IPv4/IPv6
	case dataType == "IPv4" || dataType == "IPv6":
		return fmt.Sprintf("%v", val), nil

	// Enum系列
	case strings.HasPrefix(dataType, "Enum"):
		return fmt.Sprintf("%v", val), nil

	// 基础数值类型 - 直接返回
	case dataType == "Int8", dataType == "Int16", dataType == "Int32", dataType == "Int64",
		dataType == "UInt8", dataType == "UInt16", dataType == "UInt32", dataType == "UInt64",
		dataType == "Float32", dataType == "Float64":
		return val, nil

	// String/FixedString
	case dataType == "String", strings.HasPrefix(dataType, "FixedString"):
		if str, ok := val.(string); ok {
			return str, nil
		}
		return fmt.Sprintf("%v", val), nil

	// Boolean
	case dataType == "Bool":
		return val, nil

	// JSON (ClickHouse 21.12+)
	case dataType == "JSON":
		// 保存为RawValue，延迟到Writer端处理
		if str, ok := val.(string); ok {
			return RawValue{
				IsJSON: true,
				Data:   []byte(str),
			}, nil
		}
		return val, nil

	// Nullable 和 LowCardinality - 已在 scanDest 创建时处理，这里直接返回
	case strings.HasPrefix(dataType, "Nullable"), strings.HasPrefix(dataType, "LowCardinality"):
		// 递归提取内部类型并转换
		innerType := dataType
		if strings.HasPrefix(dataType, "Nullable") {
			innerType = extractInnerType(dataType, "Nullable")
		} else if strings.HasPrefix(dataType, "LowCardinality") {
			innerType = extractInnerType(dataType, "LowCardinality")
		}
		return r.convertClickHouseType(val, innerType)

	default:
		// 其他类型转换为字符串
		return fmt.Sprintf("%v", val), nil
	}
}

// derefPointer 统一解引用各种类型的指针
func derefPointer(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	v := reflect.ValueOf(val)

	// 循环解引用，直到非指针类型
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}

	// 返回解引用后的值
	if v.IsValid() {
		return v.Interface()
	}

	return nil
}

// convertArray 转换ClickHouse Array类型
func (r *ClickHouseReader) convertArray(val interface{}) (interface{}, error) {
	// ClickHouse SDK返回的Array通常是[]interface{}
	if arr, ok := val.([]interface{}); ok {
		// 直接返回，Writer会序列化为JSON数组
		return arr, nil
	}

	// 如果是字符串格式，解析为数组
	if str, ok := val.(string); ok {
		if strings.HasPrefix(str, "[") && strings.HasSuffix(str, "]") {
			// 保存为RawValue，延迟解析
			return RawValue{
				IsJSON: true,
				Data:   []byte(str),
			}, nil
		}
	}

	// 其他情况直接返回
	return val, nil
}

// convertMap 转换ClickHouse Map类型
func (r *ClickHouseReader) convertMap(val interface{}) (interface{}, error) {
	// ClickHouse SDK返回的Map通常是map[interface{}]interface{}
	if m, ok := val.(map[interface{}]interface{}); ok {
		// 转换为map[string]interface{}以便JSON序列化
		result := make(map[string]interface{}, len(m))
		for k, v := range m {
			result[fmt.Sprintf("%v", k)] = v
		}
		return result, nil
	}

	if m, ok := val.(map[string]interface{}); ok {
		return m, nil
	}

	return val, nil
}

// convertTuple 转换ClickHouse Tuple类型
func (r *ClickHouseReader) convertTuple(val interface{}) (interface{}, error) {
	// Tuple通常返回为[]interface{}
	if arr, ok := val.([]interface{}); ok {
		return arr, nil
	}

	return val, nil
}

// convertDateTime 转换DateTime类型
func (r *ClickHouseReader) convertDateTime(val interface{}) (interface{}, error) {
	if t, ok := val.(time.Time); ok {
		// 返回ISO8601格式字符串
		return t.Format(time.RFC3339), nil
	}

	return val, nil
}

// convertDate 转换Date类型
func (r *ClickHouseReader) convertDate(val interface{}) (interface{}, error) {
	if t, ok := val.(time.Time); ok {
		// 返回日期格式字符串
		return t.Format("2006-01-02"), nil
	}

	return val, nil
}

// convertDecimal 转换Decimal类型
func (r *ClickHouseReader) convertDecimal(val interface{}) (interface{}, error) {
	// Decimal可能以不同类型返回
	switch v := val.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case string:
		// 尝试解析为float64
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, nil
		}
		return v, nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

// isColumnExcluded 检查列是否在排除列表中
func (r *ClickHouseReader) isColumnExcluded(columnName string) bool {
	for _, excludeCol := range r.excludeColumns {
		if excludeCol == columnName {
			return true
		}
	}
	return false
}

// Close 关闭读取器和相关资源
func (r *ClickHouseReader) Close() error {
	var errs []error

	if r.rows != nil {
		if err := r.rows.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close rows: %w", err))
		}
		r.rows = nil
	}

	if r.client != nil {
		if err := r.client.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close connection: %w", err))
		}
		r.client = nil
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing ClickHouseReader: %v", errs)
	}

	return nil
}
