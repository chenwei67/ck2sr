package reader

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
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

// GetRecord 获取当前记录
// 返回interface{}类型的记录数据，通常为map[string]interface{}
func (r *StarRocksMySQLReader) GetRecord() (interface{}, error) {
	if r.rows == nil {
		return nil, fmt.Errorf("no active rows")
	}

	columns, err := r.rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := r.rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	// 构建记录map，应用列过滤
	record := make(map[string]interface{})
	for i, val := range values {
		columnName := columns[i]
		if val == nil {
			r.logger.Debugf("Column %s is NULL, skipping", columnName)
			continue
		}
		// 检查是否在排除列表中
		if r.isColumnExcluded(columnName) {
			r.logger.Debugf("Column %s is filtered, skipping", columnName)
			continue
		}

		// 正确处理不同数据类型，特别是字节数组和数组类型
		switch v := val.(type) {
		case []byte:
			strVal := string(v)
			// 尝试解析为数组或JSON对象
			parsedVal, err := r.parseArrayOrJSON(strVal)
			if err == nil {
				// 成功解析为数组或对象，使用解析后的值
				record[columnName] = parsedVal
			} else {
				// 解析失败，作为普通字符串处理
				record[columnName] = strVal
			}
		case string:
			// 字符串类型，尝试解析为数组或JSON对象
			parsedVal, err := r.parseArrayOrJSON(v)
			if err == nil {
				record[columnName] = parsedVal
			} else {
				record[columnName] = v
			}
		case int64, int32, int16, int8, int, uint64, uint32, uint16, uint8, uint, float32, float64, bool:
			// 整数类型直接使用
			record[columnName] = v
		default:
			// 其他类型转换为字符串
			record[columnName] = fmt.Sprintf("%v", v)
		}
	}

	// 固定值覆盖
	maps.Copy(record, r.fixedValues)

	return record, nil
}

func (r *StarRocksMySQLReader) Close() error {
	if r.rows != nil {
		return r.rows.Close()
	}
	return nil
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
func (r *StarRocksMySQLReader) parseStarRocksArray(value string) (interface{}, error) {
	// 移除外层的方括号
	inner := strings.TrimSpace(value[1 : len(value)-1])
	if inner == "" {
		return []interface{}{}, nil // 空数组
	}

	// 简单的逗号分割解析
	items := r.splitArrayItems(inner)
	result := make([]interface{}, len(items))

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

// GetRecord 获取当前记录（FlightSQL Reader）
func (r *StarRocksFlightSQLReader) GetRecord() (interface{}, error) {
	if r.index >= len(r.records) {
		return nil, fmt.Errorf("no more records")
	}
	record := r.records[r.index]
	r.index++
	return record, nil
}

func (r *StarRocksFlightSQLReader) Close() error {
	r.records = nil
	return nil
}
