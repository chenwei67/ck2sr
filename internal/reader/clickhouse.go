package reader

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/clickhouse"
)

// ClickHouseMySQLReader ClickHouse MySQL协议读取器
// 实现了OffsetReader接口，支持偏移量和断点续传
type ClickHouseMySQLReader struct {
	config        *config.DataSourceConfig
	client        *clickhouse.MySQLClient
	rows          *sql.Rows
	ctx           context.Context
	offset        int64                  // 当前读取偏移量
	excludeColumns []string               // 排除的列
	fixedValues   map[string]interface{} // 固定值列
}

// NewClickHouseMySQLReader 创建新的ClickHouse MySQL读取器
func NewClickHouseMySQLReader(cfg *config.DataSourceConfig) (*ClickHouseMySQLReader, error) {
	return &ClickHouseMySQLReader{
		config:        cfg,
		ctx:           context.Background(),
		offset:        0,
		excludeColumns: []string{},
		fixedValues:   make(map[string]interface{}),
	}, nil
}

// SetClient 设置ClickHouse MySQL客户端
func (r *ClickHouseMySQLReader) SetClient(client *clickhouse.MySQLClient) {
	r.client = client
}

// SetColumnFilter 设置列过滤配置
// excludeColumns: 需要排除的列名列表
// fixedValues: 固定值列配置
func (r *ClickHouseMySQLReader) SetColumnFilter(excludeColumns []string, fixedValues map[string]interface{}) {
	r.excludeColumns = excludeColumns
	r.fixedValues = fixedValues
}

// Execute 执行SQL查询，支持OFFSET优化
func (r *ClickHouseMySQLReader) Execute(ctx context.Context, query string) error {
	r.ctx = ctx

	// 如果设置了offset，在query中添加OFFSET子句
	finalQuery := query
	if r.offset > 0 {
		// 在SQL末尾添加OFFSET（简化处理，实际应该更智能地解析SQL）
		finalQuery = fmt.Sprintf("%s OFFSET %d", query, r.offset)
	}

	rows, err := r.client.Query(ctx, finalQuery)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
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
// 返回interface{}类型的记录数据，通常为map[string]interface{}
func (r *ClickHouseMySQLReader) GetRecord() (interface{}, error) {
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
	for i, col := range columns {
		// 检查是否在排除列表中
		if r.isColumnExcluded(col) {
			continue
		}
		record[col] = values[i]
	}

	// 添加固定值列
	for key, value := range r.fixedValues {
		record[key] = value
	}

	return record, nil
}

// Close 关闭读取器和相关资源
func (r *ClickHouseMySQLReader) Close() error {
	if r.rows != nil {
		return r.rows.Close()
	}
	return nil
}

// ===== OffsetReader 接口实现 =====

// SetOffset 设置读取起始偏移量
func (r *ClickHouseMySQLReader) SetOffset(offset int64) error {
	r.offset = offset
	return nil
}

// GetOffset 获取当前读取位置
func (r *ClickHouseMySQLReader) GetOffset() int64 {
	return r.offset
}

// ===== 私有辅助方法 =====

// isColumnExcluded 检查列是否在排除列表中
func (r *ClickHouseMySQLReader) isColumnExcluded(columnName string) bool {
	for _, excludeCol := range r.excludeColumns {
		if excludeCol == columnName {
			return true
		}
	}
	return false
}

type ClickHouseHTTPReader struct {
	config *config.DataSourceConfig
	client *clickhouse.HTTPClient
	data   []byte
	index  int
	ctx    context.Context
}

func NewClickHouseHTTPReader(cfg *config.DataSourceConfig) (*ClickHouseHTTPReader, error) {
	return &ClickHouseHTTPReader{
		config: cfg,
		ctx:    context.Background(),
	}, nil
}

func (r *ClickHouseHTTPReader) SetClient(client *clickhouse.HTTPClient) {
	r.client = client
}

func (r *ClickHouseHTTPReader) Execute(ctx context.Context, query string) error {
	r.ctx = ctx
	data, err := r.client.Query(ctx, query, "JSONEachRow")
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