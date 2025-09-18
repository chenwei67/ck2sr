package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/sirupsen/logrus"
)

// Client ClickHouse 客户端
type Client struct {
	db        *sql.DB
	config    *config.ClickHouseConfig
	logger    *logrus.Logger
	allocator memory.Allocator
}

// NewClient 创建新的 ClickHouse 客户端
func NewClient(cfg *config.ClickHouseConfig, logger *logrus.Logger) (*Client, error) {
	if logger == nil {
		logger = logrus.New()
		logger.SetReportCaller(true)
	}

	// 创建内存分配器
	allocator := memory.NewGoAllocator()

	// 构建ClickHouse TCP连接字符串，支持ArrowStream格式
	dsn := fmt.Sprintf("tcp://%s:%d?database=%s", cfg.Host, cfg.Port, cfg.Database)
	if cfg.Username != "" {
		dsn += fmt.Sprintf("&username=%s", cfg.Username)
	}
	if cfg.Password != "" {
		dsn += fmt.Sprintf("&password=%s", cfg.Password)
	}

	// 添加ClickHouse特定配置
	if cfg.MaxBlockSize > 0 {
		dsn += fmt.Sprintf("&max_block_size=%d", cfg.MaxBlockSize)
	}

	// 注意：read_timeout 和 write_timeout 不应在DSN中设置
	// 这些超时将通过 context 和 sql.DB 配置来处理

	// 启用压缩和优化设置
	if cfg.CompressionType != "none" && cfg.CompressionType != "" {
		dsn += fmt.Sprintf("&compression=%s", cfg.CompressionType)
	}

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse database connection: %w", err)
	}

	// 配置连接池
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	// 测试连接 - 使用配置的读取超时或默认10秒
	timeout := 10 * time.Second
	if cfg.ReadTimeout > 0 {
		timeout = cfg.ReadTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	logger.Infof("Attempting to connect to ClickHouse - Host: %s, Port: %d, Database: %s, User: %s",
		cfg.Host, cfg.Port, cfg.Database, func() string {
			if cfg.Username == "" {
				return "default"
			}
			return cfg.Username
		}())

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	logger.Infof("Connected to ClickHouse at %s:%d via TCP, database: %s (compression: %s)",
		cfg.Host, cfg.Port, cfg.Database, func() string {
			if cfg.CompressionType == "" {
				return "none"
			}
			return cfg.CompressionType
		}())

	return &Client{
		db:        db,
		config:    cfg,
		logger:    logger,
		allocator: allocator,
	}, nil
}

// Close 关闭连接
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// TableInfo 表信息
type TableInfo struct {
	Name       string
	Engine     string
	TotalRows  int64
	TotalBytes int64
	Columns    []ColumnInfo
	CreateTime time.Time
	Comment    string
}

// ColumnInfo 列信息
type ColumnInfo struct {
	Name         string
	Type         string
	DefaultType  string
	DefaultValue string
	Comment      string
	IsNullable   bool
}

// GetTableInfo 获取表信息
func (c *Client) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	// 获取表基本信息
	query := `
		SELECT
			name,
			engine,
			total_rows,
			total_bytes,
			create_table_query,
			comment
		FROM system.tables
		WHERE database = ? AND name = ?
	`

	var info TableInfo
	var createQuery string
	err := c.db.QueryRowContext(ctx, query, c.config.Database, tableName).Scan(
		&info.Name,
		&info.Engine,
		&info.TotalRows,
		&info.TotalBytes,
		&createQuery,
		&info.Comment,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("table %s not found", tableName)
		}
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	// 获取列信息
	columns, err := c.getTableColumns(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table columns: %w", err)
	}
	info.Columns = columns

	// 解析创建时间（从创建语句中提取，这里简化处理）
	info.CreateTime = time.Now() // 实际应用中可能需要更复杂的解析

	return &info, nil
}

// getTableColumns 获取表列信息
func (c *Client) getTableColumns(ctx context.Context, tableName string) ([]ColumnInfo, error) {
	query := `
		SELECT
			name,
			type,
			default_kind,
			default_expression,
			comment,
			is_in_primary_key
		FROM system.columns
		WHERE database = ? AND table = ?
		ORDER BY position
	`

	rows, err := c.db.QueryContext(ctx, query, c.config.Database, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var isPrimaryKey bool

		err := rows.Scan(
			&col.Name,
			&col.Type,
			&col.DefaultType,
			&col.DefaultValue,
			&col.Comment,
			&isPrimaryKey,
		)
		if err != nil {
			return nil, err
		}

		// 简化判断是否可为空
		col.IsNullable = strings.Contains(strings.ToLower(col.Type), "nullable")

		columns = append(columns, col)
	}

	return columns, rows.Err()
}

// CountRows 统计表行数
func (c *Client) CountRows(ctx context.Context, tableName string, whereClause string) (int64, error) {
	query := fmt.Sprintf("SELECT count(*) FROM %s", tableName)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	var count int64
	err := c.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}

	return count, nil
}

// GetMinMaxValues 获取指定列的最小值和最大值
func (c *Client) GetMinMaxValues(ctx context.Context, tableName, columnName string, whereClause string) (interface{}, interface{}, error) {
	query := fmt.Sprintf("SELECT min(%s), max(%s) FROM %s", columnName, columnName, tableName)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	var minVal, maxVal interface{}
	err := c.db.QueryRowContext(ctx, query).Scan(&minVal, &maxVal)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get min/max values: %w", err)
	}

	return minVal, maxVal, nil
}

// ArrowStreamReader Arrow流式数据读取器
type ArrowStreamReader struct {
	client        *Client
	tableName     string
	columns       []string
	whereClause   string
	orderBy       string
	limit         int64
	offset        int64
	batchSize     int
	columnMapping map[string]string
}

// NewArrowStreamReader 创建Arrow流式数据读取器
func (c *Client) NewArrowStreamReader(tableName string) *ArrowStreamReader {
	return &ArrowStreamReader{
		client:    c,
		tableName: tableName,
		batchSize: c.config.BatchSize,
	}
}

// WithColumns 设置要读取的列
func (dr *ArrowStreamReader) WithColumns(columns []string) *ArrowStreamReader {
	dr.columns = columns
	return dr
}

// WithWhere 设置 WHERE 条件
func (dr *ArrowStreamReader) WithWhere(whereClause string) *ArrowStreamReader {
	dr.whereClause = whereClause
	return dr
}

// WithOrderBy 设置排序
func (dr *ArrowStreamReader) WithOrderBy(orderBy string) *ArrowStreamReader {
	dr.orderBy = orderBy
	return dr
}

// WithLimit 设置限制和偏移
func (dr *ArrowStreamReader) WithLimit(limit, offset int64) *ArrowStreamReader {
	dr.limit = limit
	dr.offset = offset
	return dr
}

// WithBatchSize 设置批次大小
func (dr *ArrowStreamReader) WithBatchSize(batchSize int) *ArrowStreamReader {
	dr.batchSize = batchSize
	return dr
}

// WithColumnMapping 设置列映射
func (dr *ArrowStreamReader) WithColumnMapping(mapping map[string]string) *ArrowStreamReader {
	dr.columnMapping = mapping
	return dr
}

// ReadArrowStream 使用TCP连接读取数据并转换为Arrow格式
func (dr *ArrowStreamReader) ReadArrowStream(ctx context.Context, callback func(record arrow.Record) error) error {
	// 构建查询语句
	var columns string
	if len(dr.columns) > 0 {
		columns = strings.Join(dr.columns, ", ")
	} else {
		columns = "*"
	}

	// 构建标准查询（不使用FORMAT ArrowStream）
	query := fmt.Sprintf("SELECT %s FROM %s", columns, dr.tableName)

	if dr.whereClause != "" {
		query += " WHERE " + dr.whereClause
	}

	if dr.orderBy != "" {
		query += " ORDER BY " + dr.orderBy
	}

	if dr.limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", dr.limit)
		if dr.offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", dr.offset)
		}
	}

	dr.client.logger.Infof("Executing ClickHouse query: %s", query)

	// 执行标准查询
	rows, err := dr.client.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute ClickHouse query: %w", err)
	}
	defer rows.Close()

	// 获取列信息
	columnNames, err := dr.getColumnInfo(rows)
	if err != nil {
		return fmt.Errorf("failed to get column info: %w", err)
	}

	// 处理数据行并转换为Arrow格式
	return dr.processRowsToArrow(rows, columnNames, callback)
}

// getColumnInfo 获取查询结果的列信息
func (dr *ArrowStreamReader) getColumnInfo(rows *sql.Rows) ([]string, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	columns := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		columns[i] = ct.Name()
	}

	return columns, nil
}

// processRowsToArrow 处理SQL行并转换为Arrow格式
func (dr *ArrowStreamReader) processRowsToArrow(rows *sql.Rows, columnNames []string, callback func(record arrow.Record) error) error {
	// 获取列类型信息
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return fmt.Errorf("failed to get column types: %w", err)
	}

	// 创建Arrow schema
	fields := make([]arrow.Field, len(columnTypes))
	for i, ct := range columnTypes {
		arrowType, err := dr.mapClickHouseTypeToArrow(ct)
		if err != nil {
			dr.client.logger.Warnf("Failed to map column %s type %s to Arrow, using string: %v", ct.Name(), ct.DatabaseTypeName(), err)
			arrowType = arrow.BinaryTypes.String
		}
		fields[i] = arrow.Field{
			Name: ct.Name(),
			Type: arrowType,
		}
	}

	schema := arrow.NewSchema(fields, nil)

	// 分批处理数据
	batchSize := dr.batchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	var rowCount int
	var batch [][]interface{}

	for rows.Next() {
		// 创建扫描目标
		values := make([]interface{}, len(columnTypes))
		valuePtrs := make([]interface{}, len(columnTypes))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		batch = append(batch, values)
		rowCount++

		// 达到批次大小时处理
		if len(batch) >= batchSize {
			if err := dr.processBatchToArrow(schema, batch, callback); err != nil {
				return fmt.Errorf("failed to process batch: %w", err)
			}
			batch = batch[:0] // 重置批次
		}
	}

	// 处理剩余数据
	if len(batch) > 0 {
		if err := dr.processBatchToArrow(schema, batch, callback); err != nil {
			return fmt.Errorf("failed to process final batch: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %w", err)
	}

	dr.client.logger.Infof("Processed %d rows from ClickHouse", rowCount)
	return nil
}

// CalculateChecksum 计算表数据的校验和
func (c *Client) CalculateChecksum(ctx context.Context, tableName string, columns []string, whereClause string) (string, error) {
	var selectColumns string
	if len(columns) > 0 {
		// 将指定列连接后计算校验和
		selectColumns = fmt.Sprintf("cityHash64(concat(%s))", strings.Join(columns, ", "))
	} else {
		// 计算所有列的校验和
		selectColumns = "cityHash64(*)"
	}

	query := fmt.Sprintf("SELECT sum(%s) FROM %s", selectColumns, tableName)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	var checksum sql.NullString
	err := c.db.QueryRowContext(ctx, query).Scan(&checksum)
	if err != nil {
		return "", fmt.Errorf("failed to calculate checksum: %w", err)
	}

	if !checksum.Valid {
		return "0", nil
	}

	return checksum.String, nil
}

// TestConnection 测试连接
func (c *Client) TestConnection(ctx context.Context) error {
	return c.db.PingContext(ctx)
}

// GetServerVersion 获取服务器版本
func (c *Client) GetServerVersion(ctx context.Context) (string, error) {
	var version string
	err := c.db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("failed to get server version: %w", err)
	}
	return version, nil
}

// mapClickHouseTypeToArrow 将ClickHouse类型映射到Arrow类型
func (dr *ArrowStreamReader) mapClickHouseTypeToArrow(ct *sql.ColumnType) (arrow.DataType, error) {
	typeName := strings.ToLower(ct.DatabaseTypeName())

	switch {
	case strings.Contains(typeName, "int8"):
		return arrow.PrimitiveTypes.Int8, nil
	case strings.Contains(typeName, "int16"):
		return arrow.PrimitiveTypes.Int16, nil
	case strings.Contains(typeName, "int32"):
		return arrow.PrimitiveTypes.Int32, nil
	case strings.Contains(typeName, "int64"):
		return arrow.PrimitiveTypes.Int64, nil
	case strings.Contains(typeName, "uint8"):
		return arrow.PrimitiveTypes.Uint8, nil
	case strings.Contains(typeName, "uint16"):
		return arrow.PrimitiveTypes.Uint16, nil
	case strings.Contains(typeName, "uint32"):
		return arrow.PrimitiveTypes.Uint32, nil
	case strings.Contains(typeName, "uint64"):
		return arrow.PrimitiveTypes.Uint64, nil
	case strings.Contains(typeName, "float32"):
		return arrow.PrimitiveTypes.Float32, nil
	case strings.Contains(typeName, "float64"):
		return arrow.PrimitiveTypes.Float64, nil
	case strings.Contains(typeName, "string") || strings.Contains(typeName, "fixedstring"):
		return arrow.BinaryTypes.String, nil
	case strings.Contains(typeName, "date"):
		return arrow.FixedWidthTypes.Date32, nil
	case strings.Contains(typeName, "datetime"):
		return arrow.FixedWidthTypes.Timestamp_s, nil
	case strings.Contains(typeName, "bool"):
		return arrow.FixedWidthTypes.Boolean, nil
	default:
		// 默认使用字符串类型
		return arrow.BinaryTypes.String, nil
	}
}

// processBatchToArrow 将批次数据转换为Arrow Record
func (dr *ArrowStreamReader) processBatchToArrow(schema *arrow.Schema, batch [][]interface{}, callback func(record arrow.Record) error) error {
	if len(batch) == 0 {
		return nil
	}

	mem := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	// 为每一列构建数据
	for colIdx, field := range schema.Fields() {
		colBuilder := builder.Field(colIdx)

		for rowIdx := 0; rowIdx < len(batch); rowIdx++ {
			value := batch[rowIdx][colIdx]

			if err := dr.appendValueToBuilder(colBuilder, value, field.Type); err != nil {
				dr.client.logger.Warnf("Failed to append value for column %s: %v, using null", field.Name, err)
				colBuilder.AppendNull()
			}
		}
	}

	// 创建Arrow Record
	record := builder.NewRecord()
	defer record.Release()

	// 调用回调函数处理记录
	return callback(record)
}

// appendValueToBuilder 将值添加到Arrow builder中
func (dr *ArrowStreamReader) appendValueToBuilder(builder array.Builder, value interface{}, dataType arrow.DataType) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch b := builder.(type) {
	case *array.StringBuilder:
		if v, ok := dr.convertToString(value); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %v to string", value)
		}
	case *array.Int64Builder:
		if v, ok := dr.convertToInt64(value); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %v to int64", value)
		}
	case *array.Float64Builder:
		if v, ok := dr.convertToFloat64(value); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %v to float64", value)
		}
	case *array.BooleanBuilder:
		if v, ok := dr.convertToBool(value); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %v to bool", value)
		}
	case *array.TimestampBuilder:
		if v, ok := dr.convertToTimestamp(value); ok {
			b.Append(v)
		} else {
			return fmt.Errorf("cannot convert %v to timestamp", value)
		}
	default:
		// 默认转换为字符串
		if sb, ok := builder.(*array.StringBuilder); ok {
			if v, ok := dr.convertToString(value); ok {
				sb.Append(v)
			} else {
				return fmt.Errorf("cannot convert %v to string", value)
			}
		} else {
			return fmt.Errorf("unsupported builder type: %T", builder)
		}
	}

	return nil
}

// 类型转换辅助方法
func (dr *ArrowStreamReader) convertToString(value interface{}) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	case nil:
		return "", true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

func (dr *ArrowStreamReader) convertToInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int16:
		return int64(v), true
	case int8:
		return int64(v), true
	case int:
		return int64(v), true
	case uint64:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint:
		return int64(v), true
	default:
		return 0, false
	}
}

func (dr *ArrowStreamReader) convertToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case int:
		return float64(v), true
	default:
		return 0, false
	}
}

func (dr *ArrowStreamReader) convertToBool(value interface{}) (bool, bool) {
	switch v := value.(type) {
	case bool:
		return v, true
	case int:
		return v != 0, true
	case int64:
		return v != 0, true
	case string:
		return v == "true" || v == "1", true
	default:
		return false, false
	}
}

func (dr *ArrowStreamReader) convertToTimestamp(value interface{}) (arrow.Timestamp, bool) {
	switch v := value.(type) {
	case time.Time:
		return arrow.Timestamp(v.Unix()), true
	case string:
		if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
			return arrow.Timestamp(t.Unix()), true
		}
		return 0, false
	default:
		return 0, false
	}
}
