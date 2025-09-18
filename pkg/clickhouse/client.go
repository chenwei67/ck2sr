package clickhouse

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/ipc"
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
	if cfg.ReadTimeout > 0 {
		dsn += fmt.Sprintf("&read_timeout=%s", cfg.ReadTimeout.String())
	}
	if cfg.WriteTimeout > 0 {
		dsn += fmt.Sprintf("&write_timeout=%s", cfg.WriteTimeout.String())
	}

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

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

// ReadArrowStream 使用TCP连接读取Arrow格式数据流
func (dr *ArrowStreamReader) ReadArrowStream(ctx context.Context, callback func(record arrow.Record) error) error {
	// 构建查询语句
	var columns string
	if len(dr.columns) > 0 {
		columns = strings.Join(dr.columns, ", ")
	} else {
		columns = "*"
	}

	// 构建查询，使用FORMAT ArrowStream
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

	// 关键：使用ArrowStream格式输出
	query += " FORMAT ArrowStream"

	dr.client.logger.Infof("Executing ClickHouse ArrowStream query: %s", query)

	// 执行查询获取ArrowStream数据
	rows, err := dr.client.db.QueryContext(ctx, query)
	if err != nil {
		// 检查是否是ArrowStream格式不支持的错误
		if strings.Contains(err.Error(), "Unknown format") ||
		   strings.Contains(err.Error(), "ArrowStream") {
			return fmt.Errorf("ClickHouse does not support ArrowStream format. Please ensure you are using ClickHouse 21.12+ or consider using CSV format as fallback: %w", err)
		}
		return fmt.Errorf("failed to execute ClickHouse query: %w", err)
	}
	defer rows.Close()

	// 读取ArrowStream数据
	for rows.Next() {
		var arrowData []byte
		if err := rows.Scan(&arrowData); err != nil {
			return fmt.Errorf("failed to scan arrow data: %w", err)
		}

		// 解析ArrowStream数据
		if err := dr.processArrowStreamData(arrowData, callback); err != nil {
			return fmt.Errorf("failed to process arrow stream data: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration error: %w", err)
	}

	return nil
}

// processArrowStreamData 处理ArrowStream数据
func (dr *ArrowStreamReader) processArrowStreamData(data []byte, callback func(record arrow.Record) error) error {
	if len(data) == 0 {
		return nil
	}

	// 创建内存读取器
	buf := bytes.NewReader(data)
	reader, err := ipc.NewReader(buf)
	if err != nil {
		return fmt.Errorf("failed to create arrow IPC reader: %w", err)
	}
	defer reader.Release()

	// 读取所有记录
	for reader.Next() {
		record := reader.Record()
		defer record.Release()

		// 调用回调函数处理记录
		if err := callback(record); err != nil {
			return fmt.Errorf("callback error: %w", err)
		}
	}

	if err := reader.Err(); err != nil {
		return fmt.Errorf("arrow reader error: %w", err)
	}

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
