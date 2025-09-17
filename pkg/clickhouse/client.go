package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/sirupsen/logrus"
)

// Client ClickHouse 客户端
type Client struct {
	conn   driver.Conn
	config *config.ClickHouseConfig
	logger *logrus.Logger
}

// NewClient 创建新的 ClickHouse 客户端
func NewClient(cfg *config.ClickHouseConfig, logger *logrus.Logger) (*Client, error) {
	if logger == nil {
		logger = logrus.New()
	}

	// 构建连接选项
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Debugf: func(format string, v ...interface{}) {
			logger.Debugf("[ClickHouse] "+format, v...)
		},
		Settings: clickhouse.Settings{
			"max_block_size": cfg.MaxBlockSize,
		},
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		DialTimeout:     30 * time.Second,
		ReadTimeout:     cfg.ReadTimeout,
	}

	// 建立连接
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	logger.Infof("Connected to ClickHouse at %s:%d, database: %s", cfg.Host, cfg.Port, cfg.Database)

	return &Client{
		conn:   conn,
		config: cfg,
		logger: logger,
	}, nil
}

// Close 关闭连接
func (c *Client) Close() error {
	return c.conn.Close()
}

// TableInfo 表信息
type TableInfo struct {
	Name        string
	Engine      string
	TotalRows   int64
	TotalBytes  int64
	Columns     []ColumnInfo
	CreateTime  time.Time
	Comment     string
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
	err := c.conn.QueryRow(ctx, query, c.config.Database, tableName).Scan(
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

	rows, err := c.conn.Query(ctx, query, c.config.Database, tableName)
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
	err := c.conn.QueryRow(ctx, query).Scan(&count)
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
	err := c.conn.QueryRow(ctx, query).Scan(&minVal, &maxVal)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get min/max values: %w", err)
	}

	return minVal, maxVal, nil
}

// DataReader 数据读取器
type DataReader struct {
	client    *Client
	tableName string
	columns   []string
	whereClause string
	orderBy   string
	limit     int64
	offset    int64
}

// NewDataReader 创建数据读取器
func (c *Client) NewDataReader(tableName string) *DataReader {
	return &DataReader{
		client:    c,
		tableName: tableName,
	}
}

// WithColumns 设置要读取的列
func (dr *DataReader) WithColumns(columns []string) *DataReader {
	dr.columns = columns
	return dr
}

// WithWhere 设置 WHERE 条件
func (dr *DataReader) WithWhere(whereClause string) *DataReader {
	dr.whereClause = whereClause
	return dr
}

// WithOrderBy 设置排序
func (dr *DataReader) WithOrderBy(orderBy string) *DataReader {
	dr.orderBy = orderBy
	return dr
}

// WithLimit 设置限制和偏移
func (dr *DataReader) WithLimit(limit, offset int64) *DataReader {
	dr.limit = limit
	dr.offset = offset
	return dr
}

// Read 读取数据
func (dr *DataReader) Read(ctx context.Context) (driver.Rows, error) {
	// 构建查询语句
	var columns string
	if len(dr.columns) > 0 {
		columns = strings.Join(dr.columns, ", ")
	} else {
		columns = "*"
	}

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

	dr.client.logger.Debugf("Executing query: %s", query)

	return dr.client.conn.Query(ctx, query)
}

// ReadBatch 批量读取数据
func (dr *DataReader) ReadBatch(ctx context.Context, batchSize int64, callback func(rows driver.Rows) error) error {
	offset := dr.offset
	for {
		batchReader := &DataReader{
			client:      dr.client,
			tableName:   dr.tableName,
			columns:     dr.columns,
			whereClause: dr.whereClause,
			orderBy:     dr.orderBy,
			limit:       batchSize,
			offset:      offset,
		}

		rows, err := batchReader.Read(ctx)
		if err != nil {
			return fmt.Errorf("failed to read batch: %w", err)
		}

		hasData := false
		if err := callback(rows); err != nil {
			rows.Close()
			return fmt.Errorf("batch callback error: %w", err)
		}

		// 检查是否还有数据
		rows.Close()

		// 重新查询检查是否还有数据（简化实现）
		checkRows, err := batchReader.Read(ctx)
		if err != nil {
			return fmt.Errorf("failed to check remaining data: %w", err)
		}

		if checkRows.Next() {
			hasData = true
		}
		checkRows.Close()

		if !hasData {
			break
		}

		offset += batchSize

		// 检查是否超过总限制
		if dr.limit > 0 && offset >= dr.offset+dr.limit {
			break
		}
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
	err := c.conn.QueryRow(ctx, query).Scan(&checksum)
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
	return c.conn.Ping(ctx)
}

// GetServerVersion 获取服务器版本
func (c *Client) GetServerVersion(ctx context.Context) (string, error) {
	var version string
	err := c.conn.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("failed to get server version: %w", err)
	}
	return version, nil
}