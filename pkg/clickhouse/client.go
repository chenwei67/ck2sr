package clickhouse

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// Client ClickHouse 客户端
type Client struct {
	db           *sql.DB
	flightClient flight.Client
	sqlClient    *flightsql.Client
	config       *config.ClickHouseConfig
	logger       *logrus.Logger
	allocator    memory.Allocator
}

// NewClient 创建新的 ClickHouse 客户端
func NewClient(cfg *config.ClickHouseConfig, logger *logrus.Logger) (*Client, error) {
	if logger == nil {
		logger = logrus.New()
	}

	// 创建内存分配器
	allocator := memory.NewGoAllocator()

	// 建立SQL连接用于元数据查询
	dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.Username, cfg.Password)
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse database connection: %w", err)
	}

	// 配置连接池
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	// 测试SQL连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	// 建立Flight SQL连接
	flightEndpoint := fmt.Sprintf("%s:%d", cfg.FlightSQLEndpoint, cfg.FlightSQLPort)
	var creds credentials.TransportCredentials
	if cfg.UseTLS {
		creds = credentials.NewTLS(nil)
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.Dial(flightEndpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to connect to ClickHouse Flight SQL: %w", err)
	}

	flightClient := flight.NewClientFromConn(conn, nil)
	sqlClient, err := flightsql.NewClient(flightEndpoint, nil, nil)
	if err != nil {
		conn.Close()
		db.Close()
		return nil, fmt.Errorf("failed to create Flight SQL client: %w", err)
	}

	logger.Infof("Connected to ClickHouse at %s:%d (SQL) and %s (Flight SQL), database: %s",
		cfg.Host, cfg.Port, flightEndpoint, cfg.Database)

	return &Client{
		db:           db,
		flightClient: flightClient,
		sqlClient:    sqlClient,
		config:       cfg,
		logger:       logger,
		allocator:    allocator,
	}, nil
}

// Close 关闭连接
func (c *Client) Close() error {
	var err error
	if c.flightClient != nil {
		if closeErr := c.flightClient.Close(); closeErr != nil {
			err = closeErr
		}
	}
	if c.db != nil {
		if closeErr := c.db.Close(); closeErr != nil {
			err = closeErr
		}
	}
	return err
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

// ArrowDataReader Arrow数据读取器
type ArrowDataReader struct {
	client       *Client
	tableName    string
	columns      []string
	whereClause  string
	orderBy      string
	limit        int64
	offset       int64
	batchSize    int
	columnMapping map[string]string
}

// NewArrowDataReader 创建Arrow数据读取器
func (c *Client) NewArrowDataReader(tableName string) *ArrowDataReader {
	return &ArrowDataReader{
		client:    c,
		tableName: tableName,
		batchSize: c.config.BatchSize,
	}
}

// WithColumns 设置要读取的列
func (dr *ArrowDataReader) WithColumns(columns []string) *ArrowDataReader {
	dr.columns = columns
	return dr
}

// WithWhere 设置 WHERE 条件
func (dr *ArrowDataReader) WithWhere(whereClause string) *ArrowDataReader {
	dr.whereClause = whereClause
	return dr
}

// WithOrderBy 设置排序
func (dr *ArrowDataReader) WithOrderBy(orderBy string) *ArrowDataReader {
	dr.orderBy = orderBy
	return dr
}

// WithLimit 设置限制和偏移
func (dr *ArrowDataReader) WithLimit(limit, offset int64) *ArrowDataReader {
	dr.limit = limit
	dr.offset = offset
	return dr
}

// WithBatchSize 设置批次大小
func (dr *ArrowDataReader) WithBatchSize(batchSize int) *ArrowDataReader {
	dr.batchSize = batchSize
	return dr
}

// WithColumnMapping 设置列映射
func (dr *ArrowDataReader) WithColumnMapping(mapping map[string]string) *ArrowDataReader {
	dr.columnMapping = mapping
	return dr
}

// ReadArrowBatch 使用Flight SQL读取Arrow格式数据
func (dr *ArrowDataReader) ReadArrowBatch(ctx context.Context, callback func(record arrow.Record) error) error {
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

	dr.client.logger.Debugf("Executing Flight SQL query: %s", query)

	// 使用Flight SQL执行查询
	ctxWithTimeout, cancel := context.WithTimeout(ctx, dr.client.config.FlightTimeout)
	defer cancel()

	info, err := dr.client.sqlClient.Execute(ctxWithTimeout, query)
	if err != nil {
		return fmt.Errorf("failed to execute Flight SQL query: %w", err)
	}

	// 读取数据流
	reader, err := dr.client.flightClient.DoGet(ctxWithTimeout, info.Endpoint[0].Ticket)
	if err != nil {
		return fmt.Errorf("failed to get Flight SQL data stream: %w", err)
	}
	// 从 Flight stream 读取数据
	for {
		msg, err := reader.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("flight stream error: %w", err)
		}

		// 解析 Arrow 数据
		buf := bytes.NewReader(msg.DataBody)
		ipcReader, err := ipc.NewReader(buf)
		if err != nil {
			return fmt.Errorf("failed to create IPC reader: %w", err)
		}
		defer ipcReader.Release()

		for ipcReader.Next() {
			record := ipcReader.Record()
			defer record.Release()

			// 调用回调函数处理记录
			if err := callback(record); err != nil {
				return fmt.Errorf("callback error: %w", err)
			}
		}

		if err := ipcReader.Err(); err != nil {
			return fmt.Errorf("IPC reader error: %w", err)
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