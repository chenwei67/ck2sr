package starrocks

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/sirupsen/logrus"
)

// Client StarRocks 客户端
type Client struct {
	db           *sql.DB
	config       *config.StarRocksConfig
	logger       *logrus.Logger
	streamLoadURL string
	httpClient   *http.Client
}

// NewClient 创建新的 StarRocks 客户端
func NewClient(cfg *config.StarRocksConfig, logger *logrus.Logger) (*Client, error) {
	if logger == nil {
		logger = logrus.New()
	}

	// 构建 MySQL 连接字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&timeout=30s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)

	// 建立数据库连接
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open StarRocks connection: %w", err)
	}

	// 设置连接池参数
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping StarRocks: %w", err)
	}

	// 构建 Stream Load URL
	streamLoadURL := cfg.StreamLoadURL
	if streamLoadURL == "" {
		streamLoadURL = fmt.Sprintf("http://%s:8030", cfg.Host)
	}

	logger.Infof("Connected to StarRocks at %s:%d, database: %s", cfg.Host, cfg.Port, cfg.Database)

	return &Client{
		db:            db,
		config:        cfg,
		logger:        logger,
		streamLoadURL: streamLoadURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Minute,
		},
	}, nil
}

// Close 关闭连接
func (c *Client) Close() error {
	return c.db.Close()
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
	DefaultValue string
	Comment      string
	IsNullable   bool
	IsPrimaryKey bool
}

// GetTableInfo 获取表信息
func (c *Client) GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	// 获取表基本信息
	query := `
		SELECT
			TABLE_NAME,
			ENGINE,
			TABLE_ROWS,
			DATA_LENGTH,
			CREATE_TIME,
			TABLE_COMMENT
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`

	var info TableInfo
	var dataLength sql.NullInt64
	var createTime sql.NullTime

	err := c.db.QueryRowContext(ctx, query, c.config.Database, tableName).Scan(
		&info.Name,
		&info.Engine,
		&info.TotalRows,
		&dataLength,
		&createTime,
		&info.Comment,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("table %s not found", tableName)
		}
		return nil, fmt.Errorf("failed to get table info: %w", err)
	}

	if dataLength.Valid {
		info.TotalBytes = dataLength.Int64
	}
	if createTime.Valid {
		info.CreateTime = createTime.Time
	}

	// 获取列信息
	columns, err := c.getTableColumns(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table columns: %w", err)
	}
	info.Columns = columns

	return &info, nil
}

// getTableColumns 获取表列信息
func (c *Client) getTableColumns(ctx context.Context, tableName string) ([]ColumnInfo, error) {
	query := `
		SELECT
			COLUMN_NAME,
			DATA_TYPE,
			COLUMN_DEFAULT,
			COLUMN_COMMENT,
			IS_NULLABLE,
			COLUMN_KEY
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`

	rows, err := c.db.QueryContext(ctx, query, c.config.Database, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var defaultValue sql.NullString
		var isNullable, columnKey string

		err := rows.Scan(
			&col.Name,
			&col.Type,
			&defaultValue,
			&col.Comment,
			&isNullable,
			&columnKey,
		)
		if err != nil {
			return nil, err
		}

		if defaultValue.Valid {
			col.DefaultValue = defaultValue.String
		}

		col.IsNullable = isNullable == "YES"
		col.IsPrimaryKey = columnKey == "PRI"

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

// StreamLoadResult Stream Load 结果
type StreamLoadResult struct {
	TxnID        int64  `json:"TxnId"`
	Label        string `json:"Label"`
	Status       string `json:"Status"`
	Message      string `json:"Message"`
	NumberTotalRows    int64  `json:"NumberTotalRows"`
	NumberLoadedRows   int64  `json:"NumberLoadedRows"`
	NumberFilteredRows int64  `json:"NumberFilteredRows"`
	NumberUnselectedRows int64 `json:"NumberUnselectedRows"`
	LoadBytes     int64  `json:"LoadBytes"`
	LoadTimeMs    int64  `json:"LoadTimeMs"`
	BeginTxnTimeMs int64 `json:"BeginTxnTimeMs"`
	StreamLoadPutTimeMs int64 `json:"StreamLoadPutTimeMs"`
	ReadDataTimeMs int64 `json:"ReadDataTimeMs"`
	WriteDataTimeMs int64 `json:"WriteDataTimeMs"`
	CommitAndPublishTimeMs int64 `json:"CommitAndPublishTimeMs"`
}

// StreamLoad 执行 Stream Load
func (c *Client) StreamLoad(ctx context.Context, tableName string, data io.Reader, props map[string]string) (*StreamLoadResult, error) {
	// 构建 URL
	loadURL := fmt.Sprintf("%s/api/%s/%s/_stream_load", c.streamLoadURL, c.config.Database, tableName)

	// 创建请求
	req, err := http.NewRequestWithContext(ctx, "PUT", loadURL, data)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream load request: %w", err)
	}

	// 设置认证
	req.SetBasicAuth(c.config.Username, c.config.Password)

	// 设置请求头
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Expect", "100-continue")

	// 合并默认属性和自定义属性
	loadProps := make(map[string]string)
	for k, v := range c.config.StreamLoadProps {
		loadProps[k] = v
	}
	for k, v := range props {
		loadProps[k] = v
	}

	// 设置 Stream Load 属性
	for key, value := range loadProps {
		req.Header.Set(key, value)
	}

	c.logger.Debugf("Stream Load URL: %s", loadURL)
	c.logger.Debugf("Stream Load headers: %v", req.Header)

	// 发送请求
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute stream load: %w", err)
	}
	defer resp.Body.Close()

	// 读取响应
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read stream load response: %w", err)
	}

	// 解析响应
	result, err := c.parseStreamLoadResponse(respBody)
	if err != nil {
		return nil, fmt.Errorf("failed to parse stream load response: %w", err)
	}

	// 检查状态
	if result.Status != "Success" {
		return result, fmt.Errorf("stream load failed: %s", result.Message)
	}

	c.logger.Infof("Stream Load completed: loaded %d rows, %d bytes in %dms",
		result.NumberLoadedRows, result.LoadBytes, result.LoadTimeMs)

	return result, nil
}

// parseStreamLoadResponse 解析 Stream Load 响应
func (c *Client) parseStreamLoadResponse(body []byte) (*StreamLoadResult, error) {
	// StarRocks Stream Load 返回的是类似 JSON 的格式，但不是标准 JSON
	// 需要手动解析
	result := &StreamLoadResult{}

	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || line == "{" || line == "}" {
			continue
		}

		// 移除末尾的逗号
		line = strings.TrimSuffix(line, ",")

		// 解析键值对
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.Trim(strings.TrimSpace(parts[0]), `"`)
		value := strings.Trim(strings.TrimSpace(parts[1]), `"`)

		switch key {
		case "TxnId":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.TxnID = v
			}
		case "Label":
			result.Label = value
		case "Status":
			result.Status = value
		case "Message":
			result.Message = value
		case "NumberTotalRows":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.NumberTotalRows = v
			}
		case "NumberLoadedRows":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.NumberLoadedRows = v
			}
		case "NumberFilteredRows":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.NumberFilteredRows = v
			}
		case "NumberUnselectedRows":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.NumberUnselectedRows = v
			}
		case "LoadBytes":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.LoadBytes = v
			}
		case "LoadTimeMs":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.LoadTimeMs = v
			}
		case "BeginTxnTimeMs":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.BeginTxnTimeMs = v
			}
		case "StreamLoadPutTimeMs":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.StreamLoadPutTimeMs = v
			}
		case "ReadDataTimeMs":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.ReadDataTimeMs = v
			}
		case "WriteDataTimeMs":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.WriteDataTimeMs = v
			}
		case "CommitAndPublishTimeMs":
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				result.CommitAndPublishTimeMs = v
			}
		}
	}

	return result, nil
}

// StreamLoadFromCSV 从 CSV 数据执行 Stream Load
func (c *Client) StreamLoadFromCSV(ctx context.Context, tableName string, csvData [][]string, props map[string]string) (*StreamLoadResult, error) {
	// 将 CSV 数据转换为字符串
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)

	for _, record := range csvData {
		if err := writer.Write(record); err != nil {
			return nil, fmt.Errorf("failed to write CSV record: %w", err)
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	return c.StreamLoad(ctx, tableName, &buf, props)
}

// DataWriter 数据写入器
type DataWriter struct {
	client      *Client
	tableName   string
	columnMapping map[string]string
	buffer      *bytes.Buffer
	writer      *csv.Writer
	rowCount    int
	batchSize   int
}

// NewDataWriter 创建数据写入器
func (c *Client) NewDataWriter(tableName string, batchSize int) *DataWriter {
	buf := &bytes.Buffer{}
	return &DataWriter{
		client:    c,
		tableName: tableName,
		buffer:    buf,
		writer:    csv.NewWriter(buf),
		batchSize: batchSize,
	}
}

// WithColumnMapping 设置列映射
func (dw *DataWriter) WithColumnMapping(mapping map[string]string) *DataWriter {
	dw.columnMapping = mapping
	return dw
}

// WriteRow 写入一行数据
func (dw *DataWriter) WriteRow(row []string) error {
	if err := dw.writer.Write(row); err != nil {
		return fmt.Errorf("failed to write row: %w", err)
	}

	dw.rowCount++
	return nil
}

// Flush 刷新缓冲区并执行 Stream Load
func (dw *DataWriter) Flush(ctx context.Context) (*StreamLoadResult, error) {
	if dw.rowCount == 0 {
		return nil, nil
	}

	dw.writer.Flush()
	if err := dw.writer.Error(); err != nil {
		return nil, fmt.Errorf("failed to flush CSV writer: %w", err)
	}

	// 执行 Stream Load
	result, err := dw.client.StreamLoad(ctx, dw.tableName, dw.buffer, nil)
	if err != nil {
		return nil, err
	}

	// 重置缓冲区
	dw.buffer.Reset()
	dw.writer = csv.NewWriter(dw.buffer)
	dw.rowCount = 0

	return result, nil
}

// ShouldFlush 检查是否应该刷新
func (dw *DataWriter) ShouldFlush() bool {
	return dw.rowCount >= dw.batchSize
}

// GetRowCount 获取当前缓冲的行数
func (dw *DataWriter) GetRowCount() int {
	return dw.rowCount
}

// CalculateChecksum 计算表数据的校验和
func (c *Client) CalculateChecksum(ctx context.Context, tableName string, columns []string, whereClause string) (string, error) {
	var selectColumns string
	if len(columns) > 0 {
		// 将指定列连接后计算校验和
		selectColumns = fmt.Sprintf("CRC32(CONCAT(%s))", strings.Join(columns, ", "))
	} else {
		// 计算所有列的校验和（简化实现）
		selectColumns = "CRC32(CONCAT(*))"
	}

	query := fmt.Sprintf("SELECT SUM(%s) FROM %s", selectColumns, tableName)
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
	err := c.db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("failed to get server version: %w", err)
	}
	return version, nil
}