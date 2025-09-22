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

	// 构建数据源名称 (DSN)
	dsn := fmt.Sprintf("tcp://%s:%d/%s?username=%s&password=%s",
		cfg.Host, cfg.Port, cfg.Database, cfg.Username, cfg.Password)

	// 移除不兼容的连接参数，避免 "Unknown setting" 错误
	// 这些参数在某些ClickHouse版本中不被支持

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", err)
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
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	client := &Client{
		db:        db,
		config:    cfg,
		logger:    logger,
		allocator: memory.NewGoAllocator(),
	}

	logger.Infof("Connected to ClickHouse at %s:%d", cfg.Host, cfg.Port)
	return client, nil
}

// Close 关闭连接
func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// ArrowStreamReader Arrow流式读取器 - 使用原生ArrowStream格式
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

// NewArrowStreamReader 创建新的Arrow流式读取器
func (c *Client) NewArrowStreamReader(tableName string) *ArrowStreamReader {
	return &ArrowStreamReader{
		client:    c,
		tableName: tableName,
		batchSize: c.config.BatchSize,
	}
}

// WithColumns 指定要查询的列
func (dr *ArrowStreamReader) WithColumns(columns []string) *ArrowStreamReader {
	dr.columns = columns
	return dr
}

// WithWhere 添加WHERE条件
func (dr *ArrowStreamReader) WithWhere(whereClause string) *ArrowStreamReader {
	dr.whereClause = whereClause
	return dr
}

// WithOrderBy 添加ORDER BY条件
func (dr *ArrowStreamReader) WithOrderBy(orderBy string) *ArrowStreamReader {
	dr.orderBy = orderBy
	return dr
}

// WithLimit 设置查询限制
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

// ReadArrowStream 直接使用ClickHouse的FORMAT ArrowStream功能读取原生Arrow数据流
func (dr *ArrowStreamReader) ReadArrowStream(ctx context.Context, callback func(record arrow.Record) error) error {
	// 构建使用FORMAT ArrowStream的查询语句
	var columns string
	if len(dr.columns) > 0 {
		columns = strings.Join(dr.columns, ", ")
	} else {
		columns = "*"
	}

	// 构建 ArrowStream 格式查询
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

	// 添加 FORMAT ArrowStream - 这是关键的零拷贝技术
	query += " FORMAT ArrowStream"

	dr.client.logger.Infof("Executing ClickHouse ArrowStream query: %s", query)

	// 先验证查询是否会有结果 - 使用COUNT查询检查
	if err := dr.validateQueryHasData(ctx, columns); err != nil {
		return err
	}

	// 执行查询并获取原生Arrow数据流
	rows, err := dr.client.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute ClickHouse ArrowStream query: %w", err)
	}
	defer rows.Close()

	// 处理ArrowStream数据 - 直接读取Arrow二进制流
	return dr.processArrowStream(ctx, rows, callback)
}

// validateQueryHasData 验证查询是否有数据
func (dr *ArrowStreamReader) validateQueryHasData(ctx context.Context, columns string) error {
	// 构建COUNT查询来验证是否有数据
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", dr.tableName)

	if dr.whereClause != "" {
		countQuery += " WHERE " + dr.whereClause
	}

	dr.client.logger.Debugf("Validating data exists with query: %s", countQuery)

	var rowCount int64
	err := dr.client.db.QueryRowContext(ctx, countQuery).Scan(&rowCount)
	if err != nil {
		return fmt.Errorf("failed to validate data existence: %w", err)
	}

	dr.client.logger.Infof("Query validation: found %d rows matching criteria", rowCount)

	if rowCount == 0 {
		dr.client.logger.Warn("No data found matching the query criteria")
		return fmt.Errorf("no data found for the specified criteria")
	}

	// 验证 ClickHouse 是否支持 ArrowStream 格式
	if err := dr.validateArrowStreamSupport(ctx); err != nil {
		return err
	}

	return nil
}

// validateArrowStreamSupport 验证ClickHouse是否支持ArrowStream格式
func (dr *ArrowStreamReader) validateArrowStreamSupport(ctx context.Context) error {
	// 尝试执行一个简单的ArrowStream查询来验证支持
	testQuery := "SELECT 1 as test_col FORMAT ArrowStream"

	dr.client.logger.Debug("Testing ClickHouse ArrowStream support")

	rows, err := dr.client.db.QueryContext(ctx, testQuery)
	if err != nil {
		if strings.Contains(err.Error(), "Unknown format") ||
		   strings.Contains(err.Error(), "UNKNOWN_FORMAT") ||
		   strings.Contains(err.Error(), "ArrowStream") {
			return fmt.Errorf("ClickHouse does not support ArrowStream format. Please upgrade to ClickHouse 21.12+")
		}
		return fmt.Errorf("failed to test ArrowStream support: %w", err)
	}
	defer rows.Close()

	// 检查是否能获取到测试数据
	hasData := false
	for rows.Next() {
		var testData []byte
		if err := rows.Scan(&testData); err != nil {
			return fmt.Errorf("failed to scan test ArrowStream data: %w", err)
		}

		if len(testData) > 8 {
			hasData = true
			dr.client.logger.Debug("ClickHouse ArrowStream support confirmed")
			break
		}
	}

	if !hasData {
		return fmt.Errorf("ClickHouse ArrowStream returns invalid data format")
	}

	return nil
}

// processArrowStream 处理ClickHouse返回的原生ArrowStream数据
func (dr *ArrowStreamReader) processArrowStream(ctx context.Context, rows *sql.Rows, callback func(record arrow.Record) error) error {
	// ClickHouse的FORMAT ArrowStream可能返回多行，每行包含Arrow数据块
	recordCount := 0
	totalRows := int64(0)
	processedBlocks := 0

	defer func() {
		dr.client.logger.Infof("ArrowStream processing completed: %d blocks, %d records, ~%d rows",
			processedBlocks, recordCount, totalRows)
	}()

	for rows.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var arrowData []byte
		if err := rows.Scan(&arrowData); err != nil {
			return fmt.Errorf("failed to scan ArrowStream data: %w", err)
		}

		processedBlocks++

		if len(arrowData) == 0 {
			dr.client.logger.Debug("Received empty ArrowStream data block")
			continue
		}

		dr.client.logger.Debugf("Processing ArrowStream data block %d: %d bytes", processedBlocks, len(arrowData))

		// 尝试处理这个Arrow数据块，增加错误容忍度
		blockRecords, err := dr.processArrowDataBlockWithRetry(arrowData, callback)
		if err != nil {
			// 根据错误类型决定是否继续处理
			if dr.shouldSkipBlock(err) {
				dr.client.logger.Warnf("Skipping problematic Arrow block %d: %v", processedBlocks, err)
				continue
			}
			return fmt.Errorf("failed to process Arrow data block %d: %w", processedBlocks, err)
		}

		recordCount += blockRecords
		if blockRecords > 0 {
			// 估算行数（这是一个近似值）
			estimatedRows := int64(blockRecords * dr.batchSize)
			totalRows += estimatedRows
		}

		dr.client.logger.Debugf("Block %d processed successfully: %d records", processedBlocks, blockRecords)
	}

	// 检查扫描过程中是否有错误
	if err := rows.Err(); err != nil {
		return fmt.Errorf("error scanning ArrowStream rows: %w", err)
	}

	if processedBlocks == 0 {
		dr.client.logger.Warn("No ArrowStream data blocks received from ClickHouse")
		return fmt.Errorf("no data received from ClickHouse ArrowStream query")
	}

	return nil
}

// processArrowDataBlockWithRetry 带重试的Arrow数据块处理
func (dr *ArrowStreamReader) processArrowDataBlockWithRetry(arrowData []byte, callback func(record arrow.Record) error) (int, error) {
	// 第一次尝试正常处理
	recordCount, err := dr.processArrowDataBlock(arrowData, callback)
	if err == nil {
		return recordCount, nil
	}

	// 如果失败，尝试一些恢复策略
	dr.client.logger.Debugf("First attempt failed: %v, trying recovery strategies", err)

	// 策略1: 尝试跳过可能损坏的头部字节
	if len(arrowData) > 32 {
		for offset := 4; offset <= 16; offset += 4 {
			dr.client.logger.Debugf("Trying offset %d bytes", offset)
			if recordCount, err2 := dr.processArrowDataBlock(arrowData[offset:], callback); err2 == nil {
				dr.client.logger.Infof("Successfully recovered Arrow data with %d bytes offset", offset)
				return recordCount, nil
			}
		}
	}

	// 策略2: 尝试从后面截断可能损坏的尾部字节
	if len(arrowData) > 32 {
		for cutOff := 4; cutOff <= 16; cutOff += 4 {
			newLen := len(arrowData) - cutOff
			if newLen > 8 {
				dr.client.logger.Debugf("Trying with %d bytes cut off from end", cutOff)
				if recordCount, err2 := dr.processArrowDataBlock(arrowData[:newLen], callback); err2 == nil {
					dr.client.logger.Infof("Successfully recovered Arrow data by cutting %d bytes from end", cutOff)
					return recordCount, nil
				}
			}
		}
	}

	// 所有恢复策略都失败
	return 0, fmt.Errorf("failed to process Arrow data block after recovery attempts: %w", err)
}

// shouldSkipBlock 判断是否应该跳过有问题的数据块
func (dr *ArrowStreamReader) shouldSkipBlock(err error) bool {
	errStr := err.Error()

	// 这些错误通常表示数据格式问题，可以跳过单个块
	skipPatterns := []string{
		"unexpected EOF",
		"invalid Arrow IPC format",
		"could not read message schema",
		"could not read message metadata",
		"Arrow data block too short",
	}

	for _, pattern := range skipPatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// processArrowDataBlock 处理单个Arrow数据块
func (dr *ArrowStreamReader) processArrowDataBlock(arrowData []byte, callback func(record arrow.Record) error) (int, error) {
	// 检查数据块是否为空或过短
	if len(arrowData) == 0 {
		dr.client.logger.Debug("Received empty Arrow data block, skipping")
		return 0, nil
	}

	// ClickHouse ArrowStream 数据块的最小长度检查
	// Arrow IPC 消息至少需要 8 字节的头部信息
	if len(arrowData) < 8 {
		dr.client.logger.Debugf("Arrow data block too short (%d bytes), may be incomplete", len(arrowData))
		return 0, nil
	}

	dr.client.logger.Debugf("Processing Arrow data block: %d bytes", len(arrowData))

	// 检查 Arrow IPC 消息头部是否有效
	// Arrow IPC 格式以特定的魔数开始
	if !dr.isValidArrowData(arrowData) {
		dr.client.logger.Warnf("Invalid Arrow IPC format detected, data length: %d", len(arrowData))
		return 0, fmt.Errorf("invalid Arrow IPC format in data block")
	}

	// 创建 Arrow IPC Reader 来解析数据流
	reader, err := ipc.NewReader(bytes.NewReader(arrowData))
	if err != nil {
		dr.client.logger.Errorf("Failed to create Arrow IPC reader for %d bytes: %v", len(arrowData), err)
		// 详细错误分析
		if len(arrowData) > 16 {
			dr.client.logger.Debugf("Data preview (first 16 bytes): %x", arrowData[:16])
		}
		return 0, fmt.Errorf("failed to create Arrow IPC reader: %w", err)
	}
	defer reader.Release()

	// 逐个读取 Arrow Record 并调用回调函数
	recordCount := 0
	for reader.Next() {
		record := reader.Record()
		if record == nil {
			dr.client.logger.Warn("Received nil record from Arrow reader")
			continue
		}

		record.Retain() // 增加引用计数，确保在回调中安全使用

		recordCount++
		dr.client.logger.Debugf("Processing Arrow record %d with %d rows", recordCount, record.NumRows())

		// 调用回调函数处理记录
		if err := callback(record); err != nil {
			record.Release() // 在错误情况下释放
			return recordCount, fmt.Errorf("callback failed for record %d: %w", recordCount, err)
		}

		record.Release() // 处理完成后释放
	}

	// 检查读取过程中是否有错误
	if err := reader.Err(); err != nil {
		dr.client.logger.Errorf("Error during Arrow record reading: %v", err)
		return recordCount, fmt.Errorf("error reading Arrow records: %w", err)
	}

	if recordCount == 0 {
		dr.client.logger.Debug("No valid Arrow records found in data block")
	} else {
		dr.client.logger.Debugf("Successfully processed %d Arrow records", recordCount)
	}

	return recordCount, nil
}

// isValidArrowData 检查数据是否为有效的 Arrow IPC 格式
func (dr *ArrowStreamReader) isValidArrowData(data []byte) bool {
	if len(data) < 8 {
		return false
	}

	// 记录数据的十六进制内容用于调试
	if len(data) <= 16 {
		dr.client.logger.Debugf("Small data block content: %x (length: %d)", data, len(data))
	} else {
		dr.client.logger.Debugf("Data block preview: %x... (length: %d)", data[:16], len(data))
	}

	// 特殊处理：8字节的数据通常表示空结果或错误
	if len(data) == 8 {
		// 检查是否是ClickHouse返回的特殊标识
		if dr.isClickHouseEmptyResult(data) {
			dr.client.logger.Debug("Detected ClickHouse empty result marker")
			return false
		}

		// 检查是否是错误代码
		if dr.isClickHouseError(data) {
			dr.client.logger.Warn("Detected ClickHouse error marker in 8-byte data")
			return false
		}
	}

	// Arrow IPC 流格式检查
	// Arrow IPC消息的基本结构验证

	// 检查前4个字节是否可能是长度字段（小端序）
	messageLen := int(data[0]) | int(data[1])<<8 | int(data[2])<<16 | int(data[3])<<24

	// 合理的消息长度范围检查
	if messageLen <= 0 || messageLen > len(data) || messageLen > 100*1024*1024 { // 最大100MB
		dr.client.logger.Debugf("Invalid message length in Arrow data: %d (data size: %d)", messageLen, len(data))
		return false
	}

	// 检查是否有Arrow IPC的魔数标识
	// Arrow IPC流通常包含特定的标识字节
	if len(data) >= 8 {
		// 简单的启发式检查，检查是否包含可能的Arrow格式标识
		// 这里可以根据Arrow IPC的实际格式进行更严格的验证
		return true
	}

	return true
}

// isClickHouseEmptyResult 检查是否是ClickHouse的空结果标识
func (dr *ArrowStreamReader) isClickHouseEmptyResult(data []byte) bool {
	if len(data) != 8 {
		return false
	}

	// 检查常见的空结果模式
	// 全零
	allZero := true
	for _, b := range data {
		if b != 0 {
			allZero = false
			break
		}
	}

	if allZero {
		dr.client.logger.Debug("Detected all-zero 8-byte pattern (likely empty result)")
		return true
	}

	// 检查其他可能的空结果模式
	// 例如：长度字段为0的情况
	if data[0] == 0 && data[1] == 0 && data[2] == 0 && data[3] == 0 {
		dr.client.logger.Debug("Detected zero-length message pattern")
		return true
	}

	return false
}

// isClickHouseError 检查是否是ClickHouse的错误标识
func (dr *ArrowStreamReader) isClickHouseError(data []byte) bool {
	if len(data) != 8 {
		return false
	}

	// 检查是否包含错误码模式
	// 例如：负数或特殊的错误标识
	firstInt := int32(data[0]) | int32(data[1])<<8 | int32(data[2])<<16 | int32(data[3])<<24
	secondInt := int32(data[4]) | int32(data[5])<<8 | int32(data[6])<<16 | int32(data[7])<<24

	// 检查是否是错误码（通常是负数或特定值）
	if firstInt < 0 || secondInt < 0 {
		dr.client.logger.Debugf("Detected potential error codes in 8-byte data: %d, %d", firstInt, secondInt)
		return true
	}

	return false
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

	return &info, nil
}

// getTableColumns 获取表列信息
func (c *Client) getTableColumns(ctx context.Context, tableName string) ([]ColumnInfo, error) {
	query := `
		SELECT
			name,
			type,
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
		var defaultExpr sql.NullString
		var isPrimaryKey uint8

		err := rows.Scan(
			&col.Name,
			&col.Type,
			&defaultExpr,
			&col.Comment,
			&isPrimaryKey,
		)
		if err != nil {
			return nil, err
		}

		if defaultExpr.Valid {
			col.DefaultValue = defaultExpr.String
		}
		col.IsPrimaryKey = isPrimaryKey == 1
		// ClickHouse中大多数列都可以为NULL，除非明确指定
		col.IsNullable = true

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

// CalculateChecksum 计算表数据的校验和
func (c *Client) CalculateChecksum(ctx context.Context, tableName string, columns []string, whereClause string) (string, error) {
	var selectColumns string
	if len(columns) > 0 {
		// 将指定列连接后计算校验和
		selectColumns = fmt.Sprintf("CRC32(concat(%s))", strings.Join(columns, ", "))
	} else {
		// 计算所有列的校验和（简化实现）
		selectColumns = "CRC32(*)"
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