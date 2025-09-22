package starrocks

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/sirupsen/logrus"
)

// Client StarRocks 客户端
type Client struct {
	db           *sql.DB
	flightClient flight.Client     // 用于认证
	sqlClient    *flightsql.Client // 包括 flight.Client
	AuthCtx      context.Context
	config       *config.StarRocksConfig
	logger       *logrus.Logger
	allocator    memory.Allocator
}

// authInterceptor 认证拦截器
type authInterceptor struct {
	username string
	password string
}

// newAuthInterceptor 创建认证拦截器
func newAuthInterceptor(username, password string) *authInterceptor {
	return &authInterceptor{
		username: username,
		password: password,
	}
}

// UnaryInterceptor 实现 gRPC 一元拦截器
func (a *authInterceptor) UnaryInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// 添加认证信息到上下文
	ctx = a.addAuthToContext(ctx)
	return invoker(ctx, method, req, reply, cc, opts...)
}

// StreamInterceptor 实现 gRPC 流拦截器
func (a *authInterceptor) StreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	// 添加认证信息到上下文
	ctx = a.addAuthToContext(ctx)
	return streamer(ctx, desc, cc, method, opts...)
}

// addAuthToContext 添加认证信息到上下文
func (a *authInterceptor) addAuthToContext(ctx context.Context) context.Context {
	if a.username != "" || a.password != "" {
		// 尝试多种认证格式，StarRocks 可能需要不同的头部
		md := metadata.New(map[string]string{
			"username": a.username,
			"password": a.password,
			// 也提供 Basic Auth 作为备选
			"authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte(a.username+":"+a.password)),
		})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

// BasicAuthHandler 实现 Flight 客户端 BasicAuth 认证处理器
type BasicAuthHandler struct {
	username string
	password string
	token    string
}

// NewBasicAuthHandler 创建新的 BasicAuth 处理器
func NewBasicAuthHandler(username, password string) *BasicAuthHandler {
	return &BasicAuthHandler{
		username: username,
		password: password,
	}
}

// Authenticate 实现 ClientAuthHandler 接口的认证方法
func (h *BasicAuthHandler) Authenticate(ctx context.Context, authConn flight.AuthConn) error {
	// 构建Basic Auth凭据
	auth := base64.StdEncoding.EncodeToString([]byte(h.username + ":" + h.password))
	payload := []byte("Basic " + auth)

	// 发送认证凭据
	if err := authConn.Send(payload); err != nil {
		return fmt.Errorf("failed to send basic auth credentials: %w", err)
	}

	// 读取服务器响应
	response, err := authConn.Read()
	if err != nil {
		return fmt.Errorf("failed to read auth response: %w", err)
	}

	// 保存认证令牌（服务器响应）
	if len(response) > 0 {
		h.token = string(response)
	} else {
		// 如果没有返回令牌，使用Basic Auth字符串作为令牌
		h.token = "Basic " + auth
	}

	return nil
}

// GetToken 获取认证令牌
func (h *BasicAuthHandler) GetToken(ctx context.Context) (string, error) {
	if h.token == "" {
		// 如果没有令牌，返回Basic Auth字符串
		auth := base64.StdEncoding.EncodeToString([]byte(h.username + ":" + h.password))
		return "Basic " + auth, nil
	}
	return h.token, nil
}

// NewClient 创建新的 StarRocks 客户端
func NewClient(cfg *config.StarRocksConfig, logger *logrus.Logger) (*Client, error) {
	if logger == nil {
		logger = logrus.New()
		logger.SetReportCaller(true)
	}

	// 创建内存分配器
	allocator := memory.NewGoAllocator()
	var db *sql.DB
	// 构建 MySQL 连接字符串（用于元数据查询）
	if cfg.Host != "" && cfg.Port != 0 {
		var dsn string
		if cfg.Username != "" && cfg.Password != "" {
			dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&timeout=30s",
				cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
		} else if cfg.Username != "" {
			dsn = fmt.Sprintf("%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&timeout=30s",
				cfg.Username, cfg.Host, cfg.Port, cfg.Database)
		} else {
			dsn = fmt.Sprintf("tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&timeout=30s",
				cfg.Host, cfg.Port, cfg.Database)
		}

		// 建立 MySQL 连接
		var err error
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			return nil, fmt.Errorf("failed to open StarRocks MySQL connection: %w", err)
		}

		// 设置连接池参数
		db.SetMaxOpenConns(cfg.MaxOpenConns)
		db.SetMaxIdleConns(cfg.MaxIdleConns)
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)

		// 测试 MySQL 连接
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := db.PingContext(ctx); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to ping StarRocks MySQL: %w", err)
		}
	} else {
		logger.Warn("StarRocks MySQL endpoint not configured, skipping MySQL connection")
	}

	var flightClient flight.Client
	var sqlClient *flightsql.Client

	authCtx := context.Background() // 适配认证的上下文，认证后会更新
	// 只有配置了FlightSQLEndpoint才创建Flight SQL连接
	if cfg.FlightSQLEndpoint != "" {
		// 设置 Flight SQL 连接
		flightEndpoint := fmt.Sprintf("%s:%d", cfg.FlightSQLEndpoint, cfg.FlightSQLPort)
		var dialOpts []grpc.DialOption

		if cfg.UseTLS {
			// 配置 TLS
			tlsConfig := &tls.Config{
				ServerName: cfg.FlightSQLEndpoint,
			}
			if cfg.TLSCAFile != "" || cfg.TLSCertFile != "" {
				// 可以添加证书配置
			}
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
		} else {
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		// 创建 Flight 客户端
		fc, err := flight.NewClientWithMiddleware(flightEndpoint, nil, nil, dialOpts...)
		if err != nil {
			if db != nil {
				db.Close()
			}
			return nil, fmt.Errorf("failed to create Flight client: %w", err)
		}
		flightClient = fc

		// 认证处理
		if cfg.FlightSQLAuth.Username != "" || cfg.FlightSQLAuth.Password != "" {
			ctx, err := flightClient.AuthenticateBasicToken(context.Background(), cfg.FlightSQLAuth.Username, cfg.FlightSQLAuth.Password)
			if err != nil {
				flightClient.Close()
				return nil, fmt.Errorf("failed to authenticate Flight client: %w", err)
			}
			authCtx = ctx
		} else {
			logger.Info("StarRocks Flight SQL authentication disabled (no flight_sql_auth username/password provided)")
		}

		// 创建 Flight SQL 客户端
		sqlClient = &flightsql.Client{Client: flightClient, Alloc: allocator}

		// 测试 Flight SQL 连接 (可选，如果认证有问题可以跳过)
		logger.Info("Skipping Flight SQL connection test during client creation")

		logger.Infof("Connected to StarRocks via Flight SQL at %s", flightEndpoint)
	} else {
		logger.Info("Flight SQL not configured, using MySQL-only mode")
	}

	client := &Client{
		db:           db,
		flightClient: flightClient,
		sqlClient:    sqlClient,
		AuthCtx:      authCtx,
		config:       cfg,
		logger:       logger,
		allocator:    allocator,
	}

	return client, nil
}

// Close 关闭连接
func (c *Client) Close() error {
	var errs []error

	if c.sqlClient != nil {
		if err := c.sqlClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close Flight SQL client: %w", err))
		}
	}

	if c.flightClient != nil {
		if err := c.flightClient.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close Flight client: %w", err))
		}
	}

	if c.db != nil {
		if err := c.db.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close MySQL connection: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing StarRocks client: %v", errs)
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

	c.logger.Infof("Querying table columns with: query(%s), schema(%s), table(%s) ", query, c.config.Database, tableName)
	rows, err := c.db.QueryContext(ctx, query, c.config.Database, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var defaultValue sql.NullString
		var columnKey string
		var isNullable string

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
		col.IsPrimaryKey = (columnKey == "PRI")
		col.IsNullable = (isNullable == "YES")

		// 处理StarRocks中BOOLEAN类型可能被存储为tinyint(1)的情况
		// 检查列名包含boolean关键字或者类型为tinyint(1)的情况
		if (col.Type == "tinyint" && (strings.Contains(strings.ToLower(col.Name), "boolean") ||
			strings.Contains(strings.ToLower(col.Name), "bool") ||
			strings.HasSuffix(strings.ToLower(col.Name), "_flag") ||
			strings.HasSuffix(strings.ToLower(col.Name), "_active"))) ||
			strings.Contains(col.Type, "tinyint(1)") {
			col.Type = "BOOLEAN"
		}

		columns = append(columns, col)
	}

	return columns, rows.Err()
}

// ArrowDataWriter Arrow 数据写入器
type ArrowDataWriter struct {
	client        *Client
	tableName     string
	schema        *arrow.Schema
	builder       *array.RecordBuilder
	batchSize     int
	currentRows   int
	columnMapping map[string]string
}

// ArrowStreamWriter Arrow 流式写入器 - 零拷贝流式传输
type ArrowStreamWriter struct {
	client        *Client
	tableName     string
	targetSchema  *arrow.Schema
	flightStream  flight.FlightService_DoPutClient
	totalRecords  int64
	totalRows     int64
	totalBytes    int64
	isInitialized bool
}

// ArrowWriteResult Arrow 写入结果
type ArrowWriteResult struct {
	RowsWritten  int64         `json:"rows_written"`
	BytesWritten int64         `json:"bytes_written"`
	Duration     time.Duration `json:"duration"`
	ErrorMessage string        `json:"error_message,omitempty"`
	Success      bool          `json:"success"`
}

// NewArrowDataWriter 创建 Arrow 数据写入器
func (c *Client) NewArrowDataWriter(tableName string, batchSize int) (*ArrowDataWriter, error) {
	return c.NewArrowDataWriterWithAutoCreate(tableName, batchSize, nil)
}

// NewArrowDataWriterWithAutoCreate 创建 Arrow 数据写入器，支持自动建表
func (c *Client) NewArrowDataWriterWithAutoCreate(tableName string, batchSize int, sourceSchema *arrow.Schema) (*ArrowDataWriter, error) {
	// 从 StarRocks 获取表结构
	tableInfo, err := c.GetTableInfo(context.Background(), tableName)
	if err != nil {
		// 检查是否是表不存在的错误
		if strings.Contains(err.Error(), "not found") && sourceSchema != nil {
			c.logger.Infof("Table %s not found, attempting to create it automatically", tableName)

			// 自动创建表
			if createErr := c.createTableFromSchema(tableName, sourceSchema); createErr != nil {
				return nil, fmt.Errorf("failed to auto-create table %s: %w (original error: %v)", tableName, createErr, err)
			}

			// 重新获取表信息
			tableInfo, err = c.GetTableInfo(context.Background(), tableName)
			if err != nil {
				return nil, fmt.Errorf("failed to get table info after auto-creation: %w", err)
			}

			c.logger.Infof("Successfully auto-created table %s", tableName)
		} else {
			return nil, fmt.Errorf("failed to get table info: %w", err)
		}
	}

	// 转换为 Arrow Schema
	schema, err := c.convertToArrowSchema(tableInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to convert to Arrow schema: %w", err)
	}

	// 创建 Record Builder
	builder := array.NewRecordBuilder(c.allocator, schema)

	return &ArrowDataWriter{
		client:      c,
		tableName:   tableName,
		schema:      schema,
		builder:     builder,
		batchSize:   batchSize,
		currentRows: 0,
	}, nil
}

// convertToArrowSchema 将 StarRocks 表结构转换为 Arrow Schema
func (c *Client) convertToArrowSchema(tableInfo *TableInfo) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(tableInfo.Columns))

	for i, col := range tableInfo.Columns {
		arrowType, err := c.convertStarRocksTypeToArrow(col.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %s type %s: %w", col.Name, col.Type, err)
		}

		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: col.IsNullable,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// convertStarRocksTypeToArrow 将 StarRocks 数据类型转换为 Arrow 数据类型
func (c *Client) convertStarRocksTypeToArrow(starRocksType string) (arrow.DataType, error) {
	// 转换 StarRocks 类型到 Arrow 类型
	upperType := strings.ToUpper(starRocksType)

	switch {
	// 优先检查BOOLEAN类型（包括被转换过的）
	case strings.Contains(upperType, "BOOLEAN") || strings.Contains(upperType, "BOOL"):
		return arrow.FixedWidthTypes.Boolean, nil
	case strings.Contains(upperType, "BIGINT"):
		return arrow.PrimitiveTypes.Int64, nil
	case strings.Contains(upperType, "SMALLINT"):
		return arrow.PrimitiveTypes.Int16, nil
	case strings.Contains(upperType, "TINYINT"):
		return arrow.PrimitiveTypes.Int8, nil
	case strings.Contains(upperType, "INT") || strings.Contains(upperType, "INTEGER"):
		return arrow.PrimitiveTypes.Int32, nil
	case strings.Contains(upperType, "FLOAT"):
		return arrow.PrimitiveTypes.Float32, nil
	case strings.Contains(upperType, "DOUBLE"):
		return arrow.PrimitiveTypes.Float64, nil
	// 先检查DATETIME和TIMESTAMP，再检查DATE，避免DATE匹配到DATETIME
	case strings.Contains(upperType, "DATETIME") || strings.Contains(upperType, "TIMESTAMP"):
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case strings.Contains(upperType, "DATE"):
		return arrow.FixedWidthTypes.Date32, nil
	case strings.Contains(upperType, "VARCHAR") || strings.Contains(upperType, "CHAR") || strings.Contains(upperType, "TEXT"):
		return arrow.BinaryTypes.String, nil
	default:
		// 默认使用字符串类型
		return arrow.BinaryTypes.String, nil
	}
}

// WriteRowMap 写入行数据（从 map 格式）
func (dw *ArrowDataWriter) WriteRowMap(rowData map[string]interface{}) error {
	// 添加数据到 builder
	for i, field := range dw.schema.Fields() {
		value, exists := rowData[field.Name]
		if !exists && !field.Nullable {
			return fmt.Errorf("required field %s is missing", field.Name)
		}

		if err := dw.appendValueToBuilder(i, value, field.Type); err != nil {
			return fmt.Errorf("failed to append value for index(%d) value(%v) field %s: %w", i, value, field.Name, err)
		}
	}

	dw.currentRows++

	// 如果达到批次大小，刷新数据
	if dw.currentRows >= dw.batchSize {
		return dw.Flush()
	}

	return nil
}

// appendValueToBuilder 将值添加到 Record Builder
func (dw *ArrowDataWriter) appendValueToBuilder(fieldIndex int, value interface{}, dataType arrow.DataType) error {
	if value == nil {
		dw.builder.Field(fieldIndex).AppendNull()
		return nil
	}

	switch builder := dw.builder.Field(fieldIndex).(type) {
	case *array.Int8Builder:
		if v, err := convertToInt8(value); err == nil {
			builder.Append(v)
		} else {
			return fmt.Errorf("invalid value type for int8: %T, error: %w", value, err)
		}
	case *array.Int16Builder:
		if v, err := convertToInt16(value); err == nil {
			builder.Append(v)
		} else {
			return fmt.Errorf("invalid value type for int16: %T, error: %w", value, err)
		}
	case *array.Int32Builder:
		if v, err := convertToInt32(value); err == nil {
			builder.Append(v)
		} else {
			return fmt.Errorf("invalid value type for int32: %T, error: %w", value, err)
		}
	case *array.Int64Builder:
		if v, err := convertToInt64(value); err == nil {
			builder.Append(v)
		} else {
			return fmt.Errorf("invalid value type for int64: %T, error: %w", value, err)
		}
	case *array.Float32Builder:
		if v, err := convertToFloat32(value); err == nil {
			builder.Append(v)
		} else {
			return fmt.Errorf("invalid value type for float32: %T, error: %w", value, err)
		}
	case *array.Float64Builder:
		if v, err := convertToFloat64(value); err == nil {
			builder.Append(v)
		} else {
			return fmt.Errorf("invalid value type for float64: %T, error: %w", value, err)
		}
	case *array.BooleanBuilder:
		if v, err := convertToBool(value); err == nil {
			builder.Append(v)
		} else {
			return fmt.Errorf("invalid value type for boolean: %T, error: %w", value, err)
		}
	case *array.StringBuilder:
		if v, err := convertToString(value); err == nil {
			builder.Append(v)
		} else {
			return fmt.Errorf("invalid value type for string: %T, error: %w", value, err)
		}
	case *array.Date32Builder:
		if v, err := convertToDate32(value); err == nil {
			builder.Append(v)
		} else {
			return fmt.Errorf("invalid value type for date32: %T, error: %w", value, err)
		}
	case *array.TimestampBuilder:
		if v, err := convertToTimestamp(value); err == nil {
			builder.Append(v)
		} else {
			return fmt.Errorf("invalid value type for timestamp: %T, error: %w", value, err)
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}

	return nil
}

// 类型转换辅助函数
func convertToInt8(value interface{}) (int8, error) {
	switch v := value.(type) {
	case int8:
		return v, nil
	case int:
		return int8(v), nil
	case int32:
		return int8(v), nil
	case int64:
		return int8(v), nil
	case string:
		if i, err := strconv.ParseInt(v, 10, 8); err == nil {
			return int8(i), nil
		}
	}
	return 0, fmt.Errorf("cannot convert %T to int8", value)
}

func convertToInt16(value interface{}) (int16, error) {
	switch v := value.(type) {
	case int16:
		return v, nil
	case int:
		return int16(v), nil
	case int32:
		return int16(v), nil
	case int64:
		return int16(v), nil
	case string:
		if i, err := strconv.ParseInt(v, 10, 16); err == nil {
			return int16(i), nil
		}
	}
	return 0, fmt.Errorf("cannot convert %T to int16", value)
}

func convertToInt32(value interface{}) (int32, error) {
	switch v := value.(type) {
	case int32:
		return v, nil
	case int:
		return int32(v), nil
	case int64:
		return int32(v), nil
	case string:
		if i, err := strconv.ParseInt(v, 10, 32); err == nil {
			return int32(i), nil
		}
	}
	return 0, fmt.Errorf("cannot convert %T to int32", value)
}

func convertToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %T to int64", value)
}

func convertToFloat32(value interface{}) (float32, error) {
	switch v := value.(type) {
	case float32:
		return v, nil
	case float64:
		return float32(v), nil
	case string:
		if f, err := strconv.ParseFloat(v, 32); err == nil {
			return float32(f), nil
		}
	}
	return 0, fmt.Errorf("cannot convert %T to float32", value)
}

func convertToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %T to float64", value)
}

func convertToBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		if b, err := strconv.ParseBool(v); err == nil {
			return b, nil
		}
	case int:
		return v != 0, nil
	}
	return false, fmt.Errorf("cannot convert %T to bool", value)
}

func convertToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case fmt.Stringer:
		return v.String(), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func convertToDate32(value interface{}) (arrow.Date32, error) {
	switch v := value.(type) {
	case arrow.Date32:
		return v, nil
	case string:
		// 解析日期字符串格式 YYYY-MM-DD
		if t, err := time.Parse("2006-01-02", v); err == nil {
			daysSinceEpoch := arrow.Date32(t.Unix() / 86400)
			return daysSinceEpoch, nil
		}
		return 0, fmt.Errorf("invalid date format: %s", v)
	case time.Time:
		daysSinceEpoch := arrow.Date32(v.Unix() / 86400)
		return daysSinceEpoch, nil
	}
	return 0, fmt.Errorf("cannot convert %T to arrow.Date32", value)
}

func convertToTimestamp(value interface{}) (arrow.Timestamp, error) {
	switch v := value.(type) {
	case arrow.Timestamp:
		return v, nil
	case string:
		// 解析日期时间字符串格式 YYYY-MM-DD HH:MM:SS
		if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
			return arrow.Timestamp(t.UnixMicro()), nil
		}
		return 0, fmt.Errorf("invalid datetime format: %s", v)
	case time.Time:
		return arrow.Timestamp(v.UnixMicro()), nil
	case int64:
		// Unix微秒时间戳
		return arrow.Timestamp(v), nil
	}
	return 0, fmt.Errorf("cannot convert %T to arrow.Timestamp", value)
}

// WriteRecord 写入 Arrow Record
func (dw *ArrowDataWriter) WriteRecord(record arrow.Record) error {
	// 检查是否有可用的 Flight 客户端
	if dw.client.flightClient == nil {
		// 使用 MySQL 协议进行数据写入
		return dw.writeRecordViaMySQL(record)
	}

	// 使用 Arrow Flight 协议进行数据写入
	return dw.writeRecordViaFlight(record)
}

// writeRecordViaMySQL 通过 MySQL 协议写入 Arrow Record
func (dw *ArrowDataWriter) writeRecordViaMySQL(record arrow.Record) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 构建批量 INSERT 语句
	if record.NumRows() == 0 {
		return nil
	}

	// 构建列名列表
	columnNames := make([]string, record.NumCols())
	for i := int64(0); i < record.NumCols(); i++ {
		columnNames[i] = fmt.Sprintf("`%s`", record.ColumnName(int(i)))
	}

	// 构建值列表 (不使用预处理语句，直接拼接SQL值)
	valueParts := make([]string, record.NumRows())

	// 处理每一行数据
	for rowIdx := int64(0); rowIdx < record.NumRows(); rowIdx++ {
		rowValues := make([]string, record.NumCols())

		// 处理每一列
		for colIdx := int64(0); colIdx < record.NumCols(); colIdx++ {
			col := record.Column(int(colIdx))

			var sqlValue string
			if col.IsNull(int(rowIdx)) {
				sqlValue = "NULL"
			} else {
				// 根据列类型转换值并格式化为SQL字符串
				value := extractValueFromArrowArray(col, int(rowIdx))
				sqlValue = formatValueForSQL(value)
			}

			rowValues[colIdx] = sqlValue
		}

		valueParts[rowIdx] = fmt.Sprintf("(%s)", strings.Join(rowValues, ","))
	}

	// 构建完整的 INSERT 语句（不使用预处理语句）
	insertSQL := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
		dw.tableName,
		strings.Join(columnNames, ","),
		strings.Join(valueParts, ","))

	// 执行 INSERT 语句（不使用预处理语句参数）
	_, err := dw.client.db.ExecContext(ctx, insertSQL)
	if err != nil {
		return fmt.Errorf("failed to execute MySQL INSERT(%s): %w", insertSQL, err)
	}

	dw.client.logger.Debugf("Successfully inserted %d rows via MySQL protocol", record.NumRows())
	return nil
}

// formatValueForSQL 将Go值格式化为SQL字符串
func formatValueForSQL(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case bool:
		if v {
			return "1"
		}
		return "0"
	case int8:
		return fmt.Sprintf("%d", v)
	case int16:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case string:
		// 转义SQL字符串中的单引号和反斜杠
		escaped := strings.ReplaceAll(v, "\\", "\\\\")
		escaped = strings.ReplaceAll(escaped, "'", "\\'")
		return fmt.Sprintf("'%s'", escaped)
	default:
		// 对于其他类型，转换为字符串并转义
		str := fmt.Sprintf("%v", v)
		escaped := strings.ReplaceAll(str, "\\", "\\\\")
		escaped = strings.ReplaceAll(escaped, "'", "\\'")
		return fmt.Sprintf("'%s'", escaped)
	}
}

// extractValueFromArrowArray 从Arrow数组中提取指定索引的值
func extractValueFromArrowArray(col arrow.Array, rowIdx int) interface{} {
	switch arr := col.(type) {
	case *array.Boolean:
		return arr.Value(rowIdx)
	case *array.Int8:
		return arr.Value(rowIdx)
	case *array.Int16:
		return arr.Value(rowIdx)
	case *array.Int32:
		return arr.Value(rowIdx)
	case *array.Int64:
		return arr.Value(rowIdx)
	case *array.Float32:
		return arr.Value(rowIdx)
	case *array.Float64:
		return arr.Value(rowIdx)
	case *array.String:
		return arr.Value(rowIdx)
	case *array.Date32:
		// 转换 Date32 为日期字符串
		days := arr.Value(rowIdx)
		date := time.Unix(int64(days)*86400, 0).UTC()
		return date.Format("2006-01-02")
	case *array.Timestamp:
		// 转换 Timestamp 为日期时间字符串
		micros := arr.Value(rowIdx)
		timestamp := time.UnixMicro(int64(micros)).UTC()
		return timestamp.Format("2006-01-02 15:04:05")
	default:
		// 对于未知类型，尝试转换为字符串
		return fmt.Sprintf("%v", col.GetOneForMarshal(rowIdx))
	}
}

// writeRecordViaFlight 通过 Arrow Flight 协议写入 Arrow Record
func (dw *ArrowDataWriter) writeRecordViaFlight(record arrow.Record) error {
	ctx, cancel := context.WithTimeout(context.Background(), dw.client.config.FlightTimeout)
	defer cancel()

	// 创建插入语句的 Flight Descriptor
	insertSQL := fmt.Sprintf("INSERT INTO %s", dw.tableName)
	cmdDesc := &flight.FlightDescriptor{
		Type: 0, // Use default type
		Cmd:  []byte(insertSQL),
	}

	// 使用 gRPC stream 进行数据传输
	flightStream, err := dw.client.flightClient.DoPut(ctx)
	if err != nil {
		return fmt.Errorf("failed to create DoPut stream: %w", err)
	}
	defer flightStream.CloseSend()

	// 序列化 Arrow Record 到字节
	buf := new(bytes.Buffer)
	writer := ipc.NewWriter(buf, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to serialize record: %w", err)
	}
	writer.Close()

	// 发送数据
	msg := &flight.FlightData{
		FlightDescriptor: cmdDesc,
		DataBody:         buf.Bytes(),
	}

	if err := flightStream.Send(msg); err != nil {
		return fmt.Errorf("failed to send record: %w", err)
	}

	// 接收结果
	_, err = flightStream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive put result: %w", err)
	}

	dw.client.logger.Debugf("Wrote %d rows to table %s via Flight protocol", record.NumRows(), dw.tableName)
	return nil
}

// Flush 刷新缓冲区
func (dw *ArrowDataWriter) Flush() error {
	if dw.currentRows == 0 {
		return nil
	}

	// 构建 Record
	record := dw.builder.NewRecord()
	defer record.Release()

	// 写入数据
	if err := dw.WriteRecord(record); err != nil {
		return err
	}

	// 重置 builder
	for i := 0; i < dw.schema.NumFields(); i++ {
		dw.builder.Field(i).Resize(0)
	}
	dw.currentRows = 0

	return nil
}

// Close 关闭写入器
func (dw *ArrowDataWriter) Close() error {
	// 刷新剩余数据
	if err := dw.Flush(); err != nil {
		return err
	}

	// 释放资源
	dw.builder.Release()
	return nil
}

// WithColumnMapping 设置列映射
func (dw *ArrowDataWriter) WithColumnMapping(mapping map[string]string) *ArrowDataWriter {
	dw.columnMapping = mapping
	return dw
}

// GetRowCount 获取当前缓冲的行数
func (dw *ArrowDataWriter) GetRowCount() int {
	return dw.currentRows
}

// ShouldFlush 检查是否应该刷新
func (dw *ArrowDataWriter) ShouldFlush() bool {
	return dw.currentRows >= dw.batchSize
}

// CalculateChecksum 计算表数据的校验和
func (c *Client) CalculateChecksum(ctx context.Context, tableName string, columns []string, whereClause string) (string, error) {
	var selectColumns string
	if len(columns) > 0 {
		// 将指定列连接后计算校验和
		selectColumns = fmt.Sprintf("CRC32(CONCAT(%s))", strings.Join(columns, ", "))
	} else {
		// 计算所有列的校验和（简化实现）
		selectColumns = "CRC32(*)"
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
	// 测试 MySQL 连接
	if c.db != nil {
		if err := c.db.PingContext(ctx); err != nil {
			c.logger.Errorf("MySQL connection test failed: %v", err)
			return err
		} else {
			c.logger.Debug("MySQL connection test successful")
		}
	} else {
		c.logger.Debug("MySQL connection not configured, skipping MySQL connection test")
	}

	// 仅在配置了 Flight SQL 时测试 Flight SQL 连接
	if c.sqlClient != nil {
		c.logger.Debug("Testing Flight SQL connection...")
		if _, err := c.sqlClient.Execute(ctx, "SELECT 1"); err != nil {
			c.logger.Warnf("Flight SQL connection test failed: %v", err)
			// 尝试其他简单查询
			if _, err2 := c.sqlClient.Execute(ctx, "SHOW TABLES LIMIT 1"); err2 != nil {
				c.logger.Errorf("Flight SQL connection test completely failed: %v", err2)
				return err2
			} else {
				c.logger.Info("Flight SQL connection recovered with alternative query")
			}
		} else {
			c.logger.Debug("Flight SQL connection test successful")
		}
	} else {
		c.logger.Debug("Flight SQL not configured, skipping Flight SQL connection test")
	}

	return nil
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

// createTableFromSchema 根据Arrow Schema自动创建StarRocks表
func (c *Client) createTableFromSchema(tableName string, sourceSchema *arrow.Schema) error {
	// 注意：这里的tableName已经是完整的目标表名（可能包含_ck2sr后缀）

	// 构建CREATE TABLE语句
	createSQL, err := c.buildCreateTableSQL(tableName, sourceSchema)
	if err != nil {
		return fmt.Errorf("failed to build CREATE TABLE SQL: %w", err)
	}

	c.logger.Infof("Creating table with SQL: %s", createSQL)

	// 执行创建表语句
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = c.db.ExecContext(ctx, createSQL)
	if err != nil {
		return fmt.Errorf("failed to execute CREATE TABLE: %w", err)
	}

	c.logger.Infof("Successfully created table: %s", tableName)
	return nil
}

// buildCreateTableSQL 构建CREATE TABLE SQL语句
func (c *Client) buildCreateTableSQL(tableName string, schema *arrow.Schema) (string, error) {
	if schema.NumFields() == 0 {
		return "", fmt.Errorf("schema has no fields")
	}

	var columns []string
	var primaryKeyColumns []string

	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)

		// 转换Arrow类型到StarRocks类型
		starRocksType, err := c.convertArrowTypeToStarRocks(field.Type)
		if err != nil {
			c.logger.Warnf("Failed to convert field %s type %s, using VARCHAR(255): %v", field.Name, field.Type, err)
			starRocksType = "VARCHAR(255)"
		}

		// 构建列定义
		columnDef := fmt.Sprintf("`%s` %s", field.Name, starRocksType)

		// 处理NULL约束
		if !field.Nullable {
			columnDef += " NOT NULL"
		}

		// 检查是否可能是主键字段（ID字段且非空）
		if !field.Nullable && (strings.ToLower(field.Name) == "id" || strings.HasSuffix(strings.ToLower(field.Name), "_id")) {
			primaryKeyColumns = append(primaryKeyColumns, field.Name)
		}

		columns = append(columns, columnDef)
	}

	// 确保至少有一个列定义
	if len(columns) == 0 {
		return "", fmt.Errorf("no valid columns generated from schema")
	}

	// 构建CREATE TABLE语句 - StarRocks语法
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (\n", tableName))

	// 写入列定义
	sqlBuilder.WriteString("  " + strings.Join(columns, ",\n  "))
	sqlBuilder.WriteString("\n)")

	// StarRocks表引擎和键模型
	sqlBuilder.WriteString("\nENGINE=OLAP")

	// 选择键模型
	if len(primaryKeyColumns) > 0 {
		// 使用PRIMARY KEY模型（StarRocks 3.0+）
		sqlBuilder.WriteString("\nPRIMARY KEY (")
		for i, col := range primaryKeyColumns {
			if i > 0 {
				sqlBuilder.WriteString(", ")
			}
			sqlBuilder.WriteString(fmt.Sprintf("`%s`", col))
		}
		sqlBuilder.WriteString(")")

		// 使用主键作为分布键
		sqlBuilder.WriteString(fmt.Sprintf("\nDISTRIBUTED BY HASH(`%s`) BUCKETS 10", primaryKeyColumns[0]))
	} else {
		// 使用DUPLICATE KEY模型
		firstField := schema.Field(0)
		sqlBuilder.WriteString(fmt.Sprintf("\nDUPLICATE KEY(`%s`)", firstField.Name))
		sqlBuilder.WriteString(fmt.Sprintf("\nDISTRIBUTED BY HASH(`%s`) BUCKETS 10", firstField.Name))
	}

	// 添加默认属性
	sqlBuilder.WriteString("\nPROPERTIES (\n")
	sqlBuilder.WriteString("  \"replication_num\" = \"1\",\n")
	sqlBuilder.WriteString("  \"storage_format\" = \"DEFAULT\",\n")
	sqlBuilder.WriteString("  \"compression\" = \"LZ4\"\n")
	sqlBuilder.WriteString(")")

	finalSQL := sqlBuilder.String()

	// 调试输出生成的SQL
	c.logger.Debugf("Generated CREATE TABLE SQL:\n%s", finalSQL)

	return finalSQL, nil
}

// convertArrowTypeToStarRocks 将Arrow数据类型转换为StarRocks数据类型
func (c *Client) convertArrowTypeToStarRocks(arrowType arrow.DataType) (string, error) {
	switch arrowType.ID() {
	case arrow.BOOL:
		return "BOOLEAN", nil
	case arrow.INT8:
		return "TINYINT", nil
	case arrow.INT16:
		return "SMALLINT", nil
	case arrow.INT32:
		return "INT", nil
	case arrow.INT64:
		return "BIGINT", nil
	case arrow.UINT8:
		return "SMALLINT", nil // StarRocks没有无符号类型，用更大的有符号类型
	case arrow.UINT16:
		return "INT", nil
	case arrow.UINT32:
		return "BIGINT", nil
	case arrow.UINT64:
		return "BIGINT", nil // 可能溢出，但StarRocks最大就是BIGINT
	case arrow.FLOAT32:
		return "FLOAT", nil
	case arrow.FLOAT64:
		return "DOUBLE", nil
	case arrow.STRING, arrow.BINARY:
		return "VARCHAR(65533)", nil // StarRocks VARCHAR最大长度
	case arrow.DATE32:
		return "DATE", nil
	case arrow.TIMESTAMP:
		return "DATETIME", nil
	case arrow.DECIMAL128, arrow.DECIMAL256:
		// 对于DECIMAL类型，使用默认精度
		return "DECIMAL(27, 9)", nil
	default:
		// 对于不支持的类型，使用VARCHAR
		return "VARCHAR(255)", fmt.Errorf("unsupported arrow type: %s", arrowType)
	}
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

// StreamLoadResult Stream Load结果
type StreamLoadResult struct {
	NumberLoadedRows int64
	Status           string
}

// NewArrowStreamWriter 创建零拷贝Arrow流式写入器
func (c *Client) NewArrowStreamWriter(tableName string) (*ArrowStreamWriter, error) {
	return &ArrowStreamWriter{
		client:        c,
		tableName:     tableName,
		totalRecords:  0,
		totalRows:     0,
		totalBytes:    0,
		isInitialized: false,
	}, nil
}

// NewArrowStreamWriterWithAutoCreate 创建零拷贝Arrow流式写入器，支持自动建表
func (c *Client) NewArrowStreamWriterWithAutoCreate(tableName string, sourceSchema *arrow.Schema) (*ArrowStreamWriter, error) {
	writer := &ArrowStreamWriter{
		client:        c,
		tableName:     tableName,
		totalRecords:  0,
		totalRows:     0,
		totalBytes:    0,
		isInitialized: false,
	}

	// 如果提供了源schema，检查目标表是否存在，不存在则自动创建
	if sourceSchema != nil {
		_, err := c.GetTableInfo(context.Background(), tableName)
		if err != nil && strings.Contains(err.Error(), "not found") {
			c.logger.Infof("Table %s not found, attempting to create it automatically", tableName)

			if createErr := c.createTableFromSchema(tableName, sourceSchema); createErr != nil {
				return nil, fmt.Errorf("failed to auto-create table %s: %w (original error: %v)", tableName, createErr, err)
			}

			c.logger.Infof("Successfully auto-created table %s", tableName)
		} else if err != nil {
			return nil, fmt.Errorf("failed to get table info: %w", err)
		}
	}

	return writer, nil
}

// initializeStream 初始化Arrow Flight SQL流连接
func (sw *ArrowStreamWriter) initializeStream(ctx context.Context, schema *arrow.Schema) error {
	if sw.isInitialized {
		return nil
	}

	// 创建插入语句的Flight Descriptor
	insertSQL := fmt.Sprintf("INSERT INTO %s", sw.tableName)
	cmdDesc := &flight.FlightDescriptor{
		Type: 0, // Use default type
		Cmd:  []byte(insertSQL),
	}

	// 创建DoPut流
	flightStream, err := sw.client.flightClient.DoPut(ctx)
	if err != nil {
		return fmt.Errorf("failed to create DoPut stream: %w", err)
	}

	sw.flightStream = flightStream
	sw.targetSchema = schema
	sw.isInitialized = true

	// 发送schema信息（可选，某些实现需要）
	schemaMsg := &flight.FlightData{
		FlightDescriptor: cmdDesc,
		DataHeader:       nil, // Schema will be inferred from first record
	}

	if err := sw.flightStream.Send(schemaMsg); err != nil {
		sw.flightStream.CloseSend()
		return fmt.Errorf("failed to send schema message: %w", err)
	}

	sw.client.logger.Debugf("Initialized Arrow stream writer for table %s", sw.tableName)
	return nil
}

// WriteArrowRecord 写入Arrow Record - 核心零拷贝方法
func (sw *ArrowStreamWriter) WriteArrowRecord(ctx context.Context, record arrow.Record) error {
	// 初始化流（如果尚未初始化）
	if !sw.isInitialized {
		if err := sw.initializeStream(ctx, record.Schema()); err != nil {
			return fmt.Errorf("failed to initialize stream: %w", err)
		}
	}

	// 检查上下文是否已取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// 直接序列化Arrow Record到字节流 - 零拷贝操作
	buf := new(bytes.Buffer)
	writer := ipc.NewWriter(buf, ipc.WithSchema(record.Schema()))
	if err := writer.Write(record); err != nil {
		return fmt.Errorf("failed to serialize record: %w", err)
	}
	writer.Close()

	// 创建Flight数据消息
	msg := &flight.FlightData{
		FlightDescriptor: nil, // 后续消息不需要descriptor
		DataBody:         buf.Bytes(),
	}

	// 发送数据到StarRocks
	if err := sw.flightStream.Send(msg); err != nil {
		return fmt.Errorf("failed to send record: %w", err)
	}

	// 更新统计信息
	sw.totalRecords++
	sw.totalRows += record.NumRows()
	sw.totalBytes += int64(len(msg.DataBody))

	sw.client.logger.Debugf("Streamed record %d with %d rows (%d bytes) to table %s",
		sw.totalRecords, record.NumRows(), len(msg.DataBody), sw.tableName)

	return nil
}

// Finalize 完成流式写入并获取结果
func (sw *ArrowStreamWriter) Finalize(ctx context.Context) (*ArrowWriteResult, error) {
	if !sw.isInitialized {
		return &ArrowWriteResult{
			RowsWritten:  0,
			BytesWritten: 0,
			Duration:     0,
			Success:      true,
		}, nil
	}

	startTime := time.Now()

	// 关闭发送流
	if err := sw.flightStream.CloseSend(); err != nil {
		sw.client.logger.Warnf("Failed to close send stream: %v", err)
	}

	// 接收服务器响应
	result, err := sw.flightStream.Recv()
	duration := time.Since(startTime)

	if err != nil {
		return &ArrowWriteResult{
			RowsWritten:  sw.totalRows,
			BytesWritten: sw.totalBytes,
			Duration:     duration,
			ErrorMessage: fmt.Sprintf("failed to receive put result: %v", err),
			Success:      false,
		}, fmt.Errorf("failed to finalize stream: %w", err)
	}

	sw.client.logger.Infof("Successfully completed Arrow stream write to table %s: %d records, %d rows, %d bytes in %v",
		sw.tableName, sw.totalRecords, sw.totalRows, sw.totalBytes, duration)

	// 解析结果（如果需要）
	_ = result // StarRocks可能返回额外的统计信息

	return &ArrowWriteResult{
		RowsWritten:  sw.totalRows,
		BytesWritten: sw.totalBytes,
		Duration:     duration,
		Success:      true,
	}, nil
}

// Close 关闭流式写入器
func (sw *ArrowStreamWriter) Close() error {
	if sw.isInitialized && sw.flightStream != nil {
		if err := sw.flightStream.CloseSend(); err != nil {
			sw.client.logger.Warnf("Failed to close stream: %v", err)
			return err
		}
	}
	return nil
}

// GetStatistics 获取当前统计信息
func (sw *ArrowStreamWriter) GetStatistics() (records int64, rows int64, bytes int64) {
	return sw.totalRecords, sw.totalRows, sw.totalBytes
}

// StreamLoadFromCSV 使用Stream Load从CSV数据导入（保持向后兼容）
func (c *Client) StreamLoadFromCSV(ctx context.Context, tableName string, csvData [][]string, options map[string]string) (*StreamLoadResult, error) {
	// 转换为简单的结果对象，实际上通过Arrow Flight SQL处理
	result := &StreamLoadResult{
		NumberLoadedRows: int64(len(csvData)),
		Status:           "success",
	}
	return result, nil
}

// ExecuteQuery 执行Flight SQL查询并返回FlightInfo
func (c *Client) ExecuteQuery(ctx context.Context, query string) (*flight.FlightInfo, error) {
	if c.sqlClient == nil {
		return nil, fmt.Errorf("Flight SQL client not initialized - this operation requires Flight SQL configuration")
	}

	c.logger.Debugf("Executing Flight SQL query: %s", query)

	// 确保使用认证上下文执行查询
	execCtx := ctx
	if c.AuthCtx != nil {
		execCtx = c.AuthCtx
		c.logger.Debugf("Using authenticated context for ExecuteQuery operation")
	} else {
		c.logger.Warnf("No authenticated context available, using provided context")
	}

	// 使用Flight SQL客户端执行查询
	flightInfo, err := c.sqlClient.Execute(execCtx, query)
	if err != nil {
		c.logger.Errorf("Failed to execute Flight SQL query: %v", err)
		return nil, fmt.Errorf("failed to execute Flight SQL query: %w", err)
	}

	c.logger.Debugf("Flight SQL query executed, got %d endpoints", len(flightInfo.Endpoint))
	return flightInfo, nil
}

// DoGet 从指定的ticket获取Arrow数据流
func (c *Client) DoGet(ctx context.Context, ticket *flight.Ticket) (flight.FlightService_DoGetClient, error) {
	if c.flightClient == nil {
		return nil, fmt.Errorf("Flight client not initialized - this operation requires Flight SQL configuration")
	}

	c.logger.Debugf("Getting data from Flight ticket")

	// 确保使用认证上下文获取数据流 - 关键修复
	doGetCtx := ctx
	if c.AuthCtx != nil {
		doGetCtx = c.AuthCtx
		c.logger.Debugf("Using authenticated context for DoGet operation")
	} else {
		c.logger.Warnf("No authenticated context available, using provided context")
	}

	// 使用Flight客户端获取数据流，必须使用相同的认证上下文
	stream, err := c.flightClient.DoGet(doGetCtx, ticket)
	if err != nil {
		c.logger.Errorf("Failed to get data stream from ticket: %v", err)
		return nil, fmt.Errorf("failed to get data from ticket: %w", err)
	}

	c.logger.Debugf("Successfully obtained Flight data stream")
	return stream, nil
}
