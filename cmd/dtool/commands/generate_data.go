package commands

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/internal/logging"
)

// ColumnInfo 表示数据库列信息
type ColumnInfo struct {
	Name         string
	DataType     string
	IsNullable   bool
	DefaultValue *string
	CharLength   *int64
	NumPrecision *int64
	NumScale     *int64
}

type GenerateDataOptions struct {
	DatabaseType string
	SQLEndpoint  string
	SQLPort      int
	Username     string
	Password     string
	Database     string
	Table        string
	Rows         int
	BatchSize    int
	LogLevel     string
}

func RunGenerateData(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("database type required (starrocks or clickhouse)")
	}

	dbType := args[0]
	if dbType != "starrocks" && dbType != "clickhouse" {
		return fmt.Errorf("unsupported database type: %s (supported: starrocks, clickhouse)", dbType)
	}

	options, err := parseGenerateDataFlags(dbType, args[1:])
	if err != nil {
		return err
	}

	logger, err := createLoggerFromOptions(options)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}

	// Test debug logging configuration
	logger.Debugf("🔧 DTOOL DEBUG: Log level configuration test - debug logging works! (Level: %s)", options.LogLevel)
	logger.Infof("ℹ️ DTOOL INFO: Starting data generation with log level: %s", options.LogLevel)

	// 设置上下文和信号处理
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 监听中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Warnf("🚨 Received signal: %v, stopping data generation...", sig)
		cancel()
	}()

	generator := &DataGenerator{
		logger:  logger,
		options: options,
		ctx:     ctx,
	}

	return generator.Generate()
}

func createLoggerFromOptions(options *GenerateDataOptions) (*logrus.Logger, error) {
	// 创建基本的日志配置
	logConfig := config.LogConfig{
		Level:  options.LogLevel,
		Format: "text",
		Output: "stdout",
	}

	// 使用logging utility创建logger
	logger, err := logging.CreateLogger(logConfig)
	if err != nil {
		// 如果失败，回退到基本的logger配置
		logger = logrus.New()
		level, parseErr := logrus.ParseLevel(options.LogLevel)
		if parseErr != nil {
			level = logrus.InfoLevel
		}
		logger.SetLevel(level)
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05",
		})
		logger.Warnf("Failed to create configured logger, using basic logger: %v", err)
	}

	return logger, nil
}

func parseGenerateDataFlags(dbType string, args []string) (*GenerateDataOptions, error) {
	fs := flag.NewFlagSet("generate_data", flag.ContinueOnError)

	options := &GenerateDataOptions{
		DatabaseType: dbType,
		Rows:         10000,  // Default 10k rows
		BatchSize:    1000,   // Default 1k batch
		LogLevel:     "info", // Default log level
	}

	fs.StringVar(&options.SQLEndpoint, "sql_endpoint", "", "Database SQL endpoint")
	fs.IntVar(&options.SQLPort, "sql_port", 0, "Database SQL port")
	fs.StringVar(&options.Username, "sql_auth_username", "", "Database username")
	fs.StringVar(&options.Password, "sql_auth_password", "", "Database password")
	fs.StringVar(&options.Database, "db", "", "Database name")
	fs.StringVar(&options.Table, "table", "", "Table name")
	fs.IntVar(&options.Rows, "rows", 10000, "Number of rows to generate")
	fs.IntVar(&options.BatchSize, "batch_size", 1000, "Batch size for inserts")
	fs.StringVar(&options.LogLevel, "log_level", "info", "Log level (panic, fatal, error, warn, info, debug, trace)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	// Validate required parameters
	if options.SQLEndpoint == "" {
		return nil, fmt.Errorf("--sql_endpoint is required")
	}
	if options.SQLPort == 0 {
		return nil, fmt.Errorf("--sql_port is required")
	}
	if options.Database == "" {
		return nil, fmt.Errorf("--db is required")
	}
	if options.Table == "" {
		return nil, fmt.Errorf("--table is required")
	}

	return options, nil
}

type DataGenerator struct {
	logger  *logrus.Logger
	options *GenerateDataOptions
	ctx     context.Context
	db      *sql.DB
	schema  []ColumnInfo // 表的schema信息
}

func (g *DataGenerator) Generate() error {
	g.logger.Infof("Starting data generation for %s", g.options.DatabaseType)
	g.logger.Infof("Target: %s:%d/%s.%s",
		g.options.SQLEndpoint, g.options.SQLPort, g.options.Database, g.options.Table)
	g.logger.Infof("Generating %d rows with batch size %d", g.options.Rows, g.options.BatchSize)

	// Check context before starting
	select {
	case <-g.ctx.Done():
		g.logger.Warnf("Data generation cancelled before starting")
		return g.ctx.Err()
	default:
	}

	// Connect to database
	if err := g.connect(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer g.db.Close()

	// Create database if not exists
	if err := g.createDatabaseIfNotExists(); err != nil {
		return fmt.Errorf("failed to create database: %w", err)
	}

	// Create table if not exists
	if err := g.createTableIfNotExists(); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Read table schema
	if err := g.readTableSchema(); err != nil {
		return fmt.Errorf("failed to read table schema: %w", err)
	}

	// Generate and insert data
	if err := g.generateAndInsertData(); err != nil {
		if g.ctx.Err() != nil {
			g.logger.Warnf("Data generation interrupted: %v", g.ctx.Err())
			return g.ctx.Err()
		}
		return fmt.Errorf("failed to generate data: %w", err)
	}

	g.logger.Infof("Data generation completed successfully")
	return nil
}

func (g *DataGenerator) connect() error {
	dsn := fmt.Sprintf("tcp(%s:%d)/",
		g.options.SQLEndpoint, g.options.SQLPort)
	if g.options.Username != "" {
		dsn = fmt.Sprintf("%s:%s@%s", g.options.Username, g.options.Password, dsn)
	}
	var err error
	g.db, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}

	// Test connection with context
	if err := g.db.PingContext(g.ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	g.logger.Infof("Connected to %s database successfully", g.options.DatabaseType)
	return nil
}

func (g *DataGenerator) createDatabaseIfNotExists() error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", g.options.Database)

	g.logger.Infof("Creating database if not exists: %s", g.options.Database)
	if _, err := g.db.ExecContext(g.ctx, query); err != nil {
		return err
	}

	// Switch to the database
	switchQuery := fmt.Sprintf("USE `%s`", g.options.Database)
	if _, err := g.db.ExecContext(g.ctx, switchQuery); err != nil {
		return err
	}

	g.logger.Infof("Database %s is ready", g.options.Database)
	return nil
}

func (g *DataGenerator) createTableIfNotExists() error {
	var createSQL string

	switch g.options.DatabaseType {
	case "starrocks":
		createSQL = g.getStarRocksCreateTableSQL()
	case "clickhouse":
		createSQL = g.getClickHouseCreateTableSQL()
	}

	g.logger.Infof("Creating table if not exists: %s", g.options.Table)
	g.logger.Debugf("Create table SQL: %s", createSQL)

	if _, err := g.db.ExecContext(g.ctx, createSQL); err != nil {
		return err
	}

	g.logger.Infof("Table %s is ready", g.options.Table)
	return nil
}

func (g *DataGenerator) getStarRocksCreateTableSQL() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id BIGINT,
		name VARCHAR(100),
		age INT,
		salary DECIMAL(10, 2),
		score FLOAT,
		weight DOUBLE,
		is_active BOOLEAN,
		birth_date DATE,
		created_at DATETIME,
		updated_timestamp DATETIME,
		description TEXT,
		metadata JSON,
		category CHAR(10),
		small_num TINYINT,
		medium_num SMALLINT,
		big_num BIGINT
	) ENGINE=OLAP
	DUPLICATE KEY(id)
	DISTRIBUTED BY HASH(id) BUCKETS 3
	PROPERTIES (
		"replication_num" = "1"
	)`, g.options.Table)
}

func (g *DataGenerator) getClickHouseCreateTableSQL() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id UInt64,
		name String,
		age UInt32,
		salary Decimal(10, 2),
		score Float32,
		weight Float64,
		is_active UInt8,
		birth_date Date,
		created_at DateTime,
		updated_timestamp DateTime64,
		description String,
		metadata String,
		category FixedString(10),
		small_num UInt8,
		medium_num UInt16,
		big_num UInt64
	) ENGINE = MergeTree()
	ORDER BY id`, g.options.Table)
}

// readTableSchema 读取表的schema信息
func (g *DataGenerator) readTableSchema() error {
	var query string
	var rows *sql.Rows
	var err error

	switch g.options.DatabaseType {
	case "starrocks":
		query = `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
				 FROM INFORMATION_SCHEMA.COLUMNS
				 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
				 ORDER BY ORDINAL_POSITION`
		g.logger.Infof("Reading table schema for %s.%s", g.options.Database, g.options.Table)
		rows, err = g.db.QueryContext(g.ctx, query, g.options.Database, g.options.Table)
	case "clickhouse":
		// ClickHouse system表不支持参数化查询，需要直接拼接
		// 过滤掉ALIAS类型的列，因为它们是虚拟列，不能插入数据
		query = fmt.Sprintf(`SELECT name, type,
				        CASE WHEN type LIKE '%%Nullable%%' THEN 'YES' ELSE 'NO' END as is_nullable,
				        default_expression,
				        NULL as character_maximum_length,
				        NULL as numeric_precision,
				        NULL as numeric_scale
				 FROM system.columns
				 WHERE database = '%s' AND table = '%s' AND default_kind != 'ALIAS'
				 ORDER BY position`, g.options.Database, g.options.Table)
		g.logger.Infof("Reading table schema for %s.%s", g.options.Database, g.options.Table)
		g.logger.Debugf("ClickHouse schema query: %s", query)
		rows, err = g.db.QueryContext(g.ctx, query)
	default:
		return fmt.Errorf("unsupported database type: %s", g.options.DatabaseType)
	}

	if err != nil {
		return fmt.Errorf("failed to query table schema: %w", err)
	}
	defer rows.Close()

	var schema []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var nullable string
		var defaultVal sql.NullString
		var charLength sql.NullInt64
		var numPrecision sql.NullInt64
		var numScale sql.NullInt64

		err := rows.Scan(&col.Name, &col.DataType, &nullable, &defaultVal,
			&charLength, &numPrecision, &numScale)
		if err != nil {
			return fmt.Errorf("failed to scan column info: %w", err)
		}

		col.IsNullable = nullable == "YES"
		if defaultVal.Valid {
			col.DefaultValue = &defaultVal.String
		}
		if charLength.Valid {
			col.CharLength = &charLength.Int64
		}
		if numPrecision.Valid {
			col.NumPrecision = &numPrecision.Int64
		}
		if numScale.Valid {
			col.NumScale = &numScale.Int64
		}

		schema = append(schema, col)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading table schema: %w", err)
	}

	if len(schema) == 0 {
		return fmt.Errorf("table %s.%s not found or has no columns", g.options.Database, g.options.Table)
	}

	g.schema = schema
	g.logger.Infof("Successfully read schema for table %s with %d columns", g.options.Table, len(g.schema))

	// 输出schema信息用于调试
	for i, col := range g.schema {
		g.logger.Debugf("Column %d: %s %s (nullable: %v)", i+1, col.Name, col.DataType, col.IsNullable)
	}

	return nil
}

func (g *DataGenerator) generateAndInsertData() error {
	startTime := time.Now()
	totalBatches := (g.options.Rows + g.options.BatchSize - 1) / g.options.BatchSize

	g.logger.Infof("Starting data generation: %d batches of %d rows each", totalBatches, g.options.BatchSize)

	for batch := 0; batch < totalBatches; batch++ {
		// Check for cancellation before each batch
		select {
		case <-g.ctx.Done():
			g.logger.Warnf("Data generation cancelled after %d batches", batch)
			return g.ctx.Err()
		default:
		}

		batchStartTime := time.Now()

		// Calculate actual batch size for the last batch
		remainingRows := g.options.Rows - (batch * g.options.BatchSize)
		currentBatchSize := g.options.BatchSize
		if remainingRows < g.options.BatchSize {
			currentBatchSize = remainingRows
		}

		if err := g.insertBatch(batch, currentBatchSize); err != nil {
			if g.ctx.Err() != nil {
				g.logger.Warnf("Batch insert cancelled: %v", g.ctx.Err())
				return g.ctx.Err()
			}
			return fmt.Errorf("failed to insert batch %d: %w", batch+1, err)
		}

		batchDuration := time.Since(batchStartTime)
		processedRows := (batch + 1) * g.options.BatchSize
		if processedRows > g.options.Rows {
			processedRows = g.options.Rows
		}

		g.logger.Infof("Batch %d/%d completed in %v (Total: %d/%d rows, %.1f%%)",
			batch+1, totalBatches, batchDuration,
			processedRows, g.options.Rows,
			float64(processedRows)/float64(g.options.Rows)*100)
	}

	totalDuration := time.Since(startTime)
	rowsPerSecond := float64(g.options.Rows) / totalDuration.Seconds()

	g.logger.Infof("Data generation completed: %d rows in %v (%.0f rows/sec)",
		g.options.Rows, totalDuration, rowsPerSecond)

	return nil
}

func (g *DataGenerator) insertBatch(batchNum, batchSize int) error {
	baseID := int64(batchNum * g.options.BatchSize)

	// 生成基于schema的批次数据
	batchData, err := g.generateBatchData(baseID, batchSize)
	if err != nil {
		return fmt.Errorf("failed to generate batch data: %w", err)
	}

	// 根据数据库类型使用不同的插入策略
	switch g.options.DatabaseType {
	case "clickhouse":
		return g.insertClickHouseBatch(batchData)
	case "starrocks":
		return g.insertStarRocksBatch(batchData)
	default:
		return fmt.Errorf("unsupported database type: %s", g.options.DatabaseType)
	}
}

// generateBatchData 基于schema生成批次数据
func (g *DataGenerator) generateBatchData(baseID int64, batchSize int) ([][]interface{}, error) {
	if len(g.schema) == 0 {
		return nil, fmt.Errorf("table schema not loaded")
	}

	var batchData [][]interface{}

	for i := 0; i < batchSize; i++ {
		rowID := baseID + int64(i)
		rowData, err := g.generateRowDataBySchema(rowID)
		if err != nil {
			return nil, fmt.Errorf("failed to generate row data for ID %d: %w", rowID, err)
		}
		batchData = append(batchData, rowData)
	}

	return batchData, nil
}

// generateRowDataBySchema 根据schema生成单行数据
func (g *DataGenerator) generateRowDataBySchema(id int64) ([]interface{}, error) {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + id))
	var rowData []interface{}

	for _, col := range g.schema {
		value, err := g.generateValueForColumn(col, id, r)
		if err != nil {
			return nil, fmt.Errorf("failed to generate value for column %s: %w", col.Name, err)
		}
		rowData = append(rowData, value)
	}

	return rowData, nil
}

// generateValueForColumn 根据列定义生成值
func (g *DataGenerator) generateValueForColumn(col ColumnInfo, id int64, r *rand.Rand) (interface{}, error) {
	// 处理可空字段，10%概率为null
	if col.IsNullable && r.Float32() < 0.1 {
		return nil, nil
	}

	// 根据数据库类型和列类型生成相应的值
	switch g.options.DatabaseType {
	case "starrocks":
		return g.generateStarRocksValue(col, id, r)
	case "clickhouse":
		return g.generateClickHouseValue(col, id, r)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", g.options.DatabaseType)
	}
}

// generateStarRocksValue 为StarRocks生成值
func (g *DataGenerator) generateStarRocksValue(col ColumnInfo, id int64, r *rand.Rand) (interface{}, error) {
	dataType := strings.ToUpper(col.DataType)

	switch {
	case strings.Contains(dataType, "BIGINT"):
		return id, nil
	case strings.Contains(dataType, "INT"):
		return int32(20 + r.Intn(60)), nil
	case strings.Contains(dataType, "SMALLINT"):
		return int16(r.Intn(1000)), nil
	case strings.Contains(dataType, "TINYINT"):
		if strings.Contains(col.Name, "active") {
			return r.Intn(2), nil
		}
		return r.Intn(256), nil
	case strings.Contains(dataType, "VARCHAR"), strings.Contains(dataType, "TEXT"):
		return g.generateStringValue(col, id, r), nil
	case strings.Contains(dataType, "CHAR"):
		return g.generateFixedStringValue(col, id, r), nil
	case strings.Contains(dataType, "DECIMAL"):
		precision := int64(10)
		scale := int64(2)
		if col.NumPrecision != nil {
			precision = *col.NumPrecision
		}
		if col.NumScale != nil {
			scale = *col.NumScale
		}
		return g.generateDecimalValue(precision, scale, r), nil
	case strings.Contains(dataType, "FLOAT"):
		return r.Float32() * 100, nil
	case strings.Contains(dataType, "DOUBLE"):
		return 50.0 + r.Float64()*100, nil
	case strings.Contains(dataType, "BOOLEAN"):
		return r.Intn(2) == 1, nil
	case strings.Contains(dataType, "DATE"):
		return g.generateDate(r), nil
	case strings.Contains(dataType, "DATETIME"), strings.Contains(dataType, "TIMESTAMP"):
		return g.generateDateTime(r), nil
	case strings.Contains(dataType, "JSON"):
		return g.generateJSONValue(col, id, r), nil
	default:
		// 默认生成字符串
		return g.generateStringValue(col, id, r), nil
	}
}

// generateClickHouseValue 为ClickHouse生成值
func (g *DataGenerator) generateClickHouseValue(col ColumnInfo, id int64, r *rand.Rand) (interface{}, error) {
	dataType := strings.ToUpper(col.DataType)

	switch {
	case strings.Contains(dataType, "UINT64"):
		return uint64(id), nil
	case strings.Contains(dataType, "UINT32"):
		return uint32(20 + r.Intn(60)), nil
	case strings.Contains(dataType, "UINT16"):
		return uint16(r.Intn(65536)), nil
	case strings.Contains(dataType, "UINT8"):
		return uint8(r.Intn(256)), nil
	case strings.Contains(dataType, "INT64"):
		return int64(id), nil
	case strings.Contains(dataType, "INT32"):
		return int32(20 + r.Intn(60)), nil
	case strings.Contains(dataType, "INT16"):
		return int16(r.Intn(1000)), nil
	case strings.Contains(dataType, "INT8"):
		return int8(r.Intn(256)), nil
	case strings.Contains(dataType, "STRING"):
		return g.generateStringValue(col, id, r), nil
	case strings.Contains(dataType, "FIXEDSTRING"):
		return g.generateFixedStringValue(col, id, r), nil
	case strings.Contains(dataType, "DECIMAL"):
		precision := int64(10)
		scale := int64(2)
		if col.NumPrecision != nil {
			precision = *col.NumPrecision
		}
		if col.NumScale != nil {
			scale = *col.NumScale
		}
		return g.generateDecimalValue(precision, scale, r), nil
	case strings.Contains(dataType, "FLOAT32"):
		return r.Float32() * 100, nil
	case strings.Contains(dataType, "FLOAT64"):
		return r.Float64() * 100, nil
	case strings.Contains(dataType, "DATE"):
		return g.generateDate(r), nil
	case strings.Contains(dataType, "DATETIME"), strings.Contains(dataType, "DATETIME64"):
		return g.generateDateTime(r), nil
	default:
		// 默认生成字符串
		return g.generateStringValue(col, id, r), nil
	}
}

// 辅助方法生成特定类型的值
func (g *DataGenerator) generateStringValue(col ColumnInfo, id int64, r *rand.Rand) string {
	switch {
	case strings.Contains(strings.ToLower(col.Name), "name"):
		return fmt.Sprintf("user_%d", id)
	case strings.Contains(strings.ToLower(col.Name), "description"):
		return fmt.Sprintf("Description for user %d with random content %d", id, r.Intn(1000))
	case strings.Contains(strings.ToLower(col.Name), "category"):
		return fmt.Sprintf("CAT%d", r.Intn(10))
	case strings.Contains(strings.ToLower(col.Name), "metadata"):
		return fmt.Sprintf(`{"user_id": %d, "level": %d, "tags": ["tag1", "tag2"]}`, id, r.Intn(10))
	default:
		length := 20
		if col.CharLength != nil && *col.CharLength < 100 {
			length = int(*col.CharLength)
		}
		return g.generateRandomString(length, r)
	}
}

func (g *DataGenerator) generateFixedStringValue(col ColumnInfo, id int64, r *rand.Rand) string {
	if strings.Contains(strings.ToLower(col.Name), "category") {
		value := fmt.Sprintf("CAT%d", r.Intn(10))
		if col.CharLength != nil {
			// 填充到固定长度
			for len(value) < int(*col.CharLength) {
				value += "\x00"
			}
		}
		return value
	}

	length := 10
	if col.CharLength != nil {
		length = int(*col.CharLength)
	}
	return g.generateRandomString(length, r)
}

func (g *DataGenerator) generateDecimalValue(precision, scale int64, r *rand.Rand) float64 {
	maxValue := 1.0
	for i := int64(0); i < precision-scale; i++ {
		maxValue *= 10
	}
	return r.Float64() * maxValue
}

func (g *DataGenerator) generateDate(r *rand.Rand) time.Time {
	return time.Date(1980+r.Intn(40), time.Month(1+r.Intn(12)), 1+r.Intn(28), 0, 0, 0, 0, time.UTC)
}

func (g *DataGenerator) generateDateTime(r *rand.Rand) time.Time {
	return time.Now()
}

func (g *DataGenerator) generateJSONValue(col ColumnInfo, id int64, r *rand.Rand) string {
	return fmt.Sprintf(`{"user_id": %d, "level": %d, "tags": ["tag1", "tag2"]}`, id, r.Intn(10))
}

func (g *DataGenerator) generateRandomString(length int, r *rand.Rand) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

// formatValueForClickHouse 为ClickHouse格式化值
func (g *DataGenerator) formatValueForClickHouse(value interface{}, col ColumnInfo) (string, error) {
	if value == nil {
		return "NULL", nil
	}

	dataType := strings.ToUpper(col.DataType)

	switch v := value.(type) {
	case string:
		// 转义单引号
		escaped := strings.ReplaceAll(v, "'", "\\'")
		return fmt.Sprintf("'%s'", escaped), nil
	case time.Time:
		if strings.Contains(dataType, "DATE") && !strings.Contains(dataType, "DATETIME") {
			return fmt.Sprintf("'%s'", v.Format("2006-01-02")), nil
		}
		return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05")), nil
	case bool:
		if v {
			return "1", nil
		}
		return "0", nil
	case float32, float64:
		return fmt.Sprintf("%v", v), nil
	default:
		// 数值类型直接返回
		return fmt.Sprintf("%v", v), nil
	}
}

// formatValueForStarRocks 为StarRocks格式化值
func (g *DataGenerator) formatValueForStarRocks(value interface{}, col ColumnInfo) (string, error) {
	if value == nil {
		return "NULL", nil
	}

	dataType := strings.ToUpper(col.DataType)

	switch v := value.(type) {
	case string:
		// 转义单引号
		escaped := strings.ReplaceAll(v, "'", "\\'")
		return fmt.Sprintf("'%s'", escaped), nil
	case time.Time:
		if strings.Contains(dataType, "DATE") && !strings.Contains(dataType, "DATETIME") {
			return fmt.Sprintf("'%s'", v.Format("2006-01-02")), nil
		}
		return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05")), nil
	case bool:
		return fmt.Sprintf("%t", v), nil
	case float32, float64:
		return fmt.Sprintf("%v", v), nil
	default:
		// 数值类型直接返回
		return fmt.Sprintf("%v", v), nil
	}
}

// insertClickHouseBatch ClickHouse批量插入
func (g *DataGenerator) insertClickHouseBatch(batchData [][]interface{}) error {
	if len(batchData) == 0 || len(g.schema) == 0 {
		return fmt.Errorf("no data or schema available for insert")
	}

	// 构建列名列表
	var columnNames []string
	for _, col := range g.schema {
		columnNames = append(columnNames, fmt.Sprintf("`%s`", col.Name))
	}

	// 构建VALUES子句
	var valuesParts []string
	for _, rowData := range batchData {
		if len(rowData) != len(g.schema) {
			return fmt.Errorf("row data length (%d) doesn't match schema length (%d)", len(rowData), len(g.schema))
		}

		var values []string
		for i, value := range rowData {
			col := g.schema[i]
			formattedValue, err := g.formatValueForClickHouse(value, col)
			if err != nil {
				return fmt.Errorf("failed to format value for column %s: %w", col.Name, err)
			}
			values = append(values, formattedValue)
		}

		valuesStr := fmt.Sprintf("(%s)", strings.Join(values, ", "))
		valuesParts = append(valuesParts, valuesStr)
	}

	insertSQL := fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s`,
		g.options.Table,
		strings.Join(columnNames, ", "),
		strings.Join(valuesParts, ", "))

	g.logger.Debugf("ClickHouse Insert SQL: %s", insertSQL)

	if _, err := g.db.ExecContext(g.ctx, insertSQL); err != nil {
		return err
	}

	return nil
}

// insertStarRocksBatch StarRocks批量插入
func (g *DataGenerator) insertStarRocksBatch(batchData [][]interface{}) error {
	if len(batchData) == 0 || len(g.schema) == 0 {
		return fmt.Errorf("no data or schema available for insert")
	}

	// 构建列名列表
	var columnNames []string
	for _, col := range g.schema {
		columnNames = append(columnNames, fmt.Sprintf("`%s`", col.Name))
	}

	// 构建VALUES子句
	var valuesParts []string
	for _, rowData := range batchData {
		if len(rowData) != len(g.schema) {
			return fmt.Errorf("row data length (%d) doesn't match schema length (%d)", len(rowData), len(g.schema))
		}

		var values []string
		for i, value := range rowData {
			col := g.schema[i]
			formattedValue, err := g.formatValueForStarRocks(value, col)
			if err != nil {
				return fmt.Errorf("failed to format value for column %s: %w", col.Name, err)
			}
			values = append(values, formattedValue)
		}

		valuesStr := fmt.Sprintf("(%s)", strings.Join(values, ", "))
		valuesParts = append(valuesParts, valuesStr)
	}

	insertSQL := fmt.Sprintf(`INSERT INTO %s (%s) VALUES %s`,
		g.options.Table,
		strings.Join(columnNames, ", "),
		strings.Join(valuesParts, ", "))

	g.logger.Debugf("StarRocks Insert SQL: %s", insertSQL)

	if _, err := g.db.ExecContext(g.ctx, insertSQL); err != nil {
		return err
	}

	return nil
}
