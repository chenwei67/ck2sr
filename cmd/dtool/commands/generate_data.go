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
// 支持复杂类型：ARRAY、MAP、JSON、STRUCT
func (g *DataGenerator) generateStarRocksValue(col ColumnInfo, id int64, r *rand.Rand) (interface{}, error) {
	dataType := col.DataType // 保留原始大小写用于精确匹配
	dataTypeUpper := strings.ToUpper(dataType)

	// P0: 处理 ARRAY 类型（StarRocks使用ARRAY<T>语法）
	if strings.HasPrefix(dataTypeUpper, "ARRAY<") {
		return g.generateStarRocksArrayValue(dataType, id, r)
	}

	// P0: 处理 MAP 类型（StarRocks使用MAP<K,V>语法）
	if strings.HasPrefix(dataTypeUpper, "MAP<") {
		return g.generateStarRocksMapValue(dataType, id, r)
	}

	// P0: 处理 STRUCT 类型（类似ClickHouse的Tuple）
	if strings.HasPrefix(dataTypeUpper, "STRUCT<") {
		return g.generateStarRocksStructValue(dataType, id, r)
	}

	// P0: 处理 JSON 类型
	if dataTypeUpper == "JSON" || strings.Contains(dataTypeUpper, "JSON") {
		return g.generateJSONValue(col, id, r), nil
	}

	// 基础类型处理
	switch {
	case strings.Contains(dataTypeUpper, "BIGINT"):
		return time.Now().Unix(), nil
	case strings.Contains(dataTypeUpper, "INT"):
		return int32(20 + r.Intn(60)), nil
	case strings.Contains(dataTypeUpper, "SMALLINT"):
		return int16(r.Intn(1000)), nil
	case strings.Contains(dataTypeUpper, "TINYINT"):
		if strings.Contains(col.Name, "active") {
			return r.Intn(2), nil
		}
		return r.Intn(256), nil
	case strings.Contains(dataTypeUpper, "VARCHAR"), strings.Contains(dataTypeUpper, "TEXT"), strings.Contains(dataTypeUpper, "STRING"):
		return g.generateStringValue(col, id, r), nil
	case strings.Contains(dataTypeUpper, "CHAR"):
		return g.generateFixedStringValue(col, id, r), nil
	case strings.Contains(dataTypeUpper, "DECIMAL"):
		precision := int64(10)
		scale := int64(2)
		if col.NumPrecision != nil {
			precision = *col.NumPrecision
		}
		if col.NumScale != nil {
			scale = *col.NumScale
		}
		return g.generateDecimalValue(precision, scale, r), nil
	case strings.Contains(dataTypeUpper, "FLOAT"):
		return r.Float32() * 100, nil
	case strings.Contains(dataTypeUpper, "DOUBLE"):
		return 50.0 + r.Float64()*100, nil
	case strings.Contains(dataTypeUpper, "BOOLEAN"):
		return r.Intn(2) == 1, nil
	case strings.Contains(dataTypeUpper, "DATE"):
		return g.generateDate(r), nil
	case strings.Contains(dataTypeUpper, "DATETIME"), strings.Contains(dataTypeUpper, "TIMESTAMP"):
		return g.generateDateTime(r), nil
	default:
		return nil, fmt.Errorf("unsupported StarRocks data type: %s for column: %s", dataType, col.Name)
	}
}

// extractStarRocksInnerType 提取StarRocks类型的内部类型
// StarRocks使用<>而不是()，例如：ARRAY<INT> -> INT, MAP<STRING,INT> -> STRING,INT
func extractStarRocksInnerType(dataType, wrapper string) string {
	upperWrapper := strings.ToUpper(wrapper)
	upperDataType := strings.ToUpper(dataType)
	prefix := upperWrapper + "<"

	if !strings.HasPrefix(upperDataType, prefix) {
		return ""
	}

	// 从原始类型中提取（保留大小写）
	startIdx := len(prefix)
	// 找到匹配的右尖括号
	depth := 1
	endIdx := startIdx
	for i := startIdx; i < len(dataType); i++ {
		if dataType[i] == '<' {
			depth++
		} else if dataType[i] == '>' {
			depth--
			if depth == 0 {
				endIdx = i
				break
			}
		}
	}

	if endIdx <= startIdx {
		return ""
	}

	return strings.TrimSpace(dataType[startIdx:endIdx])
}

// generateStarRocksArrayValue 生成 ARRAY 类型的值（支持嵌套）
func (g *DataGenerator) generateStarRocksArrayValue(dataType string, id int64, r *rand.Rand) (interface{}, error) {
	innerType := extractStarRocksInnerType(dataType, "ARRAY")
	if innerType == "" {
		return nil, fmt.Errorf("failed to extract inner type from ARRAY: %s", dataType)
	}

	// 生成数组长度（2-5个元素）
	arrLen := 2 + r.Intn(4)

	// 根据内部类型生成数组元素
	innerCol := ColumnInfo{
		Name:     "array_element",
		DataType: innerType,
	}

	var result []interface{}
	for i := 0; i < arrLen; i++ {
		value, err := g.generateStarRocksValue(innerCol, id, r)
		if err != nil {
			return nil, fmt.Errorf("failed to generate array element: %w", err)
		}
		result = append(result, value)
	}

	return result, nil
}

// generateStarRocksMapValue 生成 MAP 类型的值
func (g *DataGenerator) generateStarRocksMapValue(dataType string, id int64, r *rand.Rand) (interface{}, error) {
	innerPart := extractStarRocksInnerType(dataType, "MAP")
	if innerPart == "" {
		return nil, fmt.Errorf("failed to extract inner types from MAP: %s", dataType)
	}

	// 分割 Key 和 Value 类型（StarRocks使用逗号分隔）
	keyType, valueType, err := splitStarRocksMapTypes(innerPart)
	if err != nil {
		return nil, fmt.Errorf("failed to parse MAP types: %w", err)
	}

	// 生成 3-5 个键值对
	mapSize := 3 + r.Intn(3)
	resultMap := make(map[interface{}]interface{})

	keyCol := ColumnInfo{Name: "map_key", DataType: keyType}
	valueCol := ColumnInfo{Name: "map_value", DataType: valueType}

	for i := 0; i < mapSize; i++ {
		key, err := g.generateStarRocksValue(keyCol, id+int64(i), r)
		if err != nil {
			return nil, fmt.Errorf("failed to generate map key: %w", err)
		}
		value, err := g.generateStarRocksValue(valueCol, id, r)
		if err != nil {
			return nil, fmt.Errorf("failed to generate map value: %w", err)
		}
		resultMap[key] = value
	}

	return resultMap, nil
}

// splitStarRocksMapTypes 分割 MAP 的 Key 和 Value 类型
func splitStarRocksMapTypes(innerPart string) (string, string, error) {
	depth := 0
	commaIdx := -1

	for i := 0; i < len(innerPart); i++ {
		switch innerPart[i] {
		case '<':
			depth++
		case '>':
			depth--
		case ',':
			if depth == 0 {
				commaIdx = i
				break
			}
		}
		if commaIdx != -1 {
			break
		}
	}

	if commaIdx == -1 {
		return "", "", fmt.Errorf("invalid MAP type format: %s", innerPart)
	}

	keyType := strings.TrimSpace(innerPart[:commaIdx])
	valueType := strings.TrimSpace(innerPart[commaIdx+1:])

	if keyType == "" || valueType == "" {
		return "", "", fmt.Errorf("empty key or value type in MAP: %s", innerPart)
	}

	return keyType, valueType, nil
}

// generateStarRocksStructValue 生成 STRUCT 类型的值
// STRUCT<field1:TYPE1, field2:TYPE2>
func (g *DataGenerator) generateStarRocksStructValue(dataType string, id int64, r *rand.Rand) (interface{}, error) {
	innerPart := extractStarRocksInnerType(dataType, "STRUCT")
	if innerPart == "" {
		return nil, fmt.Errorf("failed to extract inner types from STRUCT: %s", dataType)
	}

	// 解析 STRUCT 中的字段定义
	fields, err := splitStarRocksStructFields(innerPart)
	if err != nil {
		return nil, fmt.Errorf("failed to parse STRUCT fields: %w", err)
	}

	// STRUCT在StarRocks中表示为JSON对象
	result := make(map[string]interface{})
	for fieldName, fieldType := range fields {
		fieldCol := ColumnInfo{
			Name:     fieldName,
			DataType: fieldType,
		}
		value, err := g.generateStarRocksValue(fieldCol, id, r)
		if err != nil {
			return nil, fmt.Errorf("failed to generate struct field %s: %w", fieldName, err)
		}
		result[fieldName] = value
	}

	return result, nil
}

// splitStarRocksStructFields 分割 STRUCT 中的字段定义
// 例如：field1:INT, field2:STRING -> {"field1": "INT", "field2": "STRING"}
func splitStarRocksStructFields(innerPart string) (map[string]string, error) {
	fields := make(map[string]string)
	depth := 0
	start := 0

	for i := 0; i <= len(innerPart); i++ {
		if i < len(innerPart) {
			switch innerPart[i] {
			case '<':
				depth++
			case '>':
				depth--
			case ',':
				if depth == 0 {
					fieldDef := strings.TrimSpace(innerPart[start:i])
					name, typ, err := parseStarRocksStructField(fieldDef)
					if err != nil {
						return nil, err
					}
					fields[name] = typ
					start = i + 1
				}
			}
		} else {
			// 处理最后一个字段
			if start < len(innerPart) {
				fieldDef := strings.TrimSpace(innerPart[start:])
				name, typ, err := parseStarRocksStructField(fieldDef)
				if err != nil {
					return nil, err
				}
				fields[name] = typ
			}
		}
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("no fields found in STRUCT: %s", innerPart)
	}

	return fields, nil
}

// parseStarRocksStructField 解析单个STRUCT字段定义
// 例如：field_name:INT -> ("field_name", "INT")
func parseStarRocksStructField(fieldDef string) (string, string, error) {
	colonIdx := strings.Index(fieldDef, ":")
	if colonIdx == -1 {
		return "", "", fmt.Errorf("invalid STRUCT field definition: %s", fieldDef)
	}

	fieldName := strings.TrimSpace(fieldDef[:colonIdx])
	fieldType := strings.TrimSpace(fieldDef[colonIdx+1:])

	if fieldName == "" || fieldType == "" {
		return "", "", fmt.Errorf("empty field name or type in: %s", fieldDef)
	}

	return fieldName, fieldType, nil
}

// generateClickHouseValue 为ClickHouse生成值
// 支持复杂类型：Array、Map、JSON、Tuple、Nullable、嵌套类型组合
func (g *DataGenerator) generateClickHouseValue(col ColumnInfo, id int64, r *rand.Rand) (any, error) {
	dataType := col.DataType // 保留原始大小写用于精确匹配
	dataTypeUpper := strings.ToUpper(dataType)

	// P1: 处理 Nullable 包装类型
	if strings.HasPrefix(dataTypeUpper, "NULLABLE(") {
		// 10% 概率返回 NULL
		if r.Float32() < 0.1 {
			return nil, nil
		}
		// 提取内部类型并递归生成
		innerType := extractInnerType(dataType, "Nullable")
		if innerType == "" {
			return nil, fmt.Errorf("failed to extract inner type from Nullable: %s", dataType)
		}
		innerCol := ColumnInfo{
			Name:         col.Name,
			DataType:     innerType,
			IsNullable:   false,
			DefaultValue: col.DefaultValue,
			CharLength:   col.CharLength,
			NumPrecision: col.NumPrecision,
			NumScale:     col.NumScale,
		}
		return g.generateClickHouseValue(innerCol, id, r)
	}

	// P0: 处理 Array 类型（支持嵌套）
	if strings.HasPrefix(dataTypeUpper, "ARRAY(") {
		return g.generateArrayValue(dataType, id, r)
	}

	// P0: 处理 Map 类型
	if strings.HasPrefix(dataTypeUpper, "MAP(") {
		return g.generateMapValue(dataType, id, r)
	}

	// P0: 处理 Tuple 类型
	if strings.HasPrefix(dataTypeUpper, "TUPLE(") {
		return g.generateTupleValue(dataType, id, r)
	}

	// P0: 处理 JSON 类型
	if dataTypeUpper == "JSON" || strings.Contains(dataTypeUpper, "JSON") {
		return g.generateJSONValue(col, id, r), nil
	}

	// 基础类型处理
	switch {
	case strings.Contains(dataTypeUpper, "UINT64"):
		return uint64(id), nil
	case strings.Contains(dataTypeUpper, "UINT32"):
		return uint32(20 + r.Intn(60)), nil
	case strings.Contains(dataTypeUpper, "UINT16"):
		return uint16(r.Intn(65536)), nil
	case strings.Contains(dataTypeUpper, "UINT8"):
		return uint8(r.Intn(256)), nil
	case strings.Contains(dataTypeUpper, "INT64"):
		// 特殊处理：时间戳的场景
		return int64(time.Now().Unix()), nil
	case strings.Contains(dataTypeUpper, "INT32"):
		return int32(20 + r.Intn(60)), nil
	case strings.Contains(dataTypeUpper, "INT16"):
		return int16(r.Intn(1000)), nil
	case strings.Contains(dataTypeUpper, "INT8"):
		return int8(r.Intn(256)), nil
	case strings.Contains(dataTypeUpper, "STRING"):
		return g.generateStringValue(col, id, r), nil
	case strings.Contains(dataTypeUpper, "FIXEDSTRING"):
		return g.generateFixedStringValue(col, id, r), nil
	case strings.Contains(dataTypeUpper, "DECIMAL"):
		precision := int64(10)
		scale := int64(2)
		if col.NumPrecision != nil {
			precision = *col.NumPrecision
		}
		if col.NumScale != nil {
			scale = *col.NumScale
		}
		return g.generateDecimalValue(precision, scale, r), nil
	case strings.Contains(dataTypeUpper, "FLOAT32"):
		return r.Float32() * 100, nil
	case strings.Contains(dataTypeUpper, "FLOAT64"):
		return r.Float64() * 100, nil
	case strings.Contains(dataTypeUpper, "DATETIME"), strings.Contains(dataTypeUpper, "DATETIME64"):
		return g.generateDateTime(r), nil
	case strings.Contains(dataTypeUpper, "DATE"):
		return g.generateDate(r), nil
	case strings.Contains(dataTypeUpper, "IPV4"):
		return fmt.Sprintf("%d.%d.%d.%d", r.Intn(256), r.Intn(256), r.Intn(256), r.Intn(256)), nil
	case strings.Contains(dataTypeUpper, "IPV6"):
		return fmt.Sprintf("%x:%x:%x:%x:%x:%x:%x:%x",
			r.Intn(65536), r.Intn(65536), r.Intn(65536), r.Intn(65536),
			r.Intn(65536), r.Intn(65536), r.Intn(65536), r.Intn(65536)), nil
	case strings.Contains(dataTypeUpper, "BOOLEAN"), strings.Contains(dataTypeUpper, "BOOL"):
		return r.Intn(2) == 1, nil
	default:
		return nil, fmt.Errorf("unsupported ClickHouse data type: %s for column: %s", dataType, col.Name)
	}
}

// extractInnerType 提取包装类型的内部类型
// 例如：Nullable(String) -> String, Array(Int32) -> Int32
func extractInnerType(dataType, wrapper string) string {
	upperWrapper := strings.ToUpper(wrapper)
	upperDataType := strings.ToUpper(dataType)
	prefix := upperWrapper + "("

	if !strings.HasPrefix(upperDataType, prefix) {
		return ""
	}

	// 从原始类型中提取（保留大小写）
	startIdx := len(prefix)
	// 找到匹配的右括号
	depth := 1
	endIdx := startIdx
	for i := startIdx; i < len(dataType); i++ {
		if dataType[i] == '(' {
			depth++
		} else if dataType[i] == ')' {
			depth--
			if depth == 0 {
				endIdx = i
				break
			}
		}
	}

	if endIdx <= startIdx {
		return ""
	}

	return strings.TrimSpace(dataType[startIdx:endIdx])
}

// generateArrayValue 生成 Array 类型的值（支持嵌套）
func (g *DataGenerator) generateArrayValue(dataType string, id int64, r *rand.Rand) (interface{}, error) {
	innerType := extractInnerType(dataType, "Array")
	if innerType == "" {
		return nil, fmt.Errorf("failed to extract inner type from Array: %s", dataType)
	}

	// 生成数组长度（2-5个元素）
	arrLen := 2 + r.Intn(4)

	// 根据内部类型生成数组元素
	innerCol := ColumnInfo{
		Name:     "array_element",
		DataType: innerType,
	}

	var result []interface{}
	for i := 0; i < arrLen; i++ {
		value, err := g.generateClickHouseValue(innerCol, id, r)
		if err != nil {
			return nil, fmt.Errorf("failed to generate array element: %w", err)
		}
		result = append(result, value)
	}

	return result, nil
}

// generateMapValue 生成 Map 类型的值
func (g *DataGenerator) generateMapValue(dataType string, id int64, r *rand.Rand) (interface{}, error) {
	// 提取 Map 的 Key 和 Value 类型
	// 例如：Map(String, Int32) -> keyType=String, valueType=Int32
	innerPart := extractInnerType(dataType, "Map")
	if innerPart == "" {
		return nil, fmt.Errorf("failed to extract inner types from Map: %s", dataType)
	}

	// 分割 Key 和 Value 类型
	keyType, valueType, err := splitMapTypes(innerPart)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Map types: %w", err)
	}

	// 生成 3-5 个键值对
	mapSize := 3 + r.Intn(3)
	resultMap := make(map[interface{}]interface{})

	keyCol := ColumnInfo{Name: "map_key", DataType: keyType}
	valueCol := ColumnInfo{Name: "map_value", DataType: valueType}

	for i := 0; i < mapSize; i++ {
		key, err := g.generateClickHouseValue(keyCol, id+int64(i), r)
		if err != nil {
			return nil, fmt.Errorf("failed to generate map key: %w", err)
		}
		value, err := g.generateClickHouseValue(valueCol, id, r)
		if err != nil {
			return nil, fmt.Errorf("failed to generate map value: %w", err)
		}
		resultMap[key] = value
	}

	return resultMap, nil
}

// splitMapTypes 分割 Map 的 Key 和 Value 类型
func splitMapTypes(innerPart string) (string, string, error) {
	depth := 0
	commaIdx := -1

	for i := 0; i < len(innerPart); i++ {
		switch innerPart[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				commaIdx = i
				break
			}
		}
		if commaIdx != -1 {
			break
		}
	}

	if commaIdx == -1 {
		return "", "", fmt.Errorf("invalid Map type format: %s", innerPart)
	}

	keyType := strings.TrimSpace(innerPart[:commaIdx])
	valueType := strings.TrimSpace(innerPart[commaIdx+1:])

	if keyType == "" || valueType == "" {
		return "", "", fmt.Errorf("empty key or value type in Map: %s", innerPart)
	}

	return keyType, valueType, nil
}

// generateTupleValue 生成 Tuple 类型的值
func (g *DataGenerator) generateTupleValue(dataType string, id int64, r *rand.Rand) (interface{}, error) {
	innerPart := extractInnerType(dataType, "Tuple")
	if innerPart == "" {
		return nil, fmt.Errorf("failed to extract inner types from Tuple: %s", dataType)
	}

	// 解析 Tuple 中的所有类型
	types, err := splitTupleTypes(innerPart)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Tuple types: %w", err)
	}

	// 生成每个元素
	var result []interface{}
	for i, elemType := range types {
		elemCol := ColumnInfo{
			Name:     fmt.Sprintf("tuple_elem_%d", i),
			DataType: elemType,
		}
		value, err := g.generateClickHouseValue(elemCol, id, r)
		if err != nil {
			return nil, fmt.Errorf("failed to generate tuple element %d: %w", i, err)
		}
		result = append(result, value)
	}

	return result, nil
}

// splitTupleTypes 分割 Tuple 中的所有类型
func splitTupleTypes(innerPart string) ([]string, error) {
	var types []string
	depth := 0
	start := 0

	for i := 0; i < len(innerPart); i++ {
		switch innerPart[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				types = append(types, strings.TrimSpace(innerPart[start:i]))
				start = i + 1
			}
		}
	}

	// 添加最后一个类型
	if start < len(innerPart) {
		types = append(types, strings.TrimSpace(innerPart[start:]))
	}

	if len(types) == 0 {
		return nil, fmt.Errorf("no types found in Tuple: %s", innerPart)
	}

	return types, nil
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
	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(), r.Intn(24), r.Intn(60), r.Intn(60), 0, time.UTC)
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
// 支持复杂类型：Array、Map、Tuple、JSON
func (g *DataGenerator) formatValueForClickHouse(value interface{}, col ColumnInfo) (string, error) {
	if value == nil {
		return "NULL", nil
	}

	dataType := strings.ToUpper(col.DataType)

	switch v := value.(type) {
	case string:
		// 转义单引号和反斜杠
		escaped := strings.ReplaceAll(v, "\\", "\\\\")
		escaped = strings.ReplaceAll(escaped, "'", "\\'")
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
	case []interface{}:
		// 处理 Array 或 Tuple 类型
		if strings.HasPrefix(dataType, "TUPLE(") {
			return g.formatTupleValue(v, col.DataType)
		}
		return g.formatArrayValue(v, col.DataType)
	case map[interface{}]interface{}:
		// 处理 Map 类型
		return g.formatMapValue(v, col.DataType)
	case float32:
		return fmt.Sprintf("%v", v), nil
	case float64:
		return fmt.Sprintf("%v", v), nil
	case uint8, uint16, uint32, uint64, int8, int16, int32, int64:
		return fmt.Sprintf("%v", v), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

// formatArrayValue 格式化 Array 类型的值
// 例如：[1, 2, 3] -> '[1,2,3]', ['a', 'b'] -> "['a','b']"
func (g *DataGenerator) formatArrayValue(arr []interface{}, dataType string) (string, error) {
	if len(arr) == 0 {
		return "[]", nil
	}

	innerType := extractInnerType(dataType, "Array")
	innerCol := ColumnInfo{DataType: innerType}

	var elements []string
	for _, elem := range arr {
		formatted, err := g.formatValueForClickHouse(elem, innerCol)
		if err != nil {
			return "", fmt.Errorf("failed to format array element: %w", err)
		}
		elements = append(elements, formatted)
	}

	return fmt.Sprintf("[%s]", strings.Join(elements, ",")), nil
}

// formatMapValue 格式化 Map 类型的值
// 例如：{'key1': 1, 'key2': 2}
func (g *DataGenerator) formatMapValue(m map[interface{}]interface{}, dataType string) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}

	innerPart := extractInnerType(dataType, "Map")
	keyType, valueType, err := splitMapTypes(innerPart)
	if err != nil {
		return "", fmt.Errorf("failed to parse Map types: %w", err)
	}

	keyCol := ColumnInfo{DataType: keyType}
	valueCol := ColumnInfo{DataType: valueType}

	var pairs []string
	for k, v := range m {
		formattedKey, err := g.formatValueForClickHouse(k, keyCol)
		if err != nil {
			return "", fmt.Errorf("failed to format map key: %w", err)
		}
		formattedValue, err := g.formatValueForClickHouse(v, valueCol)
		if err != nil {
			return "", fmt.Errorf("failed to format map value: %w", err)
		}
		pairs = append(pairs, fmt.Sprintf("%s:%s", formattedKey, formattedValue))
	}

	return fmt.Sprintf("{%s}", strings.Join(pairs, ",")), nil
}

// formatTupleValue 格式化 Tuple 类型的值
// 例如：('str', 123, 45.6)
func (g *DataGenerator) formatTupleValue(tuple []interface{}, dataType string) (string, error) {
	if len(tuple) == 0 {
		return "()", nil
	}

	innerPart := extractInnerType(dataType, "Tuple")
	types, err := splitTupleTypes(innerPart)
	if err != nil {
		return "", fmt.Errorf("failed to parse Tuple types: %w", err)
	}

	if len(tuple) != len(types) {
		return "", fmt.Errorf("tuple length mismatch: got %d elements, expected %d", len(tuple), len(types))
	}

	var elements []string
	for i, elem := range tuple {
		elemCol := ColumnInfo{DataType: types[i]}
		formatted, err := g.formatValueForClickHouse(elem, elemCol)
		if err != nil {
			return "", fmt.Errorf("failed to format tuple element %d: %w", i, err)
		}
		elements = append(elements, formatted)
	}

	return fmt.Sprintf("(%s)", strings.Join(elements, ",")), nil
}

// formatValueForStarRocks 为StarRocks格式化值
// 支持复杂类型：ARRAY、MAP、STRUCT、JSON
func (g *DataGenerator) formatValueForStarRocks(value interface{}, col ColumnInfo) (string, error) {
	if value == nil {
		return "NULL", nil
	}

	dataType := strings.ToUpper(col.DataType)

	switch v := value.(type) {
	case string:
		// 转义单引号和反斜杠
		escaped := strings.ReplaceAll(v, "\\", "\\\\")
		escaped = strings.ReplaceAll(escaped, "'", "\\'")
		return fmt.Sprintf("'%s'", escaped), nil
	case time.Time:
		if strings.Contains(dataType, "DATE") && !strings.Contains(dataType, "DATETIME") {
			return fmt.Sprintf("'%s'", v.Format("2006-01-02")), nil
		}
		return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05")), nil
	case bool:
		return fmt.Sprintf("%t", v), nil
	case []interface{}:
		// 处理 ARRAY 类型
		return g.formatStarRocksArrayValue(v, col.DataType)
	case map[interface{}]interface{}:
		// 处理 MAP 类型
		return g.formatStarRocksMapValue(v, col.DataType)
	case map[string]interface{}:
		// 处理 STRUCT 类型（表示为JSON对象）
		return g.formatStarRocksStructValue(v, col.DataType)
	case float32:
		return fmt.Sprintf("%v", v), nil
	case float64:
		return fmt.Sprintf("%v", v), nil
	case uint8, uint16, uint32, uint64, int8, int16, int32, int64:
		return fmt.Sprintf("%v", v), nil
	default:
		// 数值类型直接返回
		return fmt.Sprintf("%v", v), nil
	}
}

// formatStarRocksArrayValue 格式化 ARRAY 类型的值
// 例如：[1, 2, 3] -> '[1,2,3]', ['a', 'b'] -> "['a','b']"
func (g *DataGenerator) formatStarRocksArrayValue(arr []interface{}, dataType string) (string, error) {
	if len(arr) == 0 {
		return "[]", nil
	}

	innerType := extractStarRocksInnerType(dataType, "ARRAY")
	innerCol := ColumnInfo{DataType: innerType}

	var elements []string
	for _, elem := range arr {
		formatted, err := g.formatValueForStarRocks(elem, innerCol)
		if err != nil {
			return "", fmt.Errorf("failed to format array element: %w", err)
		}
		elements = append(elements, formatted)
	}

	return fmt.Sprintf("[%s]", strings.Join(elements, ",")), nil
}

// formatStarRocksMapValue 格式化 MAP 类型的值
// 例如：{'key1': 1, 'key2': 2}
func (g *DataGenerator) formatStarRocksMapValue(m map[interface{}]interface{}, dataType string) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}

	innerPart := extractStarRocksInnerType(dataType, "MAP")
	keyType, valueType, err := splitStarRocksMapTypes(innerPart)
	if err != nil {
		return "", fmt.Errorf("failed to parse MAP types: %w", err)
	}

	keyCol := ColumnInfo{DataType: keyType}
	valueCol := ColumnInfo{DataType: valueType}

	var pairs []string
	for k, v := range m {
		formattedKey, err := g.formatValueForStarRocks(k, keyCol)
		if err != nil {
			return "", fmt.Errorf("failed to format map key: %w", err)
		}
		formattedValue, err := g.formatValueForStarRocks(v, valueCol)
		if err != nil {
			return "", fmt.Errorf("failed to format map value: %w", err)
		}
		pairs = append(pairs, fmt.Sprintf("%s:%s", formattedKey, formattedValue))
	}

	return fmt.Sprintf("{%s}", strings.Join(pairs, ",")), nil
}

// formatStarRocksStructValue 格式化 STRUCT 类型的值
// STRUCT在StarRocks中表示为JSON对象
func (g *DataGenerator) formatStarRocksStructValue(structMap map[string]interface{}, dataType string) (string, error) {
	if len(structMap) == 0 {
		return "{}", nil
	}

	innerPart := extractStarRocksInnerType(dataType, "STRUCT")
	if innerPart == "" {
		return "", fmt.Errorf("failed to extract STRUCT field types from: %s", dataType)
	}

	// 解析字段类型
	fields, err := splitStarRocksStructFields(innerPart)
	if err != nil {
		return "", fmt.Errorf("failed to parse STRUCT fields: %w", err)
	}

	var pairs []string
	for fieldName, fieldValue := range structMap {
		// 查找字段类型
		fieldType, ok := fields[fieldName]
		if !ok {
			return "", fmt.Errorf("unknown field %s in STRUCT", fieldName)
		}

		fieldCol := ColumnInfo{DataType: fieldType}
		formatted, err := g.formatValueForStarRocks(fieldValue, fieldCol)
		if err != nil {
			return "", fmt.Errorf("failed to format struct field %s: %w", fieldName, err)
		}
		pairs = append(pairs, fmt.Sprintf("'%s':%s", fieldName, formatted))
	}

	return fmt.Sprintf("{%s}", strings.Join(pairs, ",")), nil
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
		g.logger.Errorf("ClickHouse Insert Failed SQL: %s", insertSQL)
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
