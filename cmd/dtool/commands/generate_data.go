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

	"github.com/ck2sr/ck2sr/internal/logging"
	"github.com/ck2sr/ck2sr/internal/config"
)

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
		Rows:         10000, // Default 10k rows
		BatchSize:    1000,  // Default 1k batch
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

	// 根据数据库类型使用不同的插入策略
	switch g.options.DatabaseType {
	case "clickhouse":
		return g.insertClickHouseBatch(baseID, batchSize)
	case "starrocks":
		return g.insertStarRocksBatch(baseID, batchSize)
	default:
		return fmt.Errorf("unsupported database type: %s", g.options.DatabaseType)
	}
}

// insertClickHouseBatch ClickHouse批量插入
func (g *DataGenerator) insertClickHouseBatch(baseID int64, batchSize int) error {
	// 构建VALUES子句
	var valuesParts []string
	for i := 0; i < batchSize; i++ {
		rowData := g.generateRowData(baseID + int64(i))
		valuesStr := fmt.Sprintf("(%d, '%s', %d, %.2f, %f, %f, %d, '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d)",
			rowData[0],  // id
			rowData[1],  // name
			rowData[2],  // age
			rowData[3],  // salary
			rowData[4],  // score
			rowData[5],  // weight
			rowData[6],  // is_active
			rowData[7].(time.Time).Format("2006-01-02"), // birth_date
			rowData[8].(time.Time).Format("2006-01-02 15:04:05"), // created_at
			rowData[9].(time.Time).Format("2006-01-02 15:04:05"), // updated_timestamp
			strings.ReplaceAll(fmt.Sprintf("%v", rowData[10]), "'", "\\'"), // description
			strings.ReplaceAll(fmt.Sprintf("%v", rowData[11]), "'", "\\'"), // metadata
			rowData[12], // category
			rowData[13], // small_num
			rowData[14], // medium_num
			rowData[15]) // big_num
		valuesParts = append(valuesParts, valuesStr)
	}

	insertSQL := fmt.Sprintf(`INSERT INTO %s
		(id, name, age, salary, score, weight, is_active, birth_date, created_at, updated_timestamp, description, metadata, category, small_num, medium_num, big_num)
		VALUES %s`, g.options.Table, strings.Join(valuesParts, ", "))

	if _, err := g.db.ExecContext(g.ctx, insertSQL); err != nil {
		return err
	}

	return nil
}

// insertStarRocksBatch StarRocks批量插入
func (g *DataGenerator) insertStarRocksBatch(baseID int64, batchSize int) error {
	// 构建VALUES子句
	var valuesParts []string
	for i := 0; i < batchSize; i++ {
		rowData := g.generateRowData(baseID + int64(i))
		valuesStr := fmt.Sprintf("(%d, '%s', %d, %.2f, %f, %f, %t, '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d)",
			rowData[0],  // id
			rowData[1],  // name
			rowData[2],  // age
			rowData[3],  // salary
			rowData[4],  // score
			rowData[5],  // weight
			rowData[6].(int) == 1, // is_active (StarRocks支持布尔值)
			rowData[7].(time.Time).Format("2006-01-02"), // birth_date
			rowData[8].(time.Time).Format("2006-01-02 15:04:05"), // created_at
			rowData[9].(time.Time).Format("2006-01-02 15:04:05"), // updated_timestamp
			strings.ReplaceAll(fmt.Sprintf("%v", rowData[10]), "'", "\\'"), // description
			strings.ReplaceAll(fmt.Sprintf("%v", rowData[11]), "'", "\\'"), // metadata
			rowData[12], // category
			rowData[13], // small_num
			rowData[14], // medium_num
			rowData[15]) // big_num
		valuesParts = append(valuesParts, valuesStr)
	}

	insertSQL := fmt.Sprintf(`INSERT INTO %s
		(id, name, age, salary, score, weight, is_active, birth_date, created_at, updated_timestamp, description, metadata, category, small_num, medium_num, big_num)
		VALUES %s`, g.options.Table, strings.Join(valuesParts, ", "))

	if _, err := g.db.ExecContext(g.ctx, insertSQL); err != nil {
		return err
	}

	return nil
}

func (g *DataGenerator) generateRowData(id int64) []interface{} {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + id))

	return []interface{}{
		id,                         // id
		fmt.Sprintf("user_%d", id), // name
		20 + r.Intn(60),            // age (20-79)
		float64(30000+r.Intn(170000)) + r.Float64(), // salary (30000-200000)
		r.Float32() * 100,      // score (0-100)
		50.0 + r.Float64()*100, // weight (50-150)
		r.Intn(2),              // is_active (0 or 1)
		time.Date(1980+r.Intn(40), time.Month(1+r.Intn(12)), 1+r.Intn(28), 0, 0, 0, 0, time.UTC), // birth_date
		time.Now().Add(-time.Duration(r.Intn(365*5)) * 24 * time.Hour),                           // created_at (last 5 years)
		time.Now().Add(-time.Duration(r.Intn(365)) * 24 * time.Hour),                             // updated_timestamp (last year)
		fmt.Sprintf("Description for user %d with random content %d", id, r.Intn(1000)),          // description
		fmt.Sprintf(`{"user_id": %d, "level": %d, "tags": ["tag1", "tag2"]}`, id, r.Intn(10)),    // metadata
		fmt.Sprintf("CAT%d", r.Intn(10)),                                                         // category (CAT0-CAT9)
		r.Intn(256),                                                                              // small_num (0-255)
		r.Intn(65536),                                                                            // medium_num (0-65535)
		int64(r.Intn(1000000)),                                                                   // big_num (0-999999)
	}
}
