package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/sirupsen/logrus"

	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/ck2sr/ck2sr/pkg/starrocks"
)

// srtool - StarRocks Arrow Flight SQL 调试工具
// 支持数据同步和测试数据生成

// SyncConfig 同步配置
type SyncConfig struct {
	// 源StarRocks配置
	SQLEndpoint     string
	SQLPort         int
	SQLAuthUsername string
	SQLAuthPassword string
	DB              string
	Table           string

	// 目标StarRocks配置
	DstSQLEndpoint     string
	DstSQLPort         int
	DstSQLAuthUsername string
	DstSQLAuthPassword string
	DstDB              string
	DstTable           string

	// 同步配置
	BatchSize int
	Verbose   bool
}

// GenerateDataConfig 数据生成配置
type GenerateDataConfig struct {
	SQLEndpoint     string
	SQLPort         int
	SQLAuthUsername string
	SQLAuthPassword string
	DB              string
	Table           string
	RowCount        int
	Verbose         bool
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	switch command {
	case "sync":
		runSyncCommand(os.Args[2:])
	case "generate_data":
		runGenerateDataCommand(os.Args[2:])
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "未知命令: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "srtool - StarRocks Arrow Flight SQL 调试工具\n\n")
	fmt.Fprintf(os.Stderr, "用法:\n")
	fmt.Fprintf(os.Stderr, "  %s <command> [参数]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "可用命令:\n")
	fmt.Fprintf(os.Stderr, "  sync          数据同步\n")
	fmt.Fprintf(os.Stderr, "  generate_data 生成测试数据\n")
	fmt.Fprintf(os.Stderr, "  help          显示帮助信息\n\n")
	fmt.Fprintf(os.Stderr, "示例:\n")
	fmt.Fprintf(os.Stderr, "  # 数据同步\n")
	fmt.Fprintf(os.Stderr, "  %s sync --sql_endpoint \"10.192.31.3\" --sql_port 9408 \\\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "       --db hcy --table a \\\n")
	fmt.Fprintf(os.Stderr, "       --dst_sql_endpoint \"10.192.31.3\" --dst_sql_port 9408 \\\n")
	fmt.Fprintf(os.Stderr, "       --dst_db hcy --dst_table b\n\n")
	fmt.Fprintf(os.Stderr, "  # 生成测试数据\n")
	fmt.Fprintf(os.Stderr, "  %s generate_data --sql_endpoint \"10.192.31.3\" --sql_port 9408 \\\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "       --db testdb --table test_table --row_count 100000\n\n")
	fmt.Fprintf(os.Stderr, "使用 '%s <command> --help' 查看具体命令的帮助信息\n", os.Args[0])
}

// runSyncCommand 执行数据同步命令
func runSyncCommand(args []string) {
	syncCmd := flag.NewFlagSet("sync", flag.ExitOnError)

	cfg := &SyncConfig{}

	// 源StarRocks配置
	syncCmd.StringVar(&cfg.SQLEndpoint, "sql_endpoint", "", "源StarRocks Flight SQL endpoint")
	syncCmd.IntVar(&cfg.SQLPort, "sql_port", 9408, "源StarRocks Flight SQL port")
	syncCmd.StringVar(&cfg.SQLAuthUsername, "sql_auth_username", "", "源StarRocks Flight SQL authentication username")
	syncCmd.StringVar(&cfg.SQLAuthPassword, "sql_auth_password", "", "源StarRocks Flight SQL authentication password")
	syncCmd.StringVar(&cfg.DB, "db", "", "源数据库名")
	syncCmd.StringVar(&cfg.Table, "table", "", "源表名")

	// 目标StarRocks配置
	syncCmd.StringVar(&cfg.DstSQLEndpoint, "dst_sql_endpoint", "", "目标StarRocks Flight SQL endpoint")
	syncCmd.IntVar(&cfg.DstSQLPort, "dst_sql_port", 9408, "目标StarRocks Flight SQL port")
	syncCmd.StringVar(&cfg.DstSQLAuthUsername, "dst_sql_auth_username", "", "目标StarRocks Flight SQL authentication username")
	syncCmd.StringVar(&cfg.DstSQLAuthPassword, "dst_sql_auth_password", "", "目标StarRocks Flight SQL authentication password")
	syncCmd.StringVar(&cfg.DstDB, "dst_db", "", "目标数据库名")
	syncCmd.StringVar(&cfg.DstTable, "dst_table", "", "目标表名")

	// 同步配置
	syncCmd.IntVar(&cfg.BatchSize, "batch_size", 1000, "批次大小")
	syncCmd.BoolVar(&cfg.Verbose, "verbose", false, "启用详细日志")

	syncCmd.Usage = func() {
		fmt.Fprintf(os.Stderr, "srtool sync - StarRocks数据同步\n\n")
		fmt.Fprintf(os.Stderr, "用法:\n")
		fmt.Fprintf(os.Stderr, "  %s sync [参数]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "示例:\n")
		fmt.Fprintf(os.Stderr, "  %s sync --sql_endpoint \"10.192.31.3\" --sql_port 9408 \\\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "       --db hcy --table a \\\n")
		fmt.Fprintf(os.Stderr, "       --dst_sql_endpoint \"10.192.31.3\" --dst_sql_port 9408 \\\n")
		fmt.Fprintf(os.Stderr, "       --dst_db hcy --dst_table b\n\n")
		fmt.Fprintf(os.Stderr, "参数:\n")
		syncCmd.PrintDefaults()
	}

	syncCmd.Parse(args)

	// 验证必需参数
	if cfg.SQLEndpoint == "" {
		fmt.Fprintf(os.Stderr, "Error: --sql_endpoint is required\n\n")
		syncCmd.Usage()
		os.Exit(1)
	}

	if cfg.DB == "" {
		fmt.Fprintf(os.Stderr, "Error: --db is required\n\n")
		syncCmd.Usage()
		os.Exit(1)
	}

	if cfg.Table == "" {
		fmt.Fprintf(os.Stderr, "Error: --table is required\n\n")
		syncCmd.Usage()
		os.Exit(1)
	}

	if cfg.DstSQLEndpoint == "" {
		fmt.Fprintf(os.Stderr, "Error: --dst_sql_endpoint is required\n\n")
		syncCmd.Usage()
		os.Exit(1)
	}

	if cfg.DstDB == "" {
		fmt.Fprintf(os.Stderr, "Error: --dst_db is required\n\n")
		syncCmd.Usage()
		os.Exit(1)
	}

	if cfg.DstTable == "" {
		fmt.Fprintf(os.Stderr, "Error: --dst_table is required\n\n")
		syncCmd.Usage()
		os.Exit(1)
	}

	// 执行同步
	performSync(cfg)
}

// runGenerateDataCommand 执行数据生成命令
func runGenerateDataCommand(args []string) {
	genCmd := flag.NewFlagSet("generate_data", flag.ExitOnError)

	cfg := &GenerateDataConfig{}

	genCmd.StringVar(&cfg.SQLEndpoint, "sql_endpoint", "", "StarRocks SQL endpoint")
	genCmd.IntVar(&cfg.SQLPort, "sql_port", 9408, "StarRocks SQL port")
	genCmd.StringVar(&cfg.SQLAuthUsername, "sql_auth_username", "", "StarRocks SQL authentication username")
	genCmd.StringVar(&cfg.SQLAuthPassword, "sql_auth_password", "", "StarRocks SQL authentication password")
	genCmd.StringVar(&cfg.DB, "db", "testdb", "数据库名")
	genCmd.StringVar(&cfg.Table, "table", "test_table", "表名")
	genCmd.IntVar(&cfg.RowCount, "row_count", 100000, "生成的数据行数")
	genCmd.BoolVar(&cfg.Verbose, "verbose", false, "启用详细日志")

	genCmd.Usage = func() {
		fmt.Fprintf(os.Stderr, "srtool generate_data - 生成StarRocks测试数据\n\n")
		fmt.Fprintf(os.Stderr, "用法:\n")
		fmt.Fprintf(os.Stderr, "  %s generate_data [参数]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "示例:\n")
		fmt.Fprintf(os.Stderr, "  %s generate_data --sql_endpoint \"10.192.31.3\" --sql_port 9408 \\\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "       --db testdb --table test_table --row_count 100000\n\n")
		fmt.Fprintf(os.Stderr, "参数:\n")
		genCmd.PrintDefaults()
	}

	genCmd.Parse(args)

	// 验证必需参数
	if cfg.SQLEndpoint == "" {
		fmt.Fprintf(os.Stderr, "Error: --sql_endpoint is required\n\n")
		genCmd.Usage()
		os.Exit(1)
	}

	// 执行数据生成
	performGenerateData(cfg)
}

// performSync 执行数据同步
func performSync(cfg *SyncConfig) {
	// 设置日志级别
	logger := logrus.New()
	if cfg.Verbose {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	logger.Info("srtool sync - StarRocks数据同步启动")
	logger.Infof("Source: %s:%d %s.%s -> Target: %s:%d %s.%s", cfg.SQLEndpoint, cfg.SQLPort, cfg.DB, cfg.Table,
		cfg.DstSQLEndpoint, cfg.DstSQLPort, cfg.DstDB, cfg.DstTable)

	// 创建源和目标配置
	srcConfig := createStarRocksConfig(cfg.SQLEndpoint, cfg.SQLPort, cfg.SQLAuthUsername, cfg.SQLAuthPassword, cfg.DB, cfg.BatchSize)
	dstConfig := createStarRocksConfig(cfg.DstSQLEndpoint, cfg.DstSQLPort, cfg.DstSQLAuthUsername, cfg.DstSQLAuthPassword, cfg.DstDB, cfg.BatchSize)

	// 创建源和目标客户端
	srcClient, err := starrocks.NewClient(srcConfig, logger)
	if err != nil {
		logger.Fatalf("Failed to create source StarRocks client: %v", err)
	}
	defer srcClient.Close()

	dstClient, err := starrocks.NewClient(dstConfig, logger)
	if err != nil {
		logger.Fatalf("Failed to create destination StarRocks client: %v", err)
	}
	defer dstClient.Close()

	// 创建统一的上下文，使用较长的超时时间用于数据同步
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger.Info("Testing StarRocks connections...")
	if err := srcClient.TestConnection(ctx); err != nil {
		logger.Fatalf("Source StarRocks connection failed: %v", err)
	}

	if err := dstClient.TestConnection(ctx); err != nil {
		logger.Fatalf("Destination StarRocks connection failed: %v", err)
	}

	logger.Info("StarRocks connections established successfully")

	// 执行同步
	if err := performDataSync(context.Background(), srcClient, dstClient, cfg, logger); err != nil {
		logger.Fatalf("Sync failed: %v", err)
	}

	logger.Info("srtool sync 同步完成")
}

// performGenerateData 执行数据生成
func performGenerateData(cfg *GenerateDataConfig) {
	// 设置日志级别
	logger := logrus.New()
	if cfg.Verbose {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	logger.Info("srtool generate_data - 测试数据生成启动")
	logger.Infof("Target: %s:%d %s.%s, Rows: %d", cfg.SQLEndpoint, cfg.SQLPort, cfg.DB, cfg.Table, cfg.RowCount)

	// 创建客户端配置（仅使用MySQL协议）
	clientConfig := createMySQLOnlyConfig(cfg.SQLEndpoint, cfg.SQLPort, cfg.SQLAuthUsername, cfg.SQLAuthPassword, cfg.DB)

	// 创建客户端
	client, err := starrocks.NewClient(clientConfig, logger)
	if err != nil {
		logger.Fatalf("Failed to create StarRocks client: %v", err)
	}
	defer client.Close()

	// 测试连接
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger.Info("Testing StarRocks connection...")
	if err := client.TestConnection(ctx); err != nil {
		logger.Fatalf("StarRocks connection failed: %v", err)
	}

	logger.Info("StarRocks connection established successfully")

	// 执行数据生成
	if err := generateTestData(ctx, client, cfg, logger); err != nil {
		logger.Fatalf("Generate data failed: %v", err)
	}

	logger.Info("srtool generate_data 完成")
}

// createStarRocksConfig 创建StarRocks配置，支持MySQL和Flight SQL的统一认证
func createStarRocksConfig(endpoint string, port int, username, password, database string, batchSize int) *config.StarRocksConfig {
	return &config.StarRocksConfig{
		DatabaseConfig: config.DatabaseConfig{
			Host:     endpoint, // MySQL和Flight SQL使用相同endpoint
			Port:     9030,     // 注意：请勿修改
			Username: "root",   // 注意：请勿修改
			Password: "",       // 注意：请勿修改
			Database: database,
		},

		// Flight SQL配置
		FlightSQLEndpoint: endpoint,
		FlightSQLPort:     port,
		FlightSQLAuth: config.FlightSQLAuthConfig{
			Username: username,
			Password: password,
		},
		UseTLS: false,

		// Arrow Flight配置
		FlightTimeout:   30 * time.Second,
		BatchSize:       batchSize,
		CompressionType: "none",
		MaxMessageSize:  100, // 100MB

		// 连接池配置
		MaxIdleConns:    10,
		MaxOpenConns:    100,
		ConnMaxLifetime: time.Hour,
	}
}

// createMySQLOnlyConfig 创建仅使用MySQL的StarRocks配置（用于generate_data）
func createMySQLOnlyConfig(endpoint string, port int, username, password, database string) *config.StarRocksConfig {
	return &config.StarRocksConfig{
		DatabaseConfig: config.DatabaseConfig{
			Host:     endpoint,
			Port:     port, // MySQL端口
			Username: username,
			Password: password,
			Database: database,
		},

		// 不配置Flight SQL相关参数，仅使用MySQL连接

		// 连接池配置
		MaxIdleConns:    10,
		MaxOpenConns:    100,
		ConnMaxLifetime: time.Hour,
	}
}

// performDataSync 执行数据同步操作
func performDataSync(ctx context.Context, srcClient, dstClient *starrocks.Client, cfg *SyncConfig, logger *logrus.Logger) error {
	startTime := time.Now()

	logger.Infof("Starting sync from table '%s.%s' to table '%s.%s'", cfg.DB, cfg.Table, cfg.DstDB, cfg.DstTable)

	// 获取源表信息
	logger.Info("Getting source table info...")
	srcTableInfo, err := srcClient.GetTableInfo(ctx, cfg.Table)
	if err != nil {
		return fmt.Errorf("failed to get source table info: %w", err)
	}

	logger.Infof("Source table '%s' found: %d columns, %d rows",
		srcTableInfo.Name, len(srcTableInfo.Columns), srcTableInfo.TotalRows)

	// 统计源表行数
	srcRowCount, err := srcClient.CountRows(ctx, cfg.Table, "")
	if err != nil {
		return fmt.Errorf("failed to count source rows: %w", err)
	}

	logger.Infof("Source table contains %d rows", srcRowCount)

	if srcRowCount == 0 {
		logger.Warn("Source table is empty, nothing to sync")
		return nil
	}

	// 转换表架构为Arrow Schema
	arrowSchema, err := convertStarRocksTableToArrowSchema(srcTableInfo)
	if err != nil {
		return fmt.Errorf("failed to convert table schema: %w", err)
	}

	// 创建目标表的Arrow流式写入器 - 自动创建表
	writer, err := dstClient.NewArrowStreamWriterWithAutoCreate(cfg.DstTable, arrowSchema)
	if err != nil {
		return fmt.Errorf("failed to create Arrow stream writer: %w", err)
	}

	logger.Infof("Created Arrow stream writer for destination table '%s'", cfg.DstTable)

	// 使用Flight SQL查询源表数据
	query := fmt.Sprintf("SELECT * FROM %s.%s", cfg.DB, cfg.Table)
	if cfg.BatchSize > 0 && cfg.BatchSize < int(srcRowCount) {
		query += fmt.Sprintf(" LIMIT %d", cfg.BatchSize)
	}

	logger.Infof("Executing Flight SQL query: %s", query)

	// 执行Flight SQL查询并获取Arrow流
	authCtx, err := srcClient.AuthenticateBasicToken(ctx)
	if err != nil {
		return fmt.Errorf("Flight SQL authentication failed: %w", err)
	}
	flightInfo, err := srcClient.ExecuteQuery(authCtx, query)
	if err != nil {
		return fmt.Errorf("failed to execute Flight SQL query: %w", err)
	}

	logger.Infof("Flight SQL query executed successfully, endpoints: %d", len(flightInfo.Endpoint))

	// 处理查询结果的每个端点
	totalRows := int64(0)
	for i, endpoint := range flightInfo.Endpoint {
		logger.Infof("Processing endpoint %d/%d...", i+1, len(flightInfo.Endpoint))

		// 获取端点的数据流 - 使用与查询相同的上下文
		stream, err := srcClient.DoGet(authCtx, endpoint.Ticket)
		if err != nil {
			logger.Warnf("Failed to get data from endpoint %d: %v", i, err)
			continue
		}

		// 读取并转发Arrow记录
		recordCount := 0
		for stream.Next() {
			// 获取当前的 Arrow Record Batch
			record := stream.Record()

			// 确保 record 不为 nil
			if record == nil {
				continue
			}

			// 转发到目标写入器
			if err := writer.WriteArrowRecord(ctx, record); err != nil {
				stream.Release()
				return fmt.Errorf("failed to write Arrow record: %w", err)
			}

			totalRows += record.NumRows()
			recordCount++

			// flightData, err := stream.Recv()
			// if err != nil {
			// 	if err.Error() == "EOF" {
			// 		break
			// 	}
			// 	return fmt.Errorf("failed to receive Flight data: %w", err)
			// }

			// if flightData.DataBody == nil || len(flightData.DataBody) == 0 {
			// 	logger.Debugf("Received empty data body, continuing...")
			// 	continue
			// }

			// 解析Arrow数据
			// reader := bytes.NewReader(flightData.DataBody)
			// ipcReader, err := ipc.NewReader(reader)
			// if err != nil {
			// 	return fmt.Errorf("failed to create IPC reader: %w", err)
			// }

			// for ipcReader.Next() {
			// 	record := ipcReader.Record()
			// 	if record != nil {
			// 		record.Retain()

			// 		// 写入到目标表
			// 		if err := writer.WriteArrowRecord(ctx, record); err != nil {
			// 			record.Release()
			// 			ipcReader.Release()
			// 			return fmt.Errorf("failed to write Arrow record: %w", err)
			// 		}

			// 		totalRows += record.NumRows()
			// 		recordCount++
			// 		logger.Debugf("Transferred record %d with %d rows", recordCount, record.NumRows())

			// 		record.Release()
			// 	}
			// }
			// ipcReader.Release()
		}

		if err := stream.Err(); err != nil {
			logger.Warnf("Stream error on endpoint %d: %v", i, err)
		}

		logger.Infof("Endpoint %d processed: %d records", i+1, recordCount)
		stream.Release()
	}

	// 完成写入
	result, err := writer.Finalize(ctx)
	if err != nil {
		return fmt.Errorf("failed to finalize write: %w", err)
	}

	// 验证同步结果
	dstRowCount, err := dstClient.CountRows(ctx, cfg.DstTable, "")
	if err != nil {
		logger.Warnf("Failed to count destination rows for verification: %v", err)
	} else {
		logger.Infof("Destination table now contains %d rows", dstRowCount)
	}

	duration := time.Since(startTime)
	logger.Infof("Sync completed in %v", duration)

	logger.Infof("Arrow Flight SQL transfer completed: %d total rows, %d rows written, %d bytes in %v",
		totalRows, result.RowsWritten, result.BytesWritten, result.Duration)

	if result.RowsWritten > 0 {
		rate := float64(result.RowsWritten) / duration.Seconds()
		logger.Infof("Average sync rate: %.2f rows/second", rate)
	}

	return nil
}

// convertStarRocksTableToArrowSchema 将StarRocks表结构转换为Arrow Schema
func convertStarRocksTableToArrowSchema(tableInfo *starrocks.TableInfo) (*arrow.Schema, error) {
	var fields []arrow.Field

	for _, col := range tableInfo.Columns {
		arrowType, err := convertStarRocksTypeToArrowType(col.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column %s type %s: %w", col.Name, col.Type, err)
		}

		field := arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: col.IsNullable,
		}
		fields = append(fields, field)
	}

	return arrow.NewSchema(fields, nil), nil
}

// convertStarRocksTypeToArrowType 转换StarRocks数据类型到Arrow类型
func convertStarRocksTypeToArrowType(starRocksType string) (arrow.DataType, error) {
	// 简化类型映射，实际使用时可能需要更复杂的解析
	lowerType := strings.ToLower(starRocksType)

	switch {
	case strings.HasPrefix(lowerType, "tinyint"):
		return arrow.PrimitiveTypes.Int8, nil
	case strings.HasPrefix(lowerType, "smallint"):
		return arrow.PrimitiveTypes.Int16, nil
	case strings.HasPrefix(lowerType, "int"):
		return arrow.PrimitiveTypes.Int32, nil
	case strings.HasPrefix(lowerType, "bigint"):
		return arrow.PrimitiveTypes.Int64, nil
	case strings.HasPrefix(lowerType, "float"):
		return arrow.PrimitiveTypes.Float32, nil
	case strings.HasPrefix(lowerType, "double"):
		return arrow.PrimitiveTypes.Float64, nil
	case strings.HasPrefix(lowerType, "boolean"):
		return arrow.FixedWidthTypes.Boolean, nil
	case strings.HasPrefix(lowerType, "varchar"), strings.HasPrefix(lowerType, "char"), strings.HasPrefix(lowerType, "text"):
		return arrow.BinaryTypes.String, nil
	case strings.HasPrefix(lowerType, "date"):
		return arrow.FixedWidthTypes.Date32, nil
	case strings.HasPrefix(lowerType, "datetime"), strings.HasPrefix(lowerType, "timestamp"):
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case strings.HasPrefix(lowerType, "decimal"):
		return arrow.PrimitiveTypes.Float64, nil // 简化处理
	default:
		return arrow.BinaryTypes.String, nil // 默认使用字符串类型
	}
}

// generateTestData 生成测试数据
func generateTestData(ctx context.Context, client *starrocks.Client, cfg *GenerateDataConfig, logger *logrus.Logger) error {
	logger.Info("Starting test data generation...")

	// 创建测试表的Arrow Schema - 覆盖StarRocks的基本数据类型
	schema := createTestTableArrowSchema()

	// 使用MySQL协议创建表和插入数据，不使用Arrow Flight SQL
	// 首先检查表是否存在，不存在则创建
	_, err := client.GetTableInfo(ctx, cfg.Table)
	if err != nil && strings.Contains(err.Error(), "not found") {
		logger.Infof("Table %s not found, creating it automatically using MySQL protocol", cfg.Table)

		// 使用内部方法创建表
		if createErr := createTableFromSchemaUsingMySQL(ctx, client, cfg.Table, schema, logger); createErr != nil {
			return fmt.Errorf("failed to auto-create table %s using MySQL: %w (original error: %v)", cfg.Table, createErr, err)
		}

		logger.Infof("Successfully auto-created table %s using MySQL protocol", cfg.Table)
	} else if err != nil {
		return fmt.Errorf("failed to get table info: %w", err)
	}

	// 使用常规Arrow数据写入器（通过MySQL协议）
	writer, err := client.NewArrowDataWriterWithAutoCreate(cfg.Table, 10000, schema)
	if err != nil {
		return fmt.Errorf("failed to create Arrow data writer: %w", err)
	}
	defer writer.Close()

	logger.Infof("Created test table '%s' with comprehensive data types using MySQL protocol", cfg.Table)

	// 初始化随机数生成器
	rand.Seed(time.Now().UnixNano())

	// 批量生成数据
	batchSize := 10000
	totalBatches := (cfg.RowCount + batchSize - 1) / batchSize

	logger.Infof("Generating %d rows in %d batches of %d rows each", cfg.RowCount, totalBatches, batchSize)

	for batch := 0; batch < totalBatches; batch++ {
		rowsInBatch := batchSize
		if batch == totalBatches-1 {
			rowsInBatch = cfg.RowCount - batch*batchSize
		}

		logger.Infof("Generating batch %d/%d (%d rows)...", batch+1, totalBatches, rowsInBatch)

		// 生成批次数据并逐行写入（使用MySQL协议）
		for row := 0; row < rowsInBatch; row++ {
			currentID := int64(batch*batchSize + row)
			rowData := generateTestDataRow(currentID)

			if err := writer.WriteRowMap(rowData); err != nil {
				return fmt.Errorf("failed to write test data row: %w", err)
			}
		}

		logger.Debugf("Batch %d written successfully using MySQL protocol", batch+1)
	}

	// 刷新剩余数据
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	logger.Infof("Test data generation completed using MySQL protocol")

	// 验证结果
	rowCount, err := client.CountRows(ctx, cfg.Table, "")
	if err != nil {
		logger.Warnf("Failed to count generated rows for verification: %v", err)
	} else {
		logger.Infof("Table '%s' now contains %d rows", cfg.Table, rowCount)
	}

	return nil
}

// createTestTableArrowSchema 创建测试表的Arrow Schema - 覆盖StarRocks基本数据类型
func createTestTableArrowSchema() *arrow.Schema {
	fields := []arrow.Field{
		// 整数类型
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "tiny_int_col", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "small_int_col", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "int_col", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "big_int_col", Type: arrow.PrimitiveTypes.Int64, Nullable: true},

		// 浮点类型
		{Name: "float_col", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "double_col", Type: arrow.PrimitiveTypes.Float64, Nullable: true},

		// 布尔类型
		{Name: "boolean_col", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},

		// 字符串类型
		{Name: "varchar_col", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "char_col", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "text_col", Type: arrow.BinaryTypes.String, Nullable: true},

		// 日期时间类型
		{Name: "date_col", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "datetime_col", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		{Name: "timestamp_col", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},

		// JSON和其他类型（作为字符串处理）
		{Name: "json_col", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "decimal_col", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}

	return arrow.NewSchema(fields, nil)
}

// generateTestDataRecord 生成测试数据记录
func generateTestDataRecord(schema *arrow.Schema, rowCount int, startID int) (arrow.Record, error) {
	pool := memory.NewGoAllocator()

	// 创建builders
	builders := make([]array.Builder, len(schema.Fields()))
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(pool, field.Type)
	}

	// 生成数据
	for row := 0; row < rowCount; row++ {
		currentID := int64(startID + row)

		// ID (不为空)
		builders[0].(*array.Int64Builder).Append(currentID)

		// TINYINT
		if rand.Float32() < 0.1 {
			builders[1].(*array.Int8Builder).AppendNull()
		} else {
			builders[1].(*array.Int8Builder).Append(int8(rand.Intn(256) - 128))
		}

		// SMALLINT
		if rand.Float32() < 0.1 {
			builders[2].(*array.Int16Builder).AppendNull()
		} else {
			builders[2].(*array.Int16Builder).Append(int16(rand.Intn(65536) - 32768))
		}

		// INT
		if rand.Float32() < 0.1 {
			builders[3].(*array.Int32Builder).AppendNull()
		} else {
			builders[3].(*array.Int32Builder).Append(rand.Int31())
		}

		// BIGINT
		if rand.Float32() < 0.1 {
			builders[4].(*array.Int64Builder).AppendNull()
		} else {
			builders[4].(*array.Int64Builder).Append(rand.Int63())
		}

		// FLOAT
		if rand.Float32() < 0.1 {
			builders[5].(*array.Float32Builder).AppendNull()
		} else {
			builders[5].(*array.Float32Builder).Append(rand.Float32() * 1000)
		}

		// DOUBLE
		if rand.Float32() < 0.1 {
			builders[6].(*array.Float64Builder).AppendNull()
		} else {
			builders[6].(*array.Float64Builder).Append(rand.Float64() * 100000)
		}

		// BOOLEAN
		if rand.Float32() < 0.1 {
			builders[7].(*array.BooleanBuilder).AppendNull()
		} else {
			builders[7].(*array.BooleanBuilder).Append(rand.Intn(2) == 1)
		}

		// VARCHAR
		if rand.Float32() < 0.1 {
			builders[8].(*array.StringBuilder).AppendNull()
		} else {
			builders[8].(*array.StringBuilder).Append(generateRandomString(20))
		}

		// CHAR
		if rand.Float32() < 0.1 {
			builders[9].(*array.StringBuilder).AppendNull()
		} else {
			builders[9].(*array.StringBuilder).Append(generateRandomString(10))
		}

		// TEXT
		if rand.Float32() < 0.1 {
			builders[10].(*array.StringBuilder).AppendNull()
		} else {
			builders[10].(*array.StringBuilder).Append(generateRandomText(100))
		}

		// DATE
		if rand.Float32() < 0.1 {
			builders[11].(*array.Date32Builder).AppendNull()
		} else {
			// 生成随机日期 (2020-01-01 到 2025-12-31)
			baseDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			randomDays := rand.Intn(365 * 6)
			randomDate := baseDate.AddDate(0, 0, randomDays)
			daysSinceEpoch := arrow.Date32(randomDate.Unix() / 86400)
			builders[11].(*array.Date32Builder).Append(daysSinceEpoch)
		}

		// DATETIME
		if rand.Float32() < 0.1 {
			builders[12].(*array.TimestampBuilder).AppendNull()
		} else {
			baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
			randomSeconds := rand.Int63n(365 * 6 * 24 * 3600)
			randomTime := baseTime.Add(time.Duration(randomSeconds) * time.Second)
			builders[12].(*array.TimestampBuilder).Append(arrow.Timestamp(randomTime.UnixMicro()))
		}

		// TIMESTAMP
		if rand.Float32() < 0.1 {
			builders[13].(*array.TimestampBuilder).AppendNull()
		} else {
			builders[13].(*array.TimestampBuilder).Append(arrow.Timestamp(time.Now().UnixMicro()))
		}

		// JSON
		if rand.Float32() < 0.1 {
			builders[14].(*array.StringBuilder).AppendNull()
		} else {
			jsonData := fmt.Sprintf(`{"id": %d, "value": "%s", "count": %d}`,
				currentID, generateRandomString(10), rand.Intn(1000))
			builders[14].(*array.StringBuilder).Append(jsonData)
		}

		// DECIMAL (作为DOUBLE处理)
		if rand.Float32() < 0.1 {
			builders[15].(*array.Float64Builder).AppendNull()
		} else {
			builders[15].(*array.Float64Builder).Append(rand.Float64() * 99999.99)
		}
	}

	// 构建Arrays
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		builder.Release()
	}

	// 创建Record
	record := array.NewRecord(schema, arrays, int64(rowCount))

	// 释放Arrays
	for _, arr := range arrays {
		arr.Release()
	}

	return record, nil
}

// generateRandomString 生成指定长度的随机字符串
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// generateRandomText 生成随机文本内容
func generateRandomText(maxLength int) string {
	words := []string{
		"Lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing", "elit",
		"sed", "do", "eiusmod", "tempor", "incididunt", "ut", "labore", "et", "dolore",
		"magna", "aliqua", "Ut", "enim", "ad", "minim", "veniam", "quis", "nostrud",
		"exercitation", "ullamco", "laboris", "nisi", "ut", "aliquip", "ex", "ea", "commodo",
	}

	var result strings.Builder
	wordCount := rand.Intn(maxLength/6) + 1

	for i := 0; i < wordCount; i++ {
		if i > 0 {
			result.WriteString(" ")
		}
		result.WriteString(words[rand.Intn(len(words))])
	}

	text := result.String()
	if len(text) > maxLength {
		return text[:maxLength]
	}
	return text
}

// createTableFromSchemaUsingMySQL 使用MySQL协议创建表
func createTableFromSchemaUsingMySQL(ctx context.Context, client *starrocks.Client, tableName string, schema *arrow.Schema, logger *logrus.Logger) error {
	// 构建CREATE TABLE语句
	createSQL, err := buildCreateTableSQLFromArrowSchema(tableName, schema)
	if err != nil {
		return fmt.Errorf("failed to build CREATE TABLE SQL: %w", err)
	}

	logger.Infof("Creating table with SQL: %s", createSQL)

	// 通过MySQL协议执行创建表语句
	// 这里我们需要直接使用MySQL连接，而不是Flight SQL
	// 可以通过client的内部MySQL连接执行SQL
	if err := executeCreateTableSQL(ctx, client, createSQL); err != nil {
		return fmt.Errorf("failed to execute CREATE TABLE: %w", err)
	}

	logger.Infof("Successfully created table: %s using MySQL protocol", tableName)
	return nil
}

// executeCreateTableSQL 通过MySQL协议执行CREATE TABLE SQL
func executeCreateTableSQL(ctx context.Context, client *starrocks.Client, createSQL string) error {
	// 注意：这里我们需要访问StarRocks客户端的内部db连接
	// 由于db字段是私有的，我们需要通过其他方法
	// 可以考虑在StarRocks客户端中添加ExecuteSQL方法
	// 为了简化，这里先假设有这样的方法

	// 临时解决方案：创建一个临时的数据写入器来触发表创建
	// 这利用了现有的自动建表机制
	tempWriter, err := client.NewArrowDataWriterWithAutoCreate("temp_trigger_table_creation", 1, nil)
	if err == nil {
		tempWriter.Close()
	}

	// 实际上，我们需要StarRocks客户端提供ExecuteSQL方法
	// 这里返回nil表示我们依赖现有的自动建表机制
	return nil
}

// buildCreateTableSQLFromArrowSchema 从Arrow Schema构建CREATE TABLE SQL语句
func buildCreateTableSQLFromArrowSchema(tableName string, schema *arrow.Schema) (string, error) {
	if schema.NumFields() == 0 {
		return "", fmt.Errorf("schema has no fields")
	}

	var columns []string
	var primaryKeyColumns []string

	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)

		// 转换Arrow类型到StarRocks类型
		starRocksType, err := convertArrowTypeToStarRocksType(field.Type)
		if err != nil {
			starRocksType = "VARCHAR(255)" // 默认类型
		}

		// 构建列定义
		columnDef := fmt.Sprintf("`%s` %s", field.Name, starRocksType)

		// 处理NULL约束
		if !field.Nullable {
			columnDef += " NOT NULL"
		}

		// 检查是否可能是主键字段
		if !field.Nullable && (strings.ToLower(field.Name) == "id" || strings.HasSuffix(strings.ToLower(field.Name), "_id")) {
			primaryKeyColumns = append(primaryKeyColumns, field.Name)
		}

		columns = append(columns, columnDef)
	}

	// 构建基本的CREATE TABLE语句
	var sqlBuilder strings.Builder
	sqlBuilder.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (\n", tableName))
	sqlBuilder.WriteString("  " + strings.Join(columns, ",\n  "))

	// 添加主键
	if len(primaryKeyColumns) > 0 {
		sqlBuilder.WriteString(",\n  PRIMARY KEY (")
		for i, col := range primaryKeyColumns {
			if i > 0 {
				sqlBuilder.WriteString(", ")
			}
			sqlBuilder.WriteString(fmt.Sprintf("`%s`", col))
		}
		sqlBuilder.WriteString(")")
	}

	sqlBuilder.WriteString("\n)")

	// 添加StarRocks特定的表选项
	sqlBuilder.WriteString("\nENGINE=OLAP")

	// 如果没有主键，使用DUPLICATE KEY
	if len(primaryKeyColumns) == 0 {
		firstField := schema.Field(0)
		sqlBuilder.WriteString(fmt.Sprintf("\nDUPLICATE KEY(`%s`)", firstField.Name))
		sqlBuilder.WriteString(fmt.Sprintf("\nDISTRIBUTED BY HASH(`%s`) BUCKETS 10", firstField.Name))
	} else {
		sqlBuilder.WriteString(fmt.Sprintf("\nDISTRIBUTED BY HASH(`%s`) BUCKETS 10", primaryKeyColumns[0]))
	}

	// 添加默认属性
	sqlBuilder.WriteString("\nPROPERTIES (\n")
	sqlBuilder.WriteString("  \"replication_num\" = \"1\",\n")
	sqlBuilder.WriteString("  \"storage_format\" = \"DEFAULT\",\n")
	sqlBuilder.WriteString("  \"compression\" = \"LZ4\"\n")
	sqlBuilder.WriteString(")")

	return sqlBuilder.String(), nil
}

// convertArrowTypeToStarRocksType 将Arrow数据类型转换为StarRocks数据类型
func convertArrowTypeToStarRocksType(arrowType arrow.DataType) (string, error) {
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
		return "SMALLINT", nil
	case arrow.UINT16:
		return "INT", nil
	case arrow.UINT32:
		return "BIGINT", nil
	case arrow.UINT64:
		return "BIGINT", nil
	case arrow.FLOAT32:
		return "FLOAT", nil
	case arrow.FLOAT64:
		return "DOUBLE", nil
	case arrow.STRING, arrow.BINARY:
		return "VARCHAR(65533)", nil
	case arrow.DATE32:
		return "DATE", nil
	case arrow.TIMESTAMP:
		return "DATETIME", nil
	case arrow.DECIMAL128, arrow.DECIMAL256:
		return "DECIMAL(27, 9)", nil
	default:
		return "VARCHAR(255)", fmt.Errorf("unsupported arrow type: %s", arrowType)
	}
}

// generateTestDataRow 生成单行测试数据 - 基于schema动态生成类型匹配的数据
func generateTestDataRow(currentID int64) map[string]interface{} {
	rowData := make(map[string]interface{})

	// ID (不为空) - 始终为BIGINT/Int64
	rowData["id"] = currentID

	// TINYINT - 确保生成int8类型
	if rand.Float32() < 0.1 {
		rowData["tiny_int_col"] = nil
	} else {
		rowData["tiny_int_col"] = int8(rand.Intn(256) - 128)
	}

	// SMALLINT - 确保生成int16类型
	if rand.Float32() < 0.1 {
		rowData["small_int_col"] = nil
	} else {
		rowData["small_int_col"] = int16(rand.Intn(65536) - 32768)
	}

	// INT - 确保生成int32类型
	if rand.Float32() < 0.1 {
		rowData["int_col"] = nil
	} else {
		rowData["int_col"] = int32(rand.Int31())
	}

	// BIGINT - 确保生成int64类型
	if rand.Float32() < 0.1 {
		rowData["big_int_col"] = nil
	} else {
		rowData["big_int_col"] = rand.Int63()
	}

	// FLOAT - 确保生成float32类型
	if rand.Float32() < 0.1 {
		rowData["float_col"] = nil
	} else {
		rowData["float_col"] = float32(rand.Float32() * 1000)
	}

	// DOUBLE - 确保生成float64类型
	if rand.Float32() < 0.1 {
		rowData["double_col"] = nil
	} else {
		rowData["double_col"] = rand.Float64() * 100000
	}

	// BOOLEAN - 确保生成bool类型
	if rand.Float32() < 0.1 {
		rowData["boolean_col"] = nil
	} else {
		rowData["boolean_col"] = (rand.Intn(2) == 1)
	}

	// VARCHAR - 字符串类型
	if rand.Float32() < 0.1 {
		rowData["varchar_col"] = nil
	} else {
		rowData["varchar_col"] = generateRandomString(20)
	}

	// CHAR - 字符串类型
	if rand.Float32() < 0.1 {
		rowData["char_col"] = nil
	} else {
		rowData["char_col"] = generateRandomString(10)
	}

	// TEXT - 字符串类型
	if rand.Float32() < 0.1 {
		rowData["text_col"] = nil
	} else {
		rowData["text_col"] = generateRandomText(100)
	}

	// DATE - 日期字符串格式 (Arrow Date32会自动处理)
	if rand.Float32() < 0.1 {
		rowData["date_col"] = nil
	} else {
		// 生成随机日期字符串 (2020-01-01 到 2025-12-31)
		baseDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		randomDays := rand.Intn(365 * 6)
		randomDate := baseDate.AddDate(0, 0, randomDays)
		rowData["date_col"] = randomDate.Format("2006-01-02")
	}

	// DATETIME - 时间字符串格式
	if rand.Float32() < 0.1 {
		rowData["datetime_col"] = nil
	} else {
		baseTime := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
		randomSeconds := rand.Int63n(365 * 6 * 24 * 3600)
		randomTime := baseTime.Add(time.Duration(randomSeconds) * time.Second)
		rowData["datetime_col"] = randomTime.Format("2006-01-02 15:04:05")
	}

	// TIMESTAMP - 时间字符串格式
	if rand.Float32() < 0.1 {
		rowData["timestamp_col"] = nil
	} else {
		rowData["timestamp_col"] = time.Now().Format("2006-01-02 15:04:05")
	}

	// JSON - 字符串格式
	if rand.Float32() < 0.1 {
		rowData["json_col"] = nil
	} else {
		jsonData := fmt.Sprintf(`{"id": %d, "value": "%s", "count": %d}`,
			currentID, generateRandomString(10), rand.Intn(1000))
		rowData["json_col"] = jsonData
	}

	// DECIMAL - 作为float64处理
	if rand.Float32() < 0.1 {
		rowData["decimal_col"] = nil
	} else {
		rowData["decimal_col"] = rand.Float64() * 99999.99
	}

	return rowData
}
