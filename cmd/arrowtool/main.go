package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

// Config arrowtool配置
type Config struct {
	// 源StarRocks配置
	SrcSQLEndpoint     string
	SrcSQLPort         int
	SrcSQLAuthUsername string
	SrcSQLAuthPassword string
	SrcDB              string
	SrcTable           string

	// 目标StarRocks配置
	DstSQLEndpoint     string
	DstSQLPort         int
	DstSQLAuthUsername string
	DstSQLAuthPassword string
	DstDB              string
	DstTable           string

	// 目标StarRocks MySQL端口（用于写入）
	DstMySQLPort       int

	// 调试配置
	Verbose bool
}

var testCaseNum = 1

func main() {
	// 解析命令行参数
	cfg := parseFlags()

	// 设置日志级别
	logger := logrus.New()
	if cfg.Verbose {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	logger.Info("arrowtool - StarRocks Arrow ADBC数据同步工具启动")
	logger.Infof("Source: %s:%d %s.%s -> Target: %s:%d %s.%s",
		cfg.SrcSQLEndpoint, cfg.SrcSQLPort, cfg.SrcDB, cfg.SrcTable,
		cfg.DstSQLEndpoint, cfg.DstSQLPort, cfg.DstDB, cfg.DstTable)

	// 创建上下文
	ctx := context.Background()

	// 执行数据同步
	if err := performDataSync(ctx, cfg, logger); err != nil {
		logger.Fatalf("数据同步失败: %v", err)
	}

	logger.Info("arrowtool 数据同步完成")
}

// parseFlags 解析命令行参数
func parseFlags() *Config {
	cfg := &Config{}

	// 源StarRocks配置
	flag.StringVar(&cfg.SrcSQLEndpoint, "src_sql_endpoint", "", "源StarRocks Flight SQL endpoint")
	flag.IntVar(&cfg.SrcSQLPort, "src_sql_port", 9408, "源StarRocks Flight SQL port")
	flag.StringVar(&cfg.SrcSQLAuthUsername, "src_sql_auth_username", "", "源StarRocks Flight SQL username")
	flag.StringVar(&cfg.SrcSQLAuthPassword, "src_sql_auth_password", "", "源StarRocks Flight SQL password")
	flag.StringVar(&cfg.SrcDB, "src_db", "", "源数据库名")
	flag.StringVar(&cfg.SrcTable, "src_table", "", "源表名")

	// 目标StarRocks配置
	flag.StringVar(&cfg.DstSQLEndpoint, "dst_sql_endpoint", "", "目标StarRocks Flight SQL endpoint")
	flag.IntVar(&cfg.DstSQLPort, "dst_sql_port", 9408, "目标StarRocks Flight SQL port")
	flag.StringVar(&cfg.DstSQLAuthUsername, "dst_sql_auth_username", "", "目标StarRocks Flight SQL username")
	flag.StringVar(&cfg.DstSQLAuthPassword, "dst_sql_auth_password", "", "目标StarRocks Flight SQL password")
	flag.StringVar(&cfg.DstDB, "dst_db", "", "目标数据库名")
	flag.StringVar(&cfg.DstTable, "dst_table", "", "目标表名")
	flag.IntVar(&cfg.DstMySQLPort, "dst_mysql_port", 9030, "目标StarRocks MySQL port (用于数据写入)")

	// 调试配置
	flag.BoolVar(&cfg.Verbose, "verbose", false, "启用详细日志")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "arrowtool - StarRocks Arrow ADBC数据同步工具\n\n")
		fmt.Fprintf(os.Stderr, "用法:\n")
		fmt.Fprintf(os.Stderr, "  %s [参数]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "示例:\n")
		fmt.Fprintf(os.Stderr, "  %s --src_sql_endpoint \"10.192.31.3\" --src_sql_port 9408 \\\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "       --src_sql_auth_username \"root\" --src_sql_auth_password \"StarRocks!@2025#.\" \\\n")
		fmt.Fprintf(os.Stderr, "       --src_db hcy --src_table a \\\n")
		fmt.Fprintf(os.Stderr, "       --dst_sql_endpoint \"10.192.31.3\" --dst_sql_port 9408 \\\n")
		fmt.Fprintf(os.Stderr, "       --dst_sql_auth_username \"root\" --dst_sql_auth_password \"StarRocks!@2025#.\" \\\n")
		fmt.Fprintf(os.Stderr, "       --dst_db hcy --dst_table b\n\n")
		fmt.Fprintf(os.Stderr, "参数:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	// 验证必需参数
	if cfg.SrcSQLEndpoint == "" {
		fmt.Fprintf(os.Stderr, "Error: --src_sql_endpoint is required\n\n")
		flag.Usage()
		os.Exit(1)
	}
	if cfg.SrcDB == "" {
		fmt.Fprintf(os.Stderr, "Error: --src_db is required\n\n")
		flag.Usage()
		os.Exit(1)
	}
	if cfg.SrcTable == "" {
		fmt.Fprintf(os.Stderr, "Error: --src_table is required\n\n")
		flag.Usage()
		os.Exit(1)
	}
	if cfg.DstSQLEndpoint == "" {
		fmt.Fprintf(os.Stderr, "Error: --dst_sql_endpoint is required\n\n")
		flag.Usage()
		os.Exit(1)
	}
	if cfg.DstDB == "" {
		fmt.Fprintf(os.Stderr, "Error: --dst_db is required\n\n")
		flag.Usage()
		os.Exit(1)
	}
	if cfg.DstTable == "" {
		fmt.Fprintf(os.Stderr, "Error: --dst_table is required\n\n")
		flag.Usage()
		os.Exit(1)
	}

	return cfg
}

// performDataSync 执行零拷贝数据同步
func performDataSync(ctx context.Context, cfg *Config, logger *logrus.Logger) error {
	startTime := time.Now()

	// 创建源数据库连接
	srcConn, srcDB, err := createADBCConnection(cfg.SrcSQLEndpoint, cfg.SrcSQLPort, cfg.SrcSQLAuthUsername, cfg.SrcSQLAuthPassword, logger)
	if err != nil {
		return fmt.Errorf("failed to create source ADBC connection: %w", err)
	}
	defer srcConn.Close()
	defer srcDB.Close()

	// 创建目标MySQL数据库连接（用于写入）
	dstMySQLConn, err := createMySQLConnection(cfg.DstSQLEndpoint, cfg.DstMySQLPort, cfg.DstSQLAuthUsername, cfg.DstSQLAuthPassword, cfg.DstDB, logger)
	if err != nil {
		return fmt.Errorf("failed to create destination MySQL connection: %w", err)
	}
	defer dstMySQLConn.Close()

	// 测试源连接
	logger.Info("Testing source StarRocks connection...")
	if err := testConnection(srcConn, logger); err != nil {
		return fmt.Errorf("source StarRocks connection test failed: %w", err)
	}

	// 测试目标MySQL连接
	logger.Info("Testing destination StarRocks MySQL connection...")
	if err := testMySQLConnection(dstMySQLConn, logger); err != nil {
		return fmt.Errorf("destination StarRocks MySQL connection test failed: %w", err)
	}

	logger.Info("StarRocks connections established successfully")

	// 执行查询并获取Arrow数据 - 零拷贝
	query := fmt.Sprintf("SELECT * FROM %s.%s", cfg.SrcDB, cfg.SrcTable)
	logger.Infof("Executing source query: %s", query)

	// 创建查询Statement
	srcStmt, err := srcConn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to create source statement: %w", err)
	}
	defer srcStmt.Close()

	// 设置查询语句
	if err := srcStmt.SetSqlQuery(query); err != nil {
		return fmt.Errorf("failed to set source query: %w", err)
	}

	// 执行查询并获取Arrow Record Reader - 直接零拷贝传输
	reader, affected, err := srcStmt.ExecuteQuery(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute source query: %w", err)
	}
	defer reader.Release()

	logger.Infof("Query executed successfully, affected rows: %d", affected)

	// 使用混合连接方案进行高性能数据同步
	logger.Info("Starting hybrid data sync: Flight SQL (read) + MySQL (write)")

	// 获取目标表的列结构
	columns, err := getTableColumns(dstMySQLConn, cfg.DstDB, cfg.DstTable, logger)
	if err != nil {
		return fmt.Errorf("failed to get target table columns: %w", err)
	}

	totalRows := int64(0)
	totalBatches := 0

	// 逐批处理Arrow RecordBatch，使用MySQL批量插入
	for reader.Next() {
		batch := reader.RecordBatch()
		if batch == nil {
			continue
		}

		totalBatches++
		batchRows := batch.NumRows()
		logger.Debugf("Processing batch %d with %d rows", totalBatches, batchRows)

		// 使用MySQL批量插入
		batchAffected, err := insertBatchMySQL(dstMySQLConn, cfg.DstDB, cfg.DstTable, batch, columns, logger)
		if err != nil {
			return fmt.Errorf("failed to insert batch %d: %w", totalBatches, err)
		}

		totalRows += int64(batchAffected)
		logger.Debugf("Batch %d completed: %d rows inserted", totalBatches, batchAffected)
	}

	// 检查读取器错误
	if err := reader.Err(); err != nil {
		return fmt.Errorf("error reading Arrow records: %w", err)
	}

	duration := time.Since(startTime)
	logger.Infof("Data sync completed: %d batches, %d rows transferred in %v", totalBatches, totalRows, duration)

	if totalRows > 0 {
		rate := float64(totalRows) / duration.Seconds()
		logger.Infof("Average sync rate: %.2f rows/second", rate)
	}

	return nil
}

// createADBCConnection 创建Arrow ADBC Flight SQL连接
func createADBCConnection(endpoint string, port int, username, password string, logger *logrus.Logger) (adbc.Connection, adbc.Database, error) {
	// 构建Arrow Flight SQL连接字符串 - 参考Java示例
	uri := fmt.Sprintf("grpc+tcp://%s:%d", endpoint, port)

	// 创建FlightSQL驱动 - 使用默认内存分配器
	driver := flightsql.NewDriver(memory.DefaultAllocator)

	// 创建ADBC数据库连接选项
	options := make(map[string]string)
	options[adbc.OptionKeyURI] = uri
	if username != "" {
		options[adbc.OptionKeyUsername] = username
	}
	if password != "" {
		options[adbc.OptionKeyPassword] = password
	}

	// 添加FlightSQL特定选项 - 参考Java连接参数
	options["adbc.flight.sql.client_option.with_block"] = "false"
	options["adbc.flight.sql.client_option.with_max_msg_size"] = "33554432" // 32MB

	logger.Debugf("Creating ADBC connection with URI: %s", uri)

	// 创建ADBC数据库实例
	adbcDB, err := driver.NewDatabase(options)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create ADBC database: %w", err)
	}

	// 创建ADBC连接
	conn, err := adbcDB.Open(context.Background())
	if err != nil {
		adbcDB.Close()
		return nil, nil, fmt.Errorf("failed to open ADBC connection: %w", err)
	}

	return conn, adbcDB, nil
}

// testConnection 测试数据库连接 - 参考Java示例的测试方法
func testConnection(conn adbc.Connection, logger *logrus.Logger) error {
	ctx := context.Background()

	// 创建测试Statement
	stmt, err := conn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to create test statement: %w", err)
	}
	defer stmt.Close()

	// 执行简单查询测试连接
	testQuery := "SELECT 1 as test_column"
	logger.Debugf("Test Case: %d", testCaseNum)
	logger.Debugf("▶ Executing query: %s", testQuery)

	// 设置查询语句
	if err := stmt.SetSqlQuery(testQuery); err != nil {
		return fmt.Errorf("failed to set test query: %w", err)
	}

	// 执行查询
	reader, affected, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return fmt.Errorf("test query failed: %w", err)
	}
	defer reader.Release()

	logger.Debugf("Result: ✅ Success (affected: %d)", affected)

	// 读取结果验证
	hasResults := false
	for reader.Next() {
		record := reader.RecordBatch()
		if record != nil && record.NumRows() > 0 {
			hasResults = true
			logger.Debugf("Test result: %d rows returned", record.NumRows())
			break
		}
	}

	if err := reader.Err(); err != nil {
		return fmt.Errorf("error reading test query results: %w", err)
	}

	if !hasResults {
		return fmt.Errorf("test query returned no results")
	}

	testCaseNum++
	logger.Debug("")

	return nil
}

// executeQuery 执行查询语句 - 参考Java示例，用于调试
func executeQuery(conn adbc.Connection, sql string, logger *logrus.Logger) error {
	logger.Infof("Test Case: %d", testCaseNum)
	logger.Infof("▶ Executing query: %s", sql)

	ctx := context.Background()

	// 创建Statement
	stmt, err := conn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to create statement: %w", err)
	}
	defer stmt.Close()

	// 设置查询语句
	if err := stmt.SetSqlQuery(sql); err != nil {
		return fmt.Errorf("failed to set query: %w", err)
	}

	// 执行查询
	reader, affected, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	defer reader.Release()

	logger.Infof("Result: (affected: %d)", affected)

	// 打印查询结果
	resultCount := 0
	for reader.Next() {
		record := reader.RecordBatch()
		if record == nil {
			continue
		}

		// 打印列数据
		for row := int64(0); row < record.NumRows(); row++ {
			var values []string
			for col := 0; col < int(record.NumCols()); col++ {
				column := record.Column(col)
				if column.IsNull(int(row)) {
					values = append(values, "NULL")
				} else {
					// 根据列类型获取值
					value := getColumnValue(column, int(row))
					values = append(values, fmt.Sprintf("%v", value))
				}
			}
			logger.Infof("Row %d: %v", resultCount+1, values)
			resultCount++
		}
	}

	if err := reader.Err(); err != nil {
		return fmt.Errorf("error reading query results: %w", err)
	}

	testCaseNum++
	logger.Info("")

	return nil
}

// getColumnValue 获取列值 - 处理不同的Arrow数据类型
func getColumnValue(column arrow.Array, row int) interface{} {
	switch arr := column.(type) {
	case *array.Int8:
		return arr.Value(row)
	case *array.Int16:
		return arr.Value(row)
	case *array.Int32:
		return arr.Value(row)
	case *array.Int64:
		return arr.Value(row)
	case *array.Uint8:
		return arr.Value(row)
	case *array.Uint16:
		return arr.Value(row)
	case *array.Uint32:
		return arr.Value(row)
	case *array.Uint64:
		return arr.Value(row)
	case *array.Float32:
		return arr.Value(row)
	case *array.Float64:
		return arr.Value(row)
	case *array.Boolean:
		return arr.Value(row)
	case *array.String:
		return arr.Value(row)
	case *array.Binary:
		return string(arr.Value(row))
	case *array.Date32:
		return arr.Value(row)
	case *array.Date64:
		return arr.Value(row)
	case *array.Timestamp:
		return arr.Value(row)
	default:
		// 对于其他类型，尝试通过通用接口获取
		return column.GetOneForMarshal(row)
	}
}

// createMySQLConnection 创建MySQL连接用于写入数据
func createMySQLConnection(endpoint string, port int, username, password, database string, logger *logrus.Logger) (*sql.DB, error) {
	// 构建MySQL连接字符串
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true",
		username, password, endpoint, port, database)

	logger.Debugf("Creating MySQL connection to: %s:%d/%s", endpoint, port, database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	// 设置连接池参数
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	return db, nil
}

// testMySQLConnection 测试MySQL连接
func testMySQLConnection(db *sql.DB, logger *logrus.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	testQuery := "SELECT 1 as test_column"
	logger.Debugf("Test Case: %d", testCaseNum)
	logger.Debugf("▶ Executing MySQL query: %s", testQuery)

	rows, err := db.QueryContext(ctx, testQuery)
	if err != nil {
		return fmt.Errorf("MySQL test query failed: %w", err)
	}
	defer rows.Close()

	hasResults := false
	for rows.Next() {
		var testValue int
		if err := rows.Scan(&testValue); err != nil {
			return fmt.Errorf("failed to scan test result: %w", err)
		}
		hasResults = true
		logger.Debugf("MySQL test result: %d", testValue)
		break
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error reading MySQL test results: %w", err)
	}

	if !hasResults {
		return fmt.Errorf("MySQL test query returned no results")
	}

	testCaseNum++
	logger.Debug("")

	return nil
}

// ColumnInfo 表示表列信息
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

// getTableColumns 获取目标表的列结构
func getTableColumns(db *sql.DB, database, table string, logger *logrus.Logger) ([]ColumnInfo, error) {
	query := `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
			  FROM INFORMATION_SCHEMA.COLUMNS
			  WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
			  ORDER BY ORDINAL_POSITION`

	logger.Debugf("Getting table structure for %s.%s", database, table)

	rows, err := db.Query(query, database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query table columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var nullable string
		if err := rows.Scan(&col.Name, &col.Type, &nullable); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		col.Nullable = nullable == "YES"
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading column info: %w", err)
	}

	logger.Debugf("Found %d columns in target table", len(columns))
	return columns, nil
}

// insertBatchMySQL 使用MySQL连接批量插入Arrow数据
func insertBatchMySQL(db *sql.DB, database, table string, batch arrow.RecordBatch, columns []ColumnInfo, logger *logrus.Logger) (int, error) {
	if batch.NumRows() == 0 {
		return 0, nil
	}

	// 构建INSERT语句头部
	insertSQL := fmt.Sprintf("INSERT INTO %s.%s VALUES ", database, table)

	// 构建VALUES子句
	var valuesClauses []string
	for row := int64(0); row < batch.NumRows(); row++ {
		var values []string
		numCols := int(batch.NumCols())
		for col := 0; col < numCols && col < len(columns); col++ {
			column := batch.Column(col)
			if column.IsNull(int(row)) {
				values = append(values, "NULL")
			} else {
				value := getColumnValue(column, int(row))
				// 对字符串值进行适当的转义和引用
				formattedValue := formatValueForSQL(value, columns[col].Type)
				values = append(values, formattedValue)
			}
		}
		valuesClauses = append(valuesClauses, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	}

	// 组合完整的INSERT语句
	finalSQL := insertSQL + strings.Join(valuesClauses, ", ")

	// 直接执行SQL语句（不使用prepared statement）
	result, err := db.Exec(finalSQL)
	if err != nil {
		logger.Debugf("Failed SQL: %s", finalSQL)
		return 0, fmt.Errorf("failed to execute insert statement: %w", err)
	}

	// 获取插入的行数
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return int(rowsAffected), nil
}

// formatValueForSQL 格式化值以适应SQL语句
func formatValueForSQL(value interface{}, columnType string) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case string:
		// 转义字符串中的特殊字符
		escaped := strings.ReplaceAll(v, "'", "\\'")
		escaped = strings.ReplaceAll(escaped, "\\", "\\\\")
		return fmt.Sprintf("'%s'", escaped)
	case []byte:
		// 处理二进制数据
		escaped := strings.ReplaceAll(string(v), "'", "\\'")
		escaped = strings.ReplaceAll(escaped, "\\", "\\\\")
		return fmt.Sprintf("'%s'", escaped)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", v)
	case float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	case time.Time:
		return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05"))
	default:
		// 对于其他类型，转换为字符串并进行转义
		str := fmt.Sprintf("%v", v)
		escaped := strings.ReplaceAll(str, "'", "\\'")
		escaped = strings.ReplaceAll(escaped, "\\", "\\\\")
		return fmt.Sprintf("'%s'", escaped)
	}
}
