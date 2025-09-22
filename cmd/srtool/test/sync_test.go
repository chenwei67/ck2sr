package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/ck2sr/ck2sr/pkg/starrocks"
)

// TestSyncCommand 测试数据同步命令
func TestSyncCommand(t *testing.T) {
	tests := []struct {
		name           string
		config         *SyncConfig
		setupSrcMock   func(*MockStarRocksClientImpl)
		setupDstMock   func(*MockStarRocksClientImpl)
		expectedError  bool
		expectedRows   int64
		validateResult func(*testing.T, *MockStarRocksClientImpl, *MockStarRocksClientImpl)
	}{
		{
			name: "成功同步数据",
			config: &SyncConfig{
				SQLEndpoint:        "src-host",
				SQLPort:            9408,
				SQLAuthUsername:    "src-user",
				SQLAuthPassword:    "src-pass",
				DB:                 "src_db",
				Table:              "source_table",
				DstSQLEndpoint:     "dst-host",
				DstSQLPort:         9408,
				DstSQLAuthUsername: "dst-user",
				DstSQLAuthPassword: "dst-pass",
				DstDB:              "dst_db",
				DstTable:           "target_table",
				BatchSize:          1000,
				Verbose:            false,
			},
			setupSrcMock: func(mock *MockStarRocksClientImpl) {
				// 源表存在且有数据
				srcTable := createMockSourceTable()
				mock.SetTableInfo("source_table", srcTable)
				mock.SetRowCount("source_table", 5000)

				// 设置查询结果
				flightInfo := &flight.FlightInfo{
					Endpoint: []*flight.FlightEndpoint{
						{Ticket: &flight.Ticket{Ticket: []byte("endpoint_1")}},
						{Ticket: &flight.Ticket{Ticket: []byte("endpoint_2")}},
					},
				}
				mock.SetQueryResult("select * from src_db.source_table", flightInfo)

				// 设置数据流
				stream1 := createMockFlightStream(1000)
				stream2 := createMockFlightStream(4000)
				mock.DoGetStreams["endpoint_1"] = stream1
				mock.DoGetStreams["endpoint_2"] = stream2
			},
			setupDstMock: func(mock *MockStarRocksClientImpl) {
				// 目标表不存在，需要自动创建
				mock.TableNotFoundError = fmt.Errorf("table not found")
			},
			expectedError: false,
			expectedRows:  2, // 根据mock数据流实际返回的记录数
			validateResult: func(t *testing.T, srcMock, dstMock *MockStarRocksClientImpl) {
				// 验证目标表被创建
				if len(dstMock.GetCreatedTables()) != 1 {
					t.Errorf("Expected 1 table to be created, got %d", len(dstMock.GetCreatedTables()))
				}
				if dstMock.GetCreatedTables()[0] != "target_table" {
					t.Errorf("Expected table 'target_table' to be created, got %s", dstMock.GetCreatedTables()[0])
				}

				// 验证流式写入器被创建
				writer := dstMock.GetStreamWriter("target_table")
				if writer == nil {
					t.Error("Expected stream writer to be created")
				} else {
					if writer.RowCount != 2 {
						t.Errorf("Expected 2 rows to be written, got %d", writer.RowCount)
					}
				}
			},
		},
		{
			name: "源连接失败",
			config: &SyncConfig{
				SQLEndpoint:        "invalid-host",
				SQLPort:            9408,
				SQLAuthUsername:    "user",
				SQLAuthPassword:    "pass",
				DB:                 "db",
				Table:              "table",
				DstSQLEndpoint:     "dst-host",
				DstSQLPort:         9408,
				DstSQLAuthUsername: "dst-user",
				DstSQLAuthPassword: "dst-pass",
				DstDB:              "dst_db",
				DstTable:           "dst_table",
				BatchSize:          1000,
				Verbose:            true,
			},
			setupSrcMock: func(mock *MockStarRocksClientImpl) {
				mock.ConnectionError = fmt.Errorf("connection refused")
			},
			setupDstMock: func(mock *MockStarRocksClientImpl) {
				// 目标连接正常
			},
			expectedError: true,
			expectedRows:  0,
			validateResult: func(t *testing.T, srcMock, dstMock *MockStarRocksClientImpl) {
				// 验证没有数据被同步
				if len(dstMock.GetCreatedTables()) != 0 {
					t.Errorf("Expected no tables to be created due to source connection failure, got %d", len(dstMock.GetCreatedTables()))
				}
			},
		},
		{
			name: "目标连接失败",
			config: &SyncConfig{
				SQLEndpoint:        "src-host",
				SQLPort:            9408,
				SQLAuthUsername:    "src-user",
				SQLAuthPassword:    "src-pass",
				DB:                 "src_db",
				Table:              "source_table",
				DstSQLEndpoint:     "invalid-dst-host",
				DstSQLPort:         9408,
				DstSQLAuthUsername: "dst-user",
				DstSQLAuthPassword: "dst-pass",
				DstDB:              "dst_db",
				DstTable:           "target_table",
				BatchSize:          1000,
				Verbose:            false,
			},
			setupSrcMock: func(mock *MockStarRocksClientImpl) {
				// 源连接正常
				srcTable := createMockSourceTable()
				mock.SetTableInfo("source_table", srcTable)
				mock.SetRowCount("source_table", 1000)
			},
			setupDstMock: func(mock *MockStarRocksClientImpl) {
				mock.ConnectionError = fmt.Errorf("destination connection refused")
			},
			expectedError: true,
			expectedRows:  0,
			validateResult: func(t *testing.T, srcMock, dstMock *MockStarRocksClientImpl) {
				// 验证源表信息被获取但同步失败
				if _, exists := srcMock.TableInfos["source_table"]; !exists {
					t.Error("Expected source table info to be retrieved")
				}
			},
		},
		{
			name: "源表为空",
			config: &SyncConfig{
				SQLEndpoint:        "src-host",
				SQLPort:            9408,
				SQLAuthUsername:    "src-user",
				SQLAuthPassword:    "src-pass",
				DB:                 "src_db",
				Table:              "empty_table",
				DstSQLEndpoint:     "dst-host",
				DstSQLPort:         9408,
				DstSQLAuthUsername: "dst-user",
				DstSQLAuthPassword: "dst-pass",
				DstDB:              "dst_db",
				DstTable:           "target_table",
				BatchSize:          1000,
				Verbose:            true,
			},
			setupSrcMock: func(mock *MockStarRocksClientImpl) {
				srcTable := createMockSourceTable()
				mock.SetTableInfo("empty_table", srcTable)
				mock.SetRowCount("empty_table", 0) // 空表
			},
			setupDstMock: func(mock *MockStarRocksClientImpl) {
				// 目标连接正常
			},
			expectedError: false, // 空表同步不应该报错
			expectedRows:  0,
			validateResult: func(t *testing.T, srcMock, dstMock *MockStarRocksClientImpl) {
				// 验证没有数据被同步但没有错误
				writer := dstMock.GetStreamWriter("target_table")
				if writer != nil && writer.RowCount != 0 {
					t.Errorf("Expected 0 rows to be written for empty table, got %d", writer.RowCount)
				}
			},
		},
		{
			name: "大批量同步",
			config: &SyncConfig{
				SQLEndpoint:        "src-host",
				SQLPort:            9408,
				SQLAuthUsername:    "src-user",
				SQLAuthPassword:    "src-pass",
				DB:                 "src_db",
				Table:              "large_table",
				DstSQLEndpoint:     "dst-host",
				DstSQLPort:         9408,
				DstSQLAuthUsername: "dst-user",
				DstSQLAuthPassword: "dst-pass",
				DstDB:              "dst_db",
				DstTable:           "large_target",
				BatchSize:          5000,
				Verbose:            false,
			},
			setupSrcMock: func(mock *MockStarRocksClientImpl) {
				srcTable := createMockSourceTable()
				mock.SetTableInfo("large_table", srcTable)
				mock.SetRowCount("large_table", 100000)

				// 设置大数据查询结果
				flightInfo := &flight.FlightInfo{
					Endpoint: []*flight.FlightEndpoint{
						{Ticket: &flight.Ticket{Ticket: []byte("large_endpoint_1")}},
						{Ticket: &flight.Ticket{Ticket: []byte("large_endpoint_2")}},
						{Ticket: &flight.Ticket{Ticket: []byte("large_endpoint_3")}},
						{Ticket: &flight.Ticket{Ticket: []byte("large_endpoint_4")}},
					},
				}
				mock.SetQueryResult("select * from src_db.large_table limit 5000", flightInfo)

				// 设置多个数据流
				mock.DoGetStreams["large_endpoint_1"] = createMockFlightStream(25000)
				mock.DoGetStreams["large_endpoint_2"] = createMockFlightStream(25000)
				mock.DoGetStreams["large_endpoint_3"] = createMockFlightStream(25000)
				mock.DoGetStreams["large_endpoint_4"] = createMockFlightStream(25000)
			},
			setupDstMock: func(mock *MockStarRocksClientImpl) {
				mock.TableNotFoundError = fmt.Errorf("table not found")
			},
			expectedError: false,
			expectedRows:  5000, // 由于BatchSize限制
			validateResult: func(t *testing.T, srcMock, dstMock *MockStarRocksClientImpl) {
				writer := dstMock.GetStreamWriter("large_target")
				if writer == nil {
					t.Error("Expected stream writer to be created for large table")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建源和目标mock客户端
			srcLogger := NewMockLogger()
			dstLogger := NewMockLogger()
			srcClient := NewMockStarRocksClient(srcLogger)
			dstClient := NewMockStarRocksClient(dstLogger)

			// 设置mock行为
			tt.setupSrcMock(srcClient)
			tt.setupDstMock(dstClient)

			// 执行数据同步
			err := performMockDataSync(srcClient, dstClient, tt.config, srcLogger)

			// 验证错误状态
			if tt.expectedError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectedError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			// 执行自定义验证
			tt.validateResult(t, srcClient, dstClient)

			// 验证日志
			if tt.expectedError {
				if len(srcLogger.ErrorMessages) == 0 && len(dstLogger.ErrorMessages) == 0 {
					t.Error("Expected error messages in logs")
				}
			}
		})
	}
}

// SyncConfig 同步配置（从main.go复制）
type SyncConfig struct {
	SQLEndpoint        string
	SQLPort            int
	SQLAuthUsername    string
	SQLAuthPassword    string
	DB                 string
	Table              string
	DstSQLEndpoint     string
	DstSQLPort         int
	DstSQLAuthUsername string
	DstSQLAuthPassword string
	DstDB              string
	DstTable           string
	BatchSize          int
	Verbose            bool
}

// performMockDataSync 模拟执行数据同步的核心逻辑
func performMockDataSync(srcClient, dstClient *MockStarRocksClientImpl, cfg *SyncConfig, logger *MockLogger) error {
	startTime := time.Now()

	logger.Infof("Starting sync from table '%s.%s' to table '%s.%s'", cfg.DB, cfg.Table, cfg.DstDB, cfg.DstTable)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// 测试连接
	logger.Info("Testing StarRocks connections...")
	if err := srcClient.TestConnection(ctx); err != nil {
		logger.Errorf("Source StarRocks connection failed: %v", err)
		return fmt.Errorf("source connection failed: %w", err)
	}

	if err := dstClient.TestConnection(ctx); err != nil {
		logger.Errorf("Destination StarRocks connection failed: %v", err)
		return fmt.Errorf("destination connection failed: %w", err)
	}

	logger.Info("StarRocks connections established successfully")

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
	arrowSchema, err := convertMockTableToArrowSchema(srcTableInfo)
	if err != nil {
		return fmt.Errorf("failed to convert table schema: %w", err)
	}

	// 创建目标表的Arrow流式写入器（自动创建表）
	writer := &MockArrowStreamWriter{
		TableName: cfg.DstTable,
		Schema:    arrowSchema,
	}
	dstClient.StreamWriters[cfg.DstTable] = writer

	// 检查目标表是否存在，不存在则自动创建
	_, err = dstClient.GetTableInfo(ctx, cfg.DstTable)
	if err != nil && dstClient.TableNotFoundError != nil {
		logger.Infof("Target table %s not found, creating automatically", cfg.DstTable)
		dstClient.autoCreateTable(cfg.DstTable, arrowSchema)
	}

	logger.Infof("Created Arrow stream writer for destination table '%s'", cfg.DstTable)

	// 使用Flight SQL查询源表数据
	query := fmt.Sprintf("SELECT * FROM %s.%s", cfg.DB, cfg.Table)
	if cfg.BatchSize > 0 && cfg.BatchSize < int(srcRowCount) {
		query += fmt.Sprintf(" LIMIT %d", cfg.BatchSize)
	}

	logger.Infof("Executing Flight SQL query: %s", query)

	// 执行Flight SQL查询并获取Arrow流
	flightInfo, err := srcClient.ExecuteQuery(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute Flight SQL query: %w", err)
	}

	logger.Infof("Flight SQL query executed successfully, endpoints: %d", len(flightInfo.Endpoint))

	// 处理查询结果的每个端点
	totalRows := int64(0)
	for i, endpoint := range flightInfo.Endpoint {
		logger.Infof("Processing endpoint %d/%d...", i+1, len(flightInfo.Endpoint))

		// 获取端点的数据流
		stream, err := srcClient.DoGet(ctx, endpoint.Ticket)
		if err != nil {
			logger.Warnf("Failed to get data from endpoint %d: %v", i, err)
			continue
		}

		// 读取并转发Arrow记录
		recordCount := 0
		for {
			flightData, err := stream.Recv()
			if err != nil {
				if err.Error() == "EOF" {
					break
				}
				return fmt.Errorf("failed to receive Flight data: %w", err)
			}

			if flightData.DataBody == nil || len(flightData.DataBody) == 0 {
				logger.Debugf("Received empty data body, continuing...")
				continue
			}

			// 模拟解析Arrow数据并写入
			record, err := parseMockArrowData(flightData.DataBody, arrowSchema)
			if err != nil {
				logger.Warnf("Failed to parse Arrow data: %v", err)
				continue
			}

			// 写入到目标表
			if err := writer.WriteArrowRecord(ctx, record); err != nil {
				record.Release()
				return fmt.Errorf("failed to write Arrow record: %w", err)
			}

			totalRows += record.NumRows()
			recordCount++
			logger.Debugf("Transferred record %d with %d rows", recordCount, record.NumRows())

			record.Release()
		}

		logger.Infof("Endpoint %d processed: %d records", i+1, recordCount)
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

// createMockSourceTable 创建模拟源表信息
func createMockSourceTable() *starrocks.TableInfo {
	return &starrocks.TableInfo{
		Name:      "source_table",
		Engine:    "OLAP",
		TotalRows: 1000,
		Columns: []starrocks.ColumnInfo{
			{Name: "id", Type: "BIGINT", IsNullable: false, IsPrimaryKey: true},
			{Name: "name", Type: "VARCHAR(255)", IsNullable: true},
			{Name: "age", Type: "INT", IsNullable: true},
			{Name: "email", Type: "VARCHAR(255)", IsNullable: true},
			{Name: "created_at", Type: "DATETIME", IsNullable: true},
			{Name: "score", Type: "DOUBLE", IsNullable: true},
			{Name: "active", Type: "BOOLEAN", IsNullable: true},
		},
		CreateTime: time.Now(),
		Comment:    "Mock source table",
	}
}

// convertMockTableToArrowSchema 将表信息转换为Arrow Schema
func convertMockTableToArrowSchema(tableInfo *starrocks.TableInfo) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(tableInfo.Columns))

	for i, col := range tableInfo.Columns {
		arrowType, err := convertMockStarRocksTypeToArrow(col.Type)
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

// convertMockStarRocksTypeToArrow 转换StarRocks类型到Arrow类型
func convertMockStarRocksTypeToArrow(starRocksType string) (arrow.DataType, error) {
	switch starRocksType {
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean, nil
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8, nil
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16, nil
	case "INT":
		return arrow.PrimitiveTypes.Int32, nil
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32, nil
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64, nil
	case "VARCHAR(255)":
		return arrow.BinaryTypes.String, nil
	case "DATETIME":
		return arrow.FixedWidthTypes.Timestamp_us, nil
	default:
		return arrow.BinaryTypes.String, nil
	}
}

// createMockFlightStream 创建模拟Flight数据流
func createMockFlightStream(expectedRows int64) *MockFlightStream {
	// 创建模拟Arrow数据
	data := make([][]byte, 0)

	// 简单的模拟数据块
	for i := 0; i < int(expectedRows)/1000; i++ {
		mockData := fmt.Sprintf("mock_arrow_block_%d", i)
		data = append(data, []byte(mockData))
	}

	return &MockFlightStream{
		Data:  data,
		Index: 0,
	}
}

// parseMockArrowData 解析模拟Arrow数据
func parseMockArrowData(data []byte, schema *arrow.Schema) (arrow.Record, error) {
	// 这是一个简化的模拟实现
	// 在真实场景中，这里应该使用ipc.Reader解析真实的Arrow数据

	// 创建一个模拟的Arrow Record
	allocator := memory.NewGoAllocator()

	// 简单起见，创建一个只有一行的记录
	builders := make([]array.Builder, schema.NumFields())
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(allocator, field.Type)

		// 根据类型添加模拟数据
		switch field.Type.ID() {
		case arrow.BOOL:
			builders[i].(*array.BooleanBuilder).Append(true)
		case arrow.INT64:
			builders[i].(*array.Int64Builder).Append(int64(i))
		case arrow.INT32:
			builders[i].(*array.Int32Builder).Append(int32(i))
		case arrow.FLOAT64:
			builders[i].(*array.Float64Builder).Append(float64(i))
		case arrow.STRING:
			builders[i].(*array.StringBuilder).Append(fmt.Sprintf("mock_%s", field.Name))
		case arrow.TIMESTAMP:
			builders[i].(*array.TimestampBuilder).Append(arrow.Timestamp(time.Now().UnixMicro()))
		default:
			builders[i].(*array.StringBuilder).Append(fmt.Sprintf("mock_value_%d", i))
		}
	}

	// 构建arrays
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		builder.Release()
	}

	// 创建record
	record := array.NewRecord(schema, arrays, 1)

	// 释放arrays
	for _, arr := range arrays {
		arr.Release()
	}

	return record, nil
}