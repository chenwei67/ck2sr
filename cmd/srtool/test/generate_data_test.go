package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
)

// TestGenerateDataCommand 测试数据生成命令
func TestGenerateDataCommand(t *testing.T) {
	tests := []struct {
		name           string
		config         *GenerateDataConfig
		setupMock      func(*MockStarRocksClientImpl)
		expectedError  bool
		expectedRows   int
		expectedTables int
		validateResult func(*testing.T, *MockStarRocksClientImpl)
	}{
		{
			name: "成功生成测试数据",
			config: &GenerateDataConfig{
				SQLEndpoint:     "localhost",
				SQLPort:         9408,
				SQLAuthUsername: "root",
				SQLAuthPassword: "password",
				DB:              "testdb",
				Table:           "test_table",
				RowCount:        1000,
				Verbose:         false,
			},
			setupMock: func(mock *MockStarRocksClientImpl) {
				// 设置连接成功
				mock.ConnectionError = nil
				// 表不存在，需要自动创建
				mock.TableNotFoundError = fmt.Errorf("table not found")
			},
			expectedError:  false,
			expectedRows:   1000,
			expectedTables: 1,
			validateResult: func(t *testing.T, mock *MockStarRocksClientImpl) {
				// 验证表被创建
				if len(mock.GetCreatedTables()) != 1 {
					t.Errorf("Expected 1 table to be created, got %d", len(mock.GetCreatedTables()))
				}
				if mock.GetCreatedTables()[0] != "test_table" {
					t.Errorf("Expected table 'test_table' to be created, got %s", mock.GetCreatedTables()[0])
				}

				// 验证数据写入器被创建
				writer := mock.GetDataWriter("test_table")
				if writer == nil {
					t.Error("Expected data writer to be created")
				} else {
					// 验证写入的数据行数
					if len(writer.WrittenRows) != 1000 {
						t.Errorf("Expected 1000 rows to be written, got %d", len(writer.WrittenRows))
					}
					// 验证数据结构完整性
					if len(writer.WrittenRows) > 0 {
						row := writer.WrittenRows[0]
						expectedFields := []string{"id", "tiny_int_col", "small_int_col", "int_col", "big_int_col",
							"float_col", "double_col", "boolean_col", "varchar_col", "char_col", "text_col",
							"date_col", "datetime_col", "timestamp_col", "json_col", "decimal_col"}
						for _, field := range expectedFields {
							if _, exists := row[field]; !exists {
								t.Errorf("Expected field %s to exist in generated data", field)
							}
						}
					}
				}
			},
		},
		{
			name: "连接失败",
			config: &GenerateDataConfig{
				SQLEndpoint:     "invalid_host",
				SQLPort:         9408,
				SQLAuthUsername: "root",
				SQLAuthPassword: "password",
				DB:              "testdb",
				Table:           "test_table",
				RowCount:        100,
				Verbose:         true,
			},
			setupMock: func(mock *MockStarRocksClientImpl) {
				mock.ConnectionError = fmt.Errorf("connection refused")
			},
			expectedError:  true,
			expectedRows:   0,
			expectedTables: 0,
			validateResult: func(t *testing.T, mock *MockStarRocksClientImpl) {
				// 验证没有表被创建
				if len(mock.GetCreatedTables()) != 0 {
					t.Errorf("Expected no tables to be created, got %d", len(mock.GetCreatedTables()))
				}
			},
		},
		{
			name: "表已存在场景",
			config: &GenerateDataConfig{
				SQLEndpoint:     "localhost",
				SQLPort:         9408,
				SQLAuthUsername: "root",
				SQLAuthPassword: "password",
				DB:              "testdb",
				Table:           "existing_table",
				RowCount:        500,
				Verbose:         false,
			},
			setupMock: func(mock *MockStarRocksClientImpl) {
				// 设置表已存在
				existingTable := mock.createDefaultTableInfo("existing_table")
				mock.SetTableInfo("existing_table", existingTable)
			},
			expectedError:  false,
			expectedRows:   500,
			expectedTables: 0, // 不需要创建新表
			validateResult: func(t *testing.T, mock *MockStarRocksClientImpl) {
				// 验证没有新表被创建
				if len(mock.GetCreatedTables()) != 0 {
					t.Errorf("Expected no new tables to be created, got %d", len(mock.GetCreatedTables()))
				}

				// 验证数据被写入到现有表
				writer := mock.GetDataWriter("existing_table")
				if writer == nil {
					t.Error("Expected data writer to be created for existing table")
				}
			},
		},
		{
			name: "小批量数据生成",
			config: &GenerateDataConfig{
				SQLEndpoint:     "localhost",
				SQLPort:         9408,
				SQLAuthUsername: "root",
				SQLAuthPassword: "password",
				DB:              "testdb",
				Table:           "small_table",
				RowCount:        10,
				Verbose:         true,
			},
			setupMock: func(mock *MockStarRocksClientImpl) {
				mock.ConnectionError = nil
				mock.TableNotFoundError = fmt.Errorf("table not found")
			},
			expectedError:  false,
			expectedRows:   10,
			expectedTables: 1,
			validateResult: func(t *testing.T, mock *MockStarRocksClientImpl) {
				writer := mock.GetDataWriter("small_table")
				if writer == nil {
					t.Error("Expected data writer to be created")
					return
				}

				// 验证所有行的数据完整性
				for i, row := range writer.WrittenRows {
					// 验证ID字段递增
					if id, ok := row["id"].(int64); ok {
						if id != int64(i) {
							t.Errorf("Expected ID %d, got %d", i, id)
						}
					} else {
						t.Errorf("Expected ID field to be int64, got %T", row["id"])
					}

					// 验证数据类型正确性
					validateDataTypes(t, row)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 创建mock对象
			logger := NewMockLogger()
			mockClient := NewMockStarRocksClient(logger)

			// 设置mock行为
			tt.setupMock(mockClient)

			// 执行数据生成
			err := performMockGenerateData(mockClient, tt.config, logger)

			// 验证错误状态
			if tt.expectedError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectedError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			// 执行自定义验证
			tt.validateResult(t, mockClient)

			// 验证日志消息
			if tt.expectedError {
				if len(logger.ErrorMessages) == 0 {
					t.Error("Expected error messages in log")
				}
			} else {
				if len(logger.InfoMessages) == 0 {
					t.Error("Expected info messages in log")
				}
			}
		})
	}
}

// GenerateDataConfig 数据生成配置（从main.go复制）
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

// performMockGenerateData 模拟执行数据生成的核心逻辑
func performMockGenerateData(client *MockStarRocksClientImpl, cfg *GenerateDataConfig, logger *MockLogger) error {
	logger.Info("Starting mock test data generation...")
	logger.Infof("Target: %s:%d %s.%s, Rows: %d", cfg.SQLEndpoint, cfg.SQLPort, cfg.DB, cfg.Table, cfg.RowCount)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger.Info("Testing StarRocks connection...")
	if err := client.TestConnection(ctx); err != nil {
		logger.Errorf("StarRocks connection failed: %v", err)
		return fmt.Errorf("connection failed: %w", err)
	}

	logger.Info("StarRocks connection established successfully")

	// 检查表是否存在
	_, err := client.GetTableInfo(ctx, cfg.Table)
	if err != nil && strings.Contains(err.Error(), "not found") {
		logger.Infof("Table %s not found, creating it automatically", cfg.Table)

		// 创建测试表的Arrow Schema
		schema := createMockTestTableArrowSchema()

		// 模拟自动创建表
		client.autoCreateTable(cfg.Table, schema)
		logger.Infof("Successfully auto-created table %s", cfg.Table)

		// 重新获取表信息
		_, err := client.GetTableInfo(ctx, cfg.Table)
		if err != nil {
			return fmt.Errorf("failed to get table info after creation: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to get table info: %w", err)
	}

	// 创建数据写入器（模拟）
	schema := createMockTestTableArrowSchema()
	writer := &MockArrowDataWriter{
		TableName:   cfg.Table,
		BatchSize:   10000,
		Schema:      schema,
		WrittenRows: make([]map[string]interface{}, 0),
	}
	client.DataWriters[cfg.Table] = writer

	logger.Infof("Created test table '%s' with comprehensive data types", cfg.Table)

	// 生成数据
	batchSize := 10000
	totalBatches := (cfg.RowCount + batchSize - 1) / batchSize

	logger.Infof("Generating %d rows in %d batches of %d rows each", cfg.RowCount, totalBatches, batchSize)

	for batch := 0; batch < totalBatches; batch++ {
		rowsInBatch := batchSize
		if batch == totalBatches-1 {
			rowsInBatch = cfg.RowCount - batch*batchSize
		}

		logger.Infof("Generating batch %d/%d (%d rows)...", batch+1, totalBatches, rowsInBatch)

		for row := 0; row < rowsInBatch; row++ {
			currentID := int64(batch*batchSize + row)
			rowData := generateMockTestDataRow(currentID)

			if err := writer.WriteRowMap(rowData); err != nil {
				return fmt.Errorf("failed to write test data row: %w", err)
			}
		}

		logger.Debugf("Batch %d written successfully", batch+1)
	}

	// 刷新数据
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	logger.Info("Test data generation completed")

	// 验证结果
	rowCount := int64(len(writer.WrittenRows))
	logger.Infof("Table '%s' now contains %d rows", cfg.Table, rowCount)

	return nil
}

// createMockTestTableArrowSchema 创建模拟测试表的Arrow Schema
func createMockTestTableArrowSchema() *arrow.Schema {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "tiny_int_col", Type: arrow.PrimitiveTypes.Int8, Nullable: true},
		{Name: "small_int_col", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "int_col", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "big_int_col", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "float_col", Type: arrow.PrimitiveTypes.Float32, Nullable: true},
		{Name: "double_col", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "boolean_col", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		{Name: "varchar_col", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "char_col", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "text_col", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "date_col", Type: arrow.FixedWidthTypes.Date32, Nullable: true},
		{Name: "datetime_col", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		{Name: "timestamp_col", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		{Name: "json_col", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "decimal_col", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}

	return arrow.NewSchema(fields, nil)
}

// generateMockTestDataRow 生成模拟测试数据行
func generateMockTestDataRow(currentID int64) map[string]interface{} {
	return map[string]interface{}{
		"id":            currentID,
		"tiny_int_col":  int8(currentID % 128),
		"small_int_col": int16(currentID % 32768),
		"int_col":       int32(currentID % 2147483647),
		"big_int_col":   currentID * 1000,
		"float_col":     float32(currentID) * 1.5,
		"double_col":    float64(currentID) * 2.5,
		"boolean_col":   currentID%2 == 0,
		"varchar_col":   fmt.Sprintf("varchar_%d", currentID),
		"char_col":      fmt.Sprintf("char_%d", currentID),
		"text_col":      fmt.Sprintf("text_content_%d", currentID),
		"date_col":      "2024-01-01",
		"datetime_col":  "2024-01-01 10:00:00",
		"timestamp_col": "2024-01-01 10:00:00",
		"json_col":      fmt.Sprintf(`{"id": %d, "type": "test"}`, currentID),
		"decimal_col":   float64(currentID) + 0.99,
	}
}

// validateDataTypes 验证数据类型正确性
func validateDataTypes(t *testing.T, row map[string]interface{}) {
	// 验证各种数据类型
	if _, ok := row["id"].(int64); !ok {
		t.Errorf("Expected id to be int64, got %T", row["id"])
	}
	if _, ok := row["tiny_int_col"].(int8); !ok {
		t.Errorf("Expected tiny_int_col to be int8, got %T", row["tiny_int_col"])
	}
	if _, ok := row["small_int_col"].(int16); !ok {
		t.Errorf("Expected small_int_col to be int16, got %T", row["small_int_col"])
	}
	if _, ok := row["int_col"].(int32); !ok {
		t.Errorf("Expected int_col to be int32, got %T", row["int_col"])
	}
	if _, ok := row["big_int_col"].(int64); !ok {
		t.Errorf("Expected big_int_col to be int64, got %T", row["big_int_col"])
	}
	if _, ok := row["float_col"].(float32); !ok {
		t.Errorf("Expected float_col to be float32, got %T", row["float_col"])
	}
	if _, ok := row["double_col"].(float64); !ok {
		t.Errorf("Expected double_col to be float64, got %T", row["double_col"])
	}
	if _, ok := row["boolean_col"].(bool); !ok {
		t.Errorf("Expected boolean_col to be bool, got %T", row["boolean_col"])
	}
	if _, ok := row["varchar_col"].(string); !ok {
		t.Errorf("Expected varchar_col to be string, got %T", row["varchar_col"])
	}
}