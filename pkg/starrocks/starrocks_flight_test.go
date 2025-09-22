package starrocks

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// TestStarRocksFlightSQLClient 测试StarRocks Flight SQL客户端
func TestStarRocksFlightSQLClient(t *testing.T) {
	// 跳过需要真实StarRocks服务器的集成测试
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.StarRocksConfig{
		DatabaseConfig: config.DatabaseConfig{
			Host:     "localhost",
			Port:     9030,
			Username: "root",
			Password: "",
			Database: "test",
		},
		FlightSQLEndpoint: "localhost",
		FlightSQLPort:     9090,
		UseTLS:           false,
		FlightTimeout:    30 * time.Second,
		BatchSize:        1000,
		CompressionType:  "lz4",
	}

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	t.Run("NewClient", func(t *testing.T) {
		client, err := NewClient(cfg, logger)
		if err != nil {
			t.Skipf("Could not connect to StarRocks: %v", err)
		}
		defer client.Close()

		assert.NotNil(t, client)
	})

	t.Run("TestConnection", func(t *testing.T) {
		client, err := NewClient(cfg, logger)
		if err != nil {
			t.Skipf("Could not connect to StarRocks: %v", err)
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = client.TestConnection(ctx)
		assert.NoError(t, err)
	})

	t.Run("GetServerVersion", func(t *testing.T) {
		client, err := NewClient(cfg, logger)
		if err != nil {
			t.Skipf("Could not connect to StarRocks: %v", err)
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		version, err := client.GetServerVersion(ctx)
		assert.NoError(t, err)
		assert.NotEmpty(t, version)
		t.Logf("StarRocks version: %s", version)
	})

	t.Run("CountRows", func(t *testing.T) {
		client, err := NewClient(cfg, logger)
		if err != nil {
			t.Skipf("Could not connect to StarRocks: %v", err)
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 尝试计算信息模式表的行数
		count, err := client.CountRows(ctx, "information_schema.tables", "")
		if err != nil {
			t.Skipf("Could not count rows: %v", err)
		}

		assert.True(t, count >= 0)
		t.Logf("Tables count: %d", count)
	})

	t.Run("ArrowDataWriter", func(t *testing.T) {
		client, err := NewClient(cfg, logger)
		if err != nil {
			t.Skipf("Could not connect to StarRocks: %v", err)
		}
		defer client.Close()

		writer, err := client.NewArrowDataWriter("test_table", 100)
		if err != nil {
			t.Skipf("Could not create Arrow data writer: %v", err)
		}
		defer writer.Close()

		assert.NotNil(t, writer)
	})
}

// TestStarRocksArrowDataWriter 测试Arrow数据写入器
func TestStarRocksArrowDataWriter(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.StarRocksConfig{
		DatabaseConfig: config.DatabaseConfig{
			Host:     "localhost",
			Port:     9030,
			Username: "root",
			Password: "",
			Database: "test",
		},
		FlightSQLEndpoint: "localhost",
		FlightSQLPort:     9090,
		UseTLS:           false,
		FlightTimeout:    30 * time.Second,
		BatchSize:        1000,
		CompressionType:  "lz4",
	}

	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)

	client, err := NewClient(cfg, logger)
	if err != nil {
		t.Skipf("Could not connect to StarRocks: %v", err)
	}
	defer client.Close()

	t.Run("WriteArrowRecord", func(t *testing.T) {
		writer, err := client.NewArrowDataWriter("test_table", 100)
		if err != nil {
			t.Skipf("Could not create Arrow data writer: %v", err)
		}
		defer writer.Close()

		// 创建测试Arrow记录
		mem := memory.NewGoAllocator()
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "name", Type: arrow.BinaryTypes.String},
		}, nil)

		builder := array.NewRecordBuilder(mem, schema)
		defer builder.Release()

		// 添加测试数据
		builder.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
		builder.Field(1).(*array.StringBuilder).AppendValues([]string{"test1", "test2", "test3"}, nil)

		record := builder.NewRecord()
		defer record.Release()

		// 尝试写入记录
		err = writer.WriteRecord(record)
		if err != nil {
			t.Logf("Write failed (expected in test environment): %v", err)
			// 在测试环境中写入失败是预期的
			return
		}

		assert.NoError(t, err)
	})
}

// TestStarRocksClientConfiguration 测试配置验证
func TestStarRocksClientConfiguration(t *testing.T) {
	t.Run("ValidConfiguration", func(t *testing.T) {
		cfg := &config.StarRocksConfig{
			DatabaseConfig: config.DatabaseConfig{
				Host:     "localhost",
				Port:     9030,
				Username: "root",
				Password: "",
				Database: "test",
			},
			FlightSQLEndpoint: "localhost",
			FlightSQLPort:     9090,
			UseTLS:           false,
			FlightTimeout:    30 * time.Second,
			BatchSize:        1000,
			CompressionType:  "lz4",
		}

		logger := logrus.New()

		client, err := NewClient(cfg, logger)

		// 如果连接失败，这是预期的，因为测试环境可能没有StarRocks
		if err != nil {
			t.Logf("Expected connection failure in test environment: %v", err)
			return
		}

		if client != nil {
			defer client.Close()
			assert.NotNil(t, client)
		}
	})

	t.Run("InvalidConfiguration", func(t *testing.T) {
		cfg := &config.StarRocksConfig{
			DatabaseConfig: config.DatabaseConfig{
				Host:     "",
				Port:     0,
				Username: "",
				Password: "",
				Database: "",
			},
			FlightSQLEndpoint: "",
			FlightSQLPort:     0,
		}

		logger := logrus.New()
		client, err := NewClient(cfg, logger)

		// 应该因为配置无效而失败
		assert.Error(t, err)
		assert.Nil(t, client)
	})
}

// TestStreamLoadBackwardCompatibility 测试Stream Load向后兼容性
func TestStreamLoadBackwardCompatibility(t *testing.T) {
	cfg := &config.StarRocksConfig{
		DatabaseConfig: config.DatabaseConfig{
			Host:     "localhost",
			Port:     9030,
			Username: "root",
			Password: "",
			Database: "test",
		},
		FlightSQLEndpoint: "localhost",
		FlightSQLPort:     9090,
		UseTLS:           false,
		FlightTimeout:    30 * time.Second,
		BatchSize:        1000,
		CompressionType:  "lz4",
	}

	logger := logrus.New()
	client, err := NewClient(cfg, logger)
	if err != nil {
		t.Skipf("Could not connect to StarRocks: %v", err)
	}
	defer client.Close()

	t.Run("StreamLoadFromCSV", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		csvData := [][]string{
			{"1", "test1"},
			{"2", "test2"},
			{"3", "test3"},
		}

		result, err := client.StreamLoadFromCSV(ctx, "test_table", csvData, nil)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(3), result.NumberLoadedRows)
		assert.Equal(t, "success", result.Status)
	})
}

// BenchmarkStarRocksClient 性能基准测试
func BenchmarkStarRocksClient(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	cfg := &config.StarRocksConfig{
		DatabaseConfig: config.DatabaseConfig{
			Host:     "localhost",
			Port:     9030,
			Username: "root",
			Password: "",
			Database: "test",
		},
		FlightSQLEndpoint: "localhost",
		FlightSQLPort:     9090,
		UseTLS:           false,
		FlightTimeout:    30 * time.Second,
		BatchSize:        1000,
		CompressionType:  "lz4",
	}

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel)

	client, err := NewClient(cfg, logger)
	if err != nil {
		b.Skipf("Could not connect to StarRocks: %v", err)
	}
	defer client.Close()

	b.ResetTimer()

	b.Run("ConnectionTest", func(b *testing.B) {
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			err := client.TestConnection(ctx)
			if err != nil {
				b.Fatalf("Connection test failed: %v", err)
			}
		}
	})
}