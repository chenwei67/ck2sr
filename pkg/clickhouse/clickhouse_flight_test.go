package clickhouse

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// TestClickHouseFlightSQLClient 测试ClickHouse Flight SQL客户端
func TestClickHouseFlightSQLClient(t *testing.T) {
	// 跳过需要真实ClickHouse服务器的集成测试
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	cfg := &config.ClickHouseConfig{
		DatabaseConfig: config.DatabaseConfig{
			Host:     "localhost",
			Port:     9000,
			Username: "default",
			Password: "",
			Database: "default",
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
			t.Skipf("Could not connect to ClickHouse: %v", err)
		}
		defer client.Close()

		assert.NotNil(t, client)
	})

	t.Run("TestConnection", func(t *testing.T) {
		client, err := NewClient(cfg, logger)
		if err != nil {
			t.Skipf("Could not connect to ClickHouse: %v", err)
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
			t.Skipf("Could not connect to ClickHouse: %v", err)
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		version, err := client.GetServerVersion(ctx)
		assert.NoError(t, err)
		assert.NotEmpty(t, version)
		t.Logf("ClickHouse version: %s", version)
	})

	t.Run("ArrowDataReader", func(t *testing.T) {
		client, err := NewClient(cfg, logger)
		if err != nil {
			t.Skipf("Could not connect to ClickHouse: %v", err)
		}
		defer client.Close()

		reader := client.NewArrowDataReader("system.numbers")
		assert.NotNil(t, reader)

		// 配置读取器
		reader.WithLimit(10, 0).WithBatchSize(5)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordCount := 0
		err = reader.ReadArrowBatch(ctx, func(record arrow.Record) error {
			recordCount++
			t.Logf("Received record %d", recordCount)
			return nil
		})

		// 如果连接失败，跳过测试
		if err != nil {
			t.Skipf("Could not read from ClickHouse: %v", err)
		}

		assert.True(t, recordCount > 0, "Should have received at least one record")
	})
}

// TestClickHouseClientConfiguration 测试配置验证
func TestClickHouseClientConfiguration(t *testing.T) {
	t.Run("ValidConfiguration", func(t *testing.T) {
		cfg := &config.ClickHouseConfig{
			DatabaseConfig: config.DatabaseConfig{
				Host:     "localhost",
				Port:     9000,
				Username: "default",
				Password: "",
				Database: "default",
			},
			FlightSQLEndpoint: "localhost",
			FlightSQLPort:     9090,
			UseTLS:           false,
			FlightTimeout:    30 * time.Second,
			BatchSize:        1000,
			CompressionType:  "lz4",
		}

		logger := logrus.New()

		// 这个测试不应该失败，即使无法连接到服务器
		client, err := NewClient(cfg, logger)

		// 如果连接失败，这是预期的，因为测试环境可能没有ClickHouse
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
		cfg := &config.ClickHouseConfig{
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

// BenchmarkClickHouseClient 性能基准测试
func BenchmarkClickHouseClient(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	cfg := &config.ClickHouseConfig{
		DatabaseConfig: config.DatabaseConfig{
			Host:     "localhost",
			Port:     9000,
			Username: "default",
			Password: "",
			Database: "default",
		},
		FlightSQLEndpoint: "localhost",
		FlightSQLPort:     9090,
		UseTLS:           false,
		FlightTimeout:    30 * time.Second,
		BatchSize:        1000,
		CompressionType:  "lz4",
	}

	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel) // 减少日志输出

	client, err := NewClient(cfg, logger)
	if err != nil {
		b.Skipf("Could not connect to ClickHouse: %v", err)
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