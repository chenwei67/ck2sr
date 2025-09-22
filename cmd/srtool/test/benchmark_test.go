package test

import (
	"fmt"
	"testing"
)

// TestMain 测试入口点
func TestMain(m *testing.M) {
	// 可以在这里添加全局测试设置
	m.Run()
}

// BenchmarkGenerateData 数据生成性能基准测试
func BenchmarkGenerateData(b *testing.B) {
	logger := NewMockLogger()
	mockClient := NewMockStarRocksClient(logger)

	config := &GenerateDataConfig{
		SQLEndpoint:     "localhost",
		SQLPort:         9408,
		SQLAuthUsername: "root",
		SQLAuthPassword: "password",
		DB:              "benchdb",
		Table:           "bench_table",
		RowCount:        10000,
		Verbose:         false,
	}

	// 设置mock
	mockClient.ConnectionError = nil
	mockClient.TableNotFoundError = fmt.Errorf("table not found")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := performMockGenerateData(mockClient, config, logger)
		if err != nil {
			b.Fatalf("Benchmark failed: %v", err)
		}
	}
}

// BenchmarkSync 数据同步性能基准测试
func BenchmarkSync(b *testing.B) {
	srcLogger := NewMockLogger()
	dstLogger := NewMockLogger()
	srcClient := NewMockStarRocksClient(srcLogger)
	dstClient := NewMockStarRocksClient(dstLogger)

	config := &SyncConfig{
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
	}

	// 设置mock
	srcTable := createMockSourceTable()
	srcClient.SetTableInfo("source_table", srcTable)
	srcClient.SetRowCount("source_table", 10000)
	dstClient.TableNotFoundError = fmt.Errorf("table not found")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := performMockDataSync(srcClient, dstClient, config, srcLogger)
		if err != nil {
			b.Fatalf("Benchmark failed: %v", err)
		}
	}
}