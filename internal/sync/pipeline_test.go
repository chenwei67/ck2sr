package sync

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/internal/storage"
)

// TestPipelineInitialization 测试Pipeline初始化
func TestPipelineInitialization(t *testing.T) {
	// 创建测试配置
	cfg := &config.SyncTaskConfig{
		TaskID: "test_task",
		Name:   "Test Task",
		Reader: config.DataSourceConfig{
			Name:     "test_clickhouse",
			Vendor:   "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
		},
		Writer: config.DataSourceConfig{
			Name:     "test_starrocks",
			Vendor:   "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize: 100,
		},
	}

	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 2,
		},
	}

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel) // 减少测试输出

	// 创建Pipeline
	pipeline := NewPipeline(cfg, policy, logger)
	if pipeline == nil {
		t.Fatal("Failed to create pipeline")
	}

	// 检查基本属性
	if pipeline.config.TaskID != cfg.TaskID {
		t.Errorf("Expected task ID '%s', got '%s'", cfg.TaskID, pipeline.config.TaskID)
	}

	if pipeline.policy.Transfer.WriterConcurrency != policy.Transfer.WriterConcurrency {
		t.Errorf("Expected writer concurrency %d, got %d",
			policy.Transfer.WriterConcurrency, pipeline.policy.Transfer.WriterConcurrency)
	}

	// 检查统计信息初始化
	stats := pipeline.getStats()
	if stats.StartTime.IsZero() {
		t.Error("Expected start time to be set")
	}
}

// TestPipelineSetStorage 测试设置存储
func TestPipelineSetStorage(t *testing.T) {
	cfg := &config.SyncTaskConfig{
		TaskID: "storage_test_task",
		Name: "Storage Test Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
		},
	}
	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 1,
		},
	}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	pipeline := NewPipeline(cfg, policy, logger)
	mockStorage := storage.NewMemoryStorage()

	// 设置存储
	pipeline.SetStorage(mockStorage)

	if pipeline.storage != mockStorage {
		t.Error("Expected storage to be set correctly")
	}
}

// TestPipelineMonitorIntegration 测试Pipeline与Monitor的集成
func TestPipelineMonitorIntegration(t *testing.T) {
	cfg := &config.SyncTaskConfig{
		TaskID: "monitor_test_task",
		Name: "Monitor Test Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize: 10,
		},
	}
	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 1,
		},
	}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	pipeline := NewPipeline(cfg, policy, logger)

	// 检查Monitor是否正确初始化
	if pipeline.monitor == nil {
		t.Fatal("Expected monitor to be initialized")
	}

	// 检查Monitor的表统计
	allTableStats := pipeline.monitor.GetAllTableStats()
	if len(allTableStats) != 0 {
		t.Error("Expected no tables in monitor initially")
	}
}

// TestPipelineChannelInitialization 测试Pipeline Channel初始化
func TestPipelineChannelInitialization(t *testing.T) {
	cfg := &config.SyncTaskConfig{
		TaskID: "channel_test_task",
		Name: "Channel Test Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
		},
	}
	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 3,
		},
	}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	pipeline := NewPipeline(cfg, policy, logger)

	// 检查channels是否正确初始化
	if pipeline.dataChan == nil {
		t.Error("Expected dataChan to be initialized")
	}
	if pipeline.errorChan == nil {
		t.Error("Expected errorChan to be initialized")
	}
	if pipeline.progressChan == nil {
		t.Error("Expected progressChan to be initialized")
	}

	// 检查channel缓冲区大小
	dataCapacity := cap(pipeline.dataChan)
	expectedDataCapacity := policy.Transfer.WriterConcurrency * 2
	if dataCapacity != expectedDataCapacity {
		t.Errorf("Expected dataChan capacity %d, got %d", expectedDataCapacity, dataCapacity)
	}

	errorCapacity := cap(pipeline.errorChan)
	if errorCapacity != policy.Transfer.WriterConcurrency {
		t.Errorf("Expected errorChan capacity %d, got %d", policy.Transfer.WriterConcurrency, errorCapacity)
	}
}

// TestPipelineStatsUpdate 测试统计信息更新
func TestPipelineStatsUpdate(t *testing.T) {
	cfg := &config.SyncTaskConfig{
		TaskID: "stats_test_task",
		Name: "Stats Test Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
		},
	}
	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 2,
		},
	}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	pipeline := NewPipeline(cfg, policy, logger)

	// 获取初始统计
	initialStats := pipeline.getStats()
	if initialStats.TotalRows != 0 {
		t.Error("Expected initial total rows to be 0")
	}
	if initialStats.BatchCount != 0 {
		t.Error("Expected initial batch count to be 0")
	}

	// 模拟统计更新
	pipeline.statsMu.Lock()
	pipeline.stats.TotalRows = 100
	pipeline.stats.BatchCount = 5
	pipeline.statsMu.Unlock()

	// 检查更新后的统计
	updatedStats := pipeline.getStats()
	if updatedStats.TotalRows != 100 {
		t.Errorf("Expected total rows 100, got %d", updatedStats.TotalRows)
	}
	if updatedStats.BatchCount != 5 {
		t.Errorf("Expected batch count 5, got %d", updatedStats.BatchCount)
	}
}

// TestPipelineEndTime 测试结束时间更新
func TestPipelineEndTime(t *testing.T) {
	cfg := &config.SyncTaskConfig{
		TaskID: "endtime_test_task",
		Name: "EndTime Test Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
		},
	}
	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 1,
		},
	}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	pipeline := NewPipeline(cfg, policy, logger)

	// 检查初始结束时间
	initialStats := pipeline.getStats()
	if !initialStats.EndTime.IsZero() {
		t.Error("Expected initial end time to be zero")
	}

	// 更新结束时间
	pipeline.updateEndTime()

	// 检查结束时间是否被设置
	updatedStats := pipeline.getStats()
	if updatedStats.EndTime.IsZero() {
		t.Error("Expected end time to be set after update")
	}

	// 检查结束时间是否在开始时间之后
	if updatedStats.EndTime.Before(updatedStats.StartTime) {
		t.Error("Expected end time to be after start time")
	}
}

// TestDataBatchStructure 测试DataBatch结构
func TestDataBatchStructure(t *testing.T) {
	testData := []interface{}{
		map[string]interface{}{"id": 1, "name": "test1"},
		map[string]interface{}{"id": 2, "name": "test2"},
	}

	batch := DataBatch{
		Data:   testData,
		Offset: 100,
		Table:  "test_table",
	}

	if len(batch.Data) != len(testData) {
		t.Errorf("Expected batch data length %d, got %d", len(testData), len(batch.Data))
	}
	if batch.Offset != 100 {
		t.Errorf("Expected batch offset 100, got %d", batch.Offset)
	}
	if batch.Table != "test_table" {
		t.Errorf("Expected batch table 'test_table', got '%s'", batch.Table)
	}
}

// TestProgressInfoStructure 测试ProgressInfo结构
func TestProgressInfoStructure(t *testing.T) {
	progress := ProgressInfo{
		Table:         "progress_table",
		Offset:        200,
		ProcessedRows: 1500,
		BatchCount:    15,
	}

	if progress.Table != "progress_table" {
		t.Errorf("Expected progress table 'progress_table', got '%s'", progress.Table)
	}
	if progress.Offset != 200 {
		t.Errorf("Expected progress offset 200, got %d", progress.Offset)
	}
	if progress.ProcessedRows != 1500 {
		t.Errorf("Expected processed rows 1500, got %d", progress.ProcessedRows)
	}
	if progress.BatchCount != 15 {
		t.Errorf("Expected batch count 15, got %d", progress.BatchCount)
	}
}

// TestPipelineConcurrencySafety 测试Pipeline并发安全性
func TestPipelineConcurrencySafety(t *testing.T) {
	cfg := &config.SyncTaskConfig{
		TaskID: "concurrency_test_task",
		Name: "Concurrency Test Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
		},
	}
	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 3,
		},
	}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	pipeline := NewPipeline(cfg, policy, logger)

	// 并发读取统计信息
	numGoroutines := 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			// 并发读取统计信息
			for j := 0; j < 100; j++ {
				stats := pipeline.getStats()
				if stats.StartTime.IsZero() {
					t.Errorf("Got invalid stats in goroutine %d", id)
					return
				}

				// 并发更新统计信息
				pipeline.statsMu.Lock()
				pipeline.stats.TotalRows += int64(id)
				pipeline.statsMu.Unlock()
			}
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	// 检查最终统计
	finalStats := pipeline.getStats()
	if finalStats.TotalRows < 0 {
		t.Error("Expected non-negative total rows after concurrent updates")
	}
}

// BenchmarkPipelineStats 性能基准测试
func BenchmarkPipelineStats(b *testing.B) {
	cfg := &config.SyncTaskConfig{
		TaskID: "benchmark_task",
		Name: "Benchmark Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
		},
	}
	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 1,
		},
	}
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	pipeline := NewPipeline(cfg, policy, logger)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stats := pipeline.getStats()
		if stats.StartTime.IsZero() {
			b.Fatal("Got invalid stats")
		}
	}
}