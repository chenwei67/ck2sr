package sync

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/internal/storage"
)

// TestTableSyncJobCreation 测试 TableSyncJob 创建
func TestTableSyncJobCreation(t *testing.T) {
	// 创建测试配置
	taskConfig := &config.SyncTaskConfig{
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
			WriterConcurrency: 1,
		},
	}

	mockStorage := storage.NewMemoryStorage()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel) // 减少测试输出

	// 创建 TableSyncJob 实例
	job := NewTableSyncJob("test_task", "test_table", "test_table", taskConfig, policy, nil, nil, mockStorage, logger)

	// 测试基本属性
	if job.GetTableName() != "test_table" {
		t.Errorf("Expected table name 'test_table', got '%s'", job.GetTableName())
	}

	// 测试初始进度
	progress := job.GetProgress()
	if progress == nil {
		t.Fatal("Expected progress to be initialized")
	}
	if progress.TableName != "test_table" {
		t.Errorf("Expected progress table name 'test_table', got '%s'", progress.TableName)
	}
	if progress.Status != "pending" {
		t.Errorf("Expected initial status 'pending', got '%s'", progress.Status)
	}
}

// TestTableSyncJobProgress 测试进度更新
func TestTableSyncJobProgress(t *testing.T) {
	taskConfig := &config.SyncTaskConfig{
		TaskID: "progress_task",
		Name: "Progress Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize: 50,
		},
	}

	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 1,
		},
	}

	mockStorage := storage.NewMemoryStorage()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	job := NewTableSyncJob("progress_task", "progress_table", "progress_table", taskConfig, policy, nil, nil, mockStorage, logger)

	// 检查初始进度
	initialProgress := job.GetProgress()
	if initialProgress.ProcessedRows != 0 {
		t.Error("Expected initial processed rows to be 0")
	}
	if initialProgress.Status != "pending" {
		t.Error("Expected initial status to be 'pending'")
	}

	// 注意：由于 TableSyncJob 的进度更新是在实际同步过程中进行的，
	// 这里我们主要测试进度结构的正确性
	if initialProgress.TaskID != "progress_task" {
		t.Errorf("Expected task ID 'progress_task', got '%s'", initialProgress.TaskID)
	}
	if initialProgress.TableName != "progress_table" {
		t.Errorf("Expected table name 'progress_table', got '%s'", initialProgress.TableName)
	}
}

// TestTableSyncJobStats 测试统计信息
func TestTableSyncJobStats(t *testing.T) {
	taskConfig := &config.SyncTaskConfig{
		TaskID: "stats_task",
		Name: "Stats Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize: 25,
		},
	}

	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 1,
		},
	}

	mockStorage := storage.NewMemoryStorage()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	job := NewTableSyncJob("stats_task", "stats_table", "stats_table", taskConfig, policy, nil, nil, mockStorage, logger)

	// 检查进度结构（替代统计信息检查）
	progress := job.GetProgress()
	if progress.TableName != "stats_table" {
		t.Errorf("Expected table name 'stats_table', got '%s'", progress.TableName)
	}
	if progress.ProcessedRows != 0 {
		t.Error("Expected initial processed rows to be 0")
	}
	if !progress.StartTime.IsZero() {
		t.Error("Expected initial start time to be zero")
	}
	if !progress.EndTime.IsZero() {
		t.Error("Expected initial end time to be zero")
	}
}

// TestTableSyncJobStop 测试停止功能
func TestTableSyncJobStop(t *testing.T) {
	taskConfig := &config.SyncTaskConfig{
		TaskID: "stop_task",
		Name: "Stop Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
			Protocol: "mysql",
			Database: "test_db",
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

	mockStorage := storage.NewMemoryStorage()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	job := NewTableSyncJob("stop_task", "stop_table", "stop_table", taskConfig, policy, nil, nil, mockStorage, logger)

	// 测试停止未启动的作业
	err := job.Stop()
	if err != nil {
		t.Errorf("Expected no error when stopping unstarted job, got: %v", err)
	}

	// 检查进度状态
	progress := job.GetProgress()
	if progress.Status == "running" {
		t.Error("Job should not be running after stop")
	}
}

// TestTableSyncJobConcurrency 测试并发安全性
func TestTableSyncJobConcurrency(t *testing.T) {
	taskConfig := &config.SyncTaskConfig{
		TaskID: "concurrent_task",
		Name: "Concurrent Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize: 100,
		},
	}

	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 3,
		},
	}

	mockStorage := storage.NewMemoryStorage()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	job := NewTableSyncJob("concurrent_task", "concurrent_table", "concurrent_table", taskConfig, policy, nil, nil, mockStorage, logger)

	// 并发读取进度和统计
	numGoroutines := 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < 100; j++ {
				// 并发读取进度
				progress := job.GetProgress()
				if progress == nil {
					t.Errorf("Got nil progress in goroutine %d", id)
					return
				}

				// 检查数据一致性
				if progress.TableName != "concurrent_table" {
					t.Errorf("Table name mismatch in goroutine %d", id)
					return
				}
			}
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}

// TestTableSyncJobProgressPersistence 测试进度持久化
func TestTableSyncJobProgressPersistence(t *testing.T) {
	taskConfig := &config.SyncTaskConfig{
		TaskID: "persistence_task",
		Name: "Persistence Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize: 50,
		},
	}

	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 1,
		},
	}

	mockStorage := storage.NewMemoryStorage()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// 预先在存储中设置一些进度数据
	savedProgress := &storage.TableSyncProgress{
		TaskID:        "persistence_task",
		TableName:     "persistence_table",
		Offset:        100,
		ProcessedRows: 500,
		TotalRows:     1000,
		Status:        "running",
		LastSyncTime:  time.Now().Add(-1 * time.Hour),
	}

	err := mockStorage.SaveProgress("persistence_task", "persistence_table", savedProgress)
	if err != nil {
		t.Fatalf("Failed to save test progress: %v", err)
	}

	// 创建新的作业实例，应该加载已保存的进度
	job := NewTableSyncJob("persistence_task", "persistence_table", "persistence_table", taskConfig, policy, nil, nil, mockStorage, logger)

	// 检查是否正确加载了进度（注意：实际的加载逻辑在 Start 方法中）
	progress := job.GetProgress()
	if progress.TaskID != "persistence_task" {
		t.Errorf("Expected task ID 'persistence_task', got '%s'", progress.TaskID)
	}
	if progress.TableName != "persistence_table" {
		t.Errorf("Expected table name 'persistence_table', got '%s'", progress.TableName)
	}
}

// TestTableSyncJobMultipleTables 测试多表场景
func TestTableSyncJobMultipleTables(t *testing.T) {
	taskConfig := &config.SyncTaskConfig{
		TaskID: "multi_table_task",
		Name: "Multi Table Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize: 20,
		},
	}

	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 2,
		},
	}

	mockStorage := storage.NewMemoryStorage()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	// 创建多个表的同步作业
	tables := []string{"table1", "table2", "table3"}
	jobs := make([]TableSyncJob, len(tables))

	for i, tableName := range tables {
		jobs[i] = NewTableSyncJob("multi_table_task", tableName, tableName, taskConfig, policy, nil, nil, mockStorage, logger)
	}

	// 验证每个作业的独立性
	for i, job := range jobs {
		expectedTable := tables[i]
		if job.GetTableName() != expectedTable {
			t.Errorf("Job %d: expected table name '%s', got '%s'", i, expectedTable, job.GetTableName())
		}

		progress := job.GetProgress()
		if progress.TableName != expectedTable {
			t.Errorf("Job %d: expected progress table name '%s', got '%s'", i, expectedTable, progress.TableName)
		}
	}
}

// BenchmarkTableSyncJobCreation 性能基准测试
func BenchmarkTableSyncJobCreation(b *testing.B) {
	taskConfig := &config.SyncTaskConfig{
		TaskID: "benchmark_task",
		Name: "Benchmark Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize: 100,
		},
	}

	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 1,
		},
	}

	mockStorage := storage.NewMemoryStorage()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		job := NewTableSyncJob("benchmark_task", "benchmark_table", "benchmark_table", taskConfig, policy, nil, nil, mockStorage, logger)
		if job == nil {
			b.Fatal("Failed to create job")
		}
	}
}

// BenchmarkTableSyncJobOperations 操作性能基准测试
func BenchmarkTableSyncJobOperations(b *testing.B) {
	taskConfig := &config.SyncTaskConfig{
		TaskID: "ops_benchmark_task",
		Name: "Ops Benchmark Task",
		Reader: config.DataSourceConfig{
			Name: "test_clickhouse",
			Vendor: "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
		},
		Writer: config.DataSourceConfig{
			Name: "test_starrocks",
			Vendor: "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize: 100,
		},
	}

	policy := &config.PolicyConfig{
		Transfer: config.TransferPolicyConfig{
			WriterConcurrency: 1,
		},
	}

	mockStorage := storage.NewMemoryStorage()
	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)

	job := NewTableSyncJob("ops_benchmark_task", "ops_benchmark_table", "ops_benchmark_table", taskConfig, policy, nil, nil, mockStorage, logger)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		progress := job.GetProgress()
		if progress == nil {
			b.Fatal("Got nil progress")
		}
	}
}