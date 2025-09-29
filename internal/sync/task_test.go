package sync

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/internal/storage"
)

// TestSyncTaskCreation 测试SyncTask创建
func TestSyncTaskCreation(t *testing.T) {
	// 创建测试配置
	cfg := &config.SyncTaskConfig{
		TaskID:  "test_task",
		Name:    "Test Task",
		Enabled: true,
		Reader: config.DataSourceConfig{
			Name:     "test_clickhouse",
			Vendor:   "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
			Tables:   []string{"table1", "table2", "table3"},
		},
		Writer: config.DataSourceConfig{
			Name:     "test_starrocks",
			Vendor:   "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize:       1000,
			ParallelTables:  2,
			BatchInterval:   time.Second * 5,
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

	// 创建SyncTask
	task := NewSyncTask(cfg, policy, mockStorage, logger)

	// 验证基本属性
	if task.GetTaskID() != cfg.TaskID {
		t.Errorf("Expected task ID '%s', got '%s'", cfg.TaskID, task.GetTaskID())
	}

	if task.GetName() != cfg.Name {
		t.Errorf("Expected task name '%s', got '%s'", cfg.Name, task.GetName())
	}

	if task.GetStatus() != TaskStatusIdle {
		t.Errorf("Expected initial status '%s', got '%s'", TaskStatusIdle, task.GetStatus())
	}

	if !task.IsEnabled() {
		t.Error("Expected task to be enabled")
	}

	// 验证TableSyncJob创建
	allJobs := task.GetAllTableJobs()
	if len(allJobs) != len(cfg.Reader.Tables) {
		t.Errorf("Expected %d jobs, got %d", len(cfg.Reader.Tables), len(allJobs))
	}

	for _, tableName := range cfg.Reader.Tables {
		job, exists := task.GetTableJob(tableName)
		if !exists {
			t.Errorf("Expected job for table '%s' to exist", tableName)
		}
		if job.GetTableName() != tableName {
			t.Errorf("Expected job table name '%s', got '%s'", tableName, job.GetTableName())
		}
	}
}

// TestSyncTaskTableJobs 测试TableJob管理
func TestSyncTaskTableJobs(t *testing.T) {
	cfg := &config.SyncTaskConfig{
		TaskID:  "jobs_test_task",
		Name:    "Jobs Test Task",
		Enabled: true,
		Reader: config.DataSourceConfig{
			Name:     "test_clickhouse",
			Vendor:   "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
			Tables:   []string{"users", "orders", "products"},
		},
		Writer: config.DataSourceConfig{
			Name:     "test_starrocks",
			Vendor:   "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize:      500,
			ParallelTables: 3,
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

	task := NewSyncTask(cfg, policy, mockStorage, logger)

	// 测试GetTableJob
	job, exists := task.GetTableJob("users")
	if !exists {
		t.Fatal("Expected job for 'users' table to exist")
	}
	if job.GetTableName() != "users" {
		t.Errorf("Expected table name 'users', got '%s'", job.GetTableName())
	}

	// 测试不存在的表
	_, exists = task.GetTableJob("nonexistent")
	if exists {
		t.Error("Expected job for 'nonexistent' table to not exist")
	}

	// 测试GetAllTableJobs
	allJobs := task.GetAllTableJobs()
	expectedTables := map[string]bool{
		"users":    true,
		"orders":   true,
		"products": true,
	}

	if len(allJobs) != len(expectedTables) {
		t.Errorf("Expected %d jobs, got %d", len(expectedTables), len(allJobs))
	}

	for tableName := range allJobs {
		if !expectedTables[tableName] {
			t.Errorf("Unexpected table '%s' in jobs", tableName)
		}
	}
}

// TestSyncTaskProgress 测试进度管理
func TestSyncTaskProgress(t *testing.T) {
	cfg := &config.SyncTaskConfig{
		TaskID:  "progress_test_task",
		Name:    "Progress Test Task",
		Enabled: true,
		Reader: config.DataSourceConfig{
			Name:     "test_clickhouse",
			Vendor:   "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
			Tables:   []string{"test_table"},
		},
		Writer: config.DataSourceConfig{
			Name:     "test_starrocks",
			Vendor:   "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize:      100,
			ParallelTables: 1,
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

	task := NewSyncTask(cfg, policy, mockStorage, logger)

	// 测试GetTableProgress
	progress := task.GetTableProgress("test_table")
	if progress == nil {
		t.Fatal("Expected progress for 'test_table' to exist")
	}
	if progress.TableName != "test_table" {
		t.Errorf("Expected table name 'test_table', got '%s'", progress.TableName)
	}
	if progress.TaskID != cfg.TaskID {
		t.Errorf("Expected task ID '%s', got '%s'", cfg.TaskID, progress.TaskID)
	}

	// 测试GetAllTableProgress
	allProgress := task.GetAllTableProgress()
	if len(allProgress) != 1 {
		t.Errorf("Expected 1 progress entry, got %d", len(allProgress))
	}
	if allProgress["test_table"] == nil {
		t.Error("Expected progress for 'test_table' to exist in all progress")
	}
}

// TestSyncTaskStatusManagement 测试状态管理
func TestSyncTaskStatusManagement(t *testing.T) {
	cfg := &config.SyncTaskConfig{
		TaskID:  "status_test_task",
		Name:    "Status Test Task",
		Enabled: false, // 禁用状态
		Reader: config.DataSourceConfig{
			Name:     "test_clickhouse",
			Vendor:   "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
			Tables:   []string{"test_table"},
		},
		Writer: config.DataSourceConfig{
			Name:     "test_starrocks",
			Vendor:   "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize:      100,
			ParallelTables: 1,
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

	task := NewSyncTask(cfg, policy, mockStorage, logger)

	// 初始状态应该是Idle
	if task.GetStatus() != TaskStatusIdle {
		t.Errorf("Expected initial status '%s', got '%s'", TaskStatusIdle, task.GetStatus())
	}

	// 测试禁用任务执行
	ctx := context.Background()
	err := task.Execute(ctx)
	if err != nil {
		t.Errorf("Expected no error for disabled task, got: %v", err)
	}

	// 状态应该仍然是Idle（因为任务被跳过）
	if task.GetStatus() != TaskStatusIdle {
		t.Errorf("Expected status to remain '%s' for disabled task, got '%s'", TaskStatusIdle, task.GetStatus())
	}
}

// TestSyncTaskStop 测试停止功能
func TestSyncTaskStop(t *testing.T) {
	cfg := &config.SyncTaskConfig{
		TaskID:  "stop_test_task",
		Name:    "Stop Test Task",
		Enabled: true,
		Reader: config.DataSourceConfig{
			Name:     "test_clickhouse",
			Vendor:   "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
			Tables:   []string{"test_table"},
		},
		Writer: config.DataSourceConfig{
			Name:     "test_starrocks",
			Vendor:   "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize:      100,
			ParallelTables: 1,
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

	task := NewSyncTask(cfg, policy, mockStorage, logger)

	// 测试停止任务
	err := task.Stop()
	if err != nil {
		t.Errorf("Expected no error when stopping task, got: %v", err)
	}

	// 状态应该变为Cancelled
	if task.GetStatus() != TaskStatusCancelled {
		t.Errorf("Expected status '%s' after stop, got '%s'", TaskStatusCancelled, task.GetStatus())
	}
}

// TestSyncTaskConcurrency 测试并发安全性
func TestSyncTaskConcurrency(t *testing.T) {
	cfg := &config.SyncTaskConfig{
		TaskID:  "concurrent_test_task",
		Name:    "Concurrent Test Task",
		Enabled: true,
		Reader: config.DataSourceConfig{
			Name:     "test_clickhouse",
			Vendor:   "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
			Tables:   []string{"table1", "table2", "table3"},
		},
		Writer: config.DataSourceConfig{
			Name:     "test_starrocks",
			Vendor:   "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize:      100,
			ParallelTables: 3,
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

	task := NewSyncTask(cfg, policy, mockStorage, logger)

	// 并发访问测试
	numGoroutines := 10
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer func() { done <- true }()

			// 并发读取作业
			for j := 0; j < 100; j++ {
				allJobs := task.GetAllTableJobs()
				if len(allJobs) != 3 {
					t.Errorf("Goroutine %d: expected 3 jobs, got %d", id, len(allJobs))
					return
				}

				// 并发读取进度
				allProgress := task.GetAllTableProgress()
				if len(allProgress) != 3 {
					t.Errorf("Goroutine %d: expected 3 progress entries, got %d", id, len(allProgress))
					return
				}

				// 读取状态
				status := task.GetStatus()
				if status == "" {
					t.Errorf("Goroutine %d: got empty status", id)
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

// TestSyncTaskConfiguration 测试配置记录
func TestSyncTaskConfiguration(t *testing.T) {
	cfg := &config.SyncTaskConfig{
		TaskID:  "config_test_task",
		Name:    "Config Test Task",
		Enabled: true,
		Reader: config.DataSourceConfig{
			Name:     "test_clickhouse",
			Vendor:   "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
			Tables:   []string{"test_table"},
		},
		Writer: config.DataSourceConfig{
			Name:     "test_starrocks",
			Vendor:   "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize:      1000,
			ParallelTables: 2,
			BatchInterval:  time.Second * 10,
			DataRange: config.DataRangeConfig{
				StartTime: "2023-01-01 00:00:00",
				EndTime:   "2023-12-31 23:59:59",
			},
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

	task := NewSyncTask(cfg, policy, mockStorage, logger)

	// 测试配置访问方法
	taskConfig := task.GetTaskConfig()
	if taskConfig.TaskID != cfg.TaskID {
		t.Errorf("Expected task ID '%s', got '%s'", cfg.TaskID, taskConfig.TaskID)
	}

	if taskConfig.Settings.BatchSize != cfg.Settings.BatchSize {
		t.Errorf("Expected batch size %d, got %d", cfg.Settings.BatchSize, taskConfig.Settings.BatchSize)
	}

	if taskConfig.Settings.ParallelTables != cfg.Settings.ParallelTables {
		t.Errorf("Expected parallel tables %d, got %d", cfg.Settings.ParallelTables, taskConfig.Settings.ParallelTables)
	}
}

// BenchmarkSyncTaskOperations 性能基准测试
func BenchmarkSyncTaskOperations(b *testing.B) {
	cfg := &config.SyncTaskConfig{
		TaskID:  "benchmark_task",
		Name:    "Benchmark Task",
		Enabled: true,
		Reader: config.DataSourceConfig{
			Name:     "test_clickhouse",
			Vendor:   "clickhouse",
			Protocol: "mysql",
			Database: "test_db",
			Tables:   []string{"table1", "table2", "table3", "table4", "table5"},
		},
		Writer: config.DataSourceConfig{
			Name:     "test_starrocks",
			Vendor:   "starrocks",
			Protocol: "mysql",
			Database: "test_db",
		},
		Settings: config.SyncSettingsConfig{
			BatchSize:      1000,
			ParallelTables: 5,
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

	task := NewSyncTask(cfg, policy, mockStorage, logger)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 基准测试主要操作
		allJobs := task.GetAllTableJobs()
		if len(allJobs) != 5 {
			b.Fatal("Expected 5 jobs")
		}

		allProgress := task.GetAllTableProgress()
		if len(allProgress) != 5 {
			b.Fatal("Expected 5 progress entries")
		}

		status := task.GetStatus()
		if status == "" {
			b.Fatal("Got empty status")
		}
	}
}