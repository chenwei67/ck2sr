package tests

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config == nil {
		t.Fatal("DefaultConfig() returned nil")
	}

	// 测试默认值
	if config.ClickHouse.Port != 9000 {
		t.Errorf("Expected ClickHouse port 9000, got %d", config.ClickHouse.Port)
	}

	if config.StarRocks.Port != 9030 {
		t.Errorf("Expected StarRocks port 9030, got %d", config.StarRocks.Port)
	}

	if config.GlobalRateLimit.MaxBytesPerSecond != 100*1024*1024 {
		t.Errorf("Expected rate limit 100MB/s, got %d", config.GlobalRateLimit.MaxBytesPerSecond)
	}
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name      string
		config    *Config
		expectErr bool
	}{
		{
			name: "valid config",
			config: &Config{
				ClickHouse: ClickHouseConfig{
					DatabaseConfig: DatabaseConfig{
						Host:     "localhost",
						Username: "default",
					},
				},
				StarRocks: StarRocksConfig{
					DatabaseConfig: DatabaseConfig{
						Host:     "localhost",
						Username: "root",
					},
				},
				SyncTasks: []SyncTaskConfig{
					{
						TaskID:      "test_task",
						SourceTable: "source",
						TargetTable: "target",
					},
				},
			},
			expectErr: false,
		},
		{
			name: "missing clickhouse host",
			config: &Config{
				ClickHouse: ClickHouseConfig{
					DatabaseConfig: DatabaseConfig{
						Username: "default",
					},
				},
				StarRocks: StarRocksConfig{
					DatabaseConfig: DatabaseConfig{
						Host:     "localhost",
						Username: "root",
					},
				},
			},
			expectErr: true,
		},
		{
			name: "duplicate task ids",
			config: &Config{
				ClickHouse: ClickHouseConfig{
					DatabaseConfig: DatabaseConfig{
						Host:     "localhost",
						Username: "default",
					},
				},
				StarRocks: StarRocksConfig{
					DatabaseConfig: DatabaseConfig{
						Host:     "localhost",
						Username: "root",
					},
				},
				SyncTasks: []SyncTaskConfig{
					{
						TaskID:      "test_task",
						SourceTable: "source",
						TargetTable: "target",
					},
					{
						TaskID:      "test_task",
						SourceTable: "source2",
						TargetTable: "target2",
					},
				},
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectErr && err == nil {
				t.Error("Expected validation error, got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Expected no validation error, got %v", err)
			}
		})
	}
}

func TestTimeWindow(t *testing.T) {
	tests := []struct {
		name     string
		window   *TimeWindow
		testTime time.Time
		expected bool
	}{
		{
			name:     "nil window",
			window:   nil,
			testTime: time.Now(),
			expected: true,
		},
		{
			name: "same day window - inside",
			window: &TimeWindow{
				StartTime: "09:00",
				EndTime:   "17:00",
			},
			testTime: time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC),
			expected: true,
		},
		{
			name: "same day window - outside",
			window: &TimeWindow{
				StartTime: "09:00",
				EndTime:   "17:00",
			},
			testTime: time.Date(2023, 1, 1, 20, 0, 0, 0, time.UTC),
			expected: false,
		},
		{
			name: "cross day window - inside",
			window: &TimeWindow{
				StartTime: "22:00",
				EndTime:   "06:00",
			},
			testTime: time.Date(2023, 1, 1, 2, 0, 0, 0, time.UTC),
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.window.IsInWindow(tt.testTime)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestConfigManager(t *testing.T) {
	// 创建临时配置文件
	tempDir := t.TempDir()
	configFile := filepath.Join(tempDir, "test_config.yaml")

	configContent := `
clickhouse:
  host: "test-clickhouse"
  username: "test-user"
starrocks:
  host: "test-starrocks"
  username: "test-user"
sync_tasks:
  - task_id: "test_task"
    source_table: "test_source"
    target_table: "test_target"
    enabled: true
`

	err := os.WriteFile(configFile, []byte(configContent), 0644)
	if err != nil {
		t.Fatalf("Failed to write test config file: %v", err)
	}

	// 测试配置管理器
	manager := NewManager()
	err = manager.LoadFromFile(configFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	config := manager.GetConfig()
	if config.ClickHouse.Host != "test-clickhouse" {
		t.Errorf("Expected ClickHouse host 'test-clickhouse', got '%s'", config.ClickHouse.Host)
	}

	// 测试任务配置获取
	taskConfig, err := manager.GetTaskConfig("test_task")
	if err != nil {
		t.Fatalf("Failed to get task config: %v", err)
	}

	if taskConfig.SourceTable != "test_source" {
		t.Errorf("Expected source table 'test_source', got '%s'", taskConfig.SourceTable)
	}

	// 测试启用的任务
	enabledTasks := manager.GetEnabledTasks()
	if len(enabledTasks) != 1 {
		t.Errorf("Expected 1 enabled task, got %d", len(enabledTasks))
	}
}

func TestMergeTaskConfig(t *testing.T) {
	config := &Config{
		GlobalRateLimit: RateLimitConfig{
			MaxBytesPerSecond: 1000,
			MaxRowsPerSecond:  100,
		},
		GlobalConcurrency: ConcurrencyConfig{
			MaxWorkers: 5,
			BatchSize:  1000,
		},
		GlobalRetry: RetryConfig{
			MaxRetries:   3,
			InitialDelay: time.Second,
		},
	}

	taskConfig := &SyncTaskConfig{
		TaskID: "test_task",
		RateLimit: RateLimitConfig{
			MaxBytesPerSecond: 2000, // 覆盖全局配置
		},
		// 其他字段使用全局配置
	}

	merged := config.MergeTaskConfig(taskConfig)

	// 检查覆盖的值
	if merged.RateLimit.MaxBytesPerSecond != 2000 {
		t.Errorf("Expected merged bytes per second 2000, got %d", merged.RateLimit.MaxBytesPerSecond)
	}

	// 检查使用全局配置的值
	if merged.RateLimit.MaxRowsPerSecond != 100 {
		t.Errorf("Expected merged rows per second 100, got %d", merged.RateLimit.MaxRowsPerSecond)
	}

	if merged.Concurrency.MaxWorkers != 5 {
		t.Errorf("Expected merged max workers 5, got %d", merged.Concurrency.MaxWorkers)
	}
}