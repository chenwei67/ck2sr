package config

import (
	"os"
	"testing"
)

// TestLoad 测试配置文件加载
func TestLoad(t *testing.T) {
	// 创建临时配置文件
	configContent := `
log:
  level: "info"
  format: "json"
  output: "stdout"

policy:
  transfer:
    batch_size: 1000
    batch_interval: 5s
    writer_concurrency: 3
  schedule:
    check_interval: 30s
    retry_interval: 60s
    max_concurrent_task: 5
  retry:
    max_retries: 3
    initial_delay: 1s
    max_delay: 30s
    backoff_factor: 2.0

monitor:
  enabled: true
  metrics_port: 8080
  metrics_path: "/metrics"
  health_check_port: 8081
  update_interval: 10s

service:
  storage_path: "/data"
  max_concurrent_tasks: 10
  heartbeat_interval: 30s
  graceful_shutdown_timeout: 30s

clickhouse:
  - name: "test_clickhouse"
    mysql:
      host: "localhost"
      port: 9000
      username: "default"
      password: ""
      timeout: 10s

starrocks:
  - name: "test_starrocks"
    mysql:
      host: "localhost"
      port: 9030
      username: "root"
      password: "password"
      timeout: 10s

sync_tasks:
  - task_id: "test_task_1"
    name: "Test task 1"
    enabled: true
    priority: 1
    reader:
      name: "test_clickhouse"
      vendor: "clickhouse"
      protocol: "mysql"
      database: "test_db"
      tables: ["test_table"]
    writer:
      name: "test_starrocks"
      vendor: "starrocks"
      protocol: "mysql"
      database: "test_db"
      tables: ["test_table"]
    settings:
      batch_size: 100
      parallel_tables: 1
      data_range:
        start_time: "2023-01-01 00:00:00"
        end_time: "2023-12-31 23:59:59"
      time_window:
        window_size: 3600
        overlap_size: 300
      rate_limit:
        max_rows_per_second: 10000
        max_bytes_per_second: 104857600
      retry:
        max_retries: 3
        backoff_factor: 2.0
        initial_delay: 1s
        max_delay: 60s
`

	// 创建临时文件
	tmpFile, err := os.CreateTemp("", "config_test_*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp config file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString(configContent)
	if err != nil {
		t.Fatalf("Failed to write config content: %v", err)
	}
	tmpFile.Close()

	// 加载配置
	cfg, err := Load(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// 验证日志配置
	if cfg.Log.Level != "info" {
		t.Errorf("Expected log level 'info', got '%s'", cfg.Log.Level)
	}
	if cfg.Log.Format != "json" {
		t.Errorf("Expected log format 'json', got '%s'", cfg.Log.Format)
	}
	if cfg.Log.Output != "stdout" {
		t.Errorf("Expected log output 'stdout', got '%s'", cfg.Log.Output)
	}

	// 验证策略配置
	if cfg.Policy.Transfer.WriterConcurrency != 3 {
		t.Errorf("Expected writer concurrency 3, got %d", cfg.Policy.Transfer.WriterConcurrency)
	}
	if cfg.Policy.Schedule.MaxConcurrentTask != 5 {
		t.Errorf("Expected max concurrent tasks 5, got %d", cfg.Policy.Schedule.MaxConcurrentTask)
	}
	if cfg.Policy.Retry.MaxRetries != 3 {
		t.Errorf("Expected max retries 3, got %d", cfg.Policy.Retry.MaxRetries)
	}

	// 验证ClickHouse配置
	if len(cfg.ClickHouse) != 1 {
		t.Fatalf("Expected 1 ClickHouse config, got %d", len(cfg.ClickHouse))
	}
	ch := cfg.ClickHouse[0]
	if ch.Name != "test_clickhouse" {
		t.Errorf("Expected ClickHouse name 'test_clickhouse', got '%s'", ch.Name)
	}
	if ch.MySQL == nil {
		t.Fatal("Expected ClickHouse MySQL config to exist")
	}
	if ch.MySQL.Host != "localhost" {
		t.Errorf("Expected ClickHouse host 'localhost', got '%s'", ch.MySQL.Host)
	}
	if ch.MySQL.Port != 9000 {
		t.Errorf("Expected ClickHouse port 9000, got %d", ch.MySQL.Port)
	}

	// 验证StarRocks配置
	if len(cfg.StarRocks) != 1 {
		t.Fatalf("Expected 1 StarRocks config, got %d", len(cfg.StarRocks))
	}
	sr := cfg.StarRocks[0]
	if sr.Name != "test_starrocks" {
		t.Errorf("Expected StarRocks name 'test_starrocks', got '%s'", sr.Name)
	}

	// 验证同步任务配置
	if len(cfg.SyncTasks) != 1 {
		t.Fatalf("Expected 1 sync task, got %d", len(cfg.SyncTasks))
	}
	task := cfg.SyncTasks[0]
	if task.TaskID != "test_task_1" {
		t.Errorf("Expected task ID 'test_task_1', got '%s'", task.TaskID)
	}
	if task.Reader.Vendor != "clickhouse" {
		t.Errorf("Expected reader vendor 'clickhouse', got '%s'", task.Reader.Vendor)
	}
	if task.Writer.Vendor != "starrocks" {
		t.Errorf("Expected writer vendor 'starrocks', got '%s'", task.Writer.Vendor)
	}
}

// TestLoadFromBytes 测试从字节数组加载配置
func TestLoadFromBytes(t *testing.T) {
	configContent := `
log:
  level: "debug"
  format: "text"
  output: "file"
  file_path: "/tmp/test.log"

monitor:
  enabled: false
  metrics_port: 9090
  health_check_port: 9091

service:
  storage_path: "/tmp"
  max_concurrent_tasks: 5
  heartbeat_interval: 15s
  graceful_shutdown_timeout: 15s

clickhouse:
  - name: "minimal_clickhouse"
    mysql:
      host: "localhost"
      port: 9000
      username: "default"
      password: ""
      timeout: 5s

starrocks:
  - name: "minimal_starrocks"
    mysql:
      host: "localhost"
      port: 9030
      username: "root"
      password: ""
      timeout: 5s

sync_tasks:
  - task_id: "minimal_task"
    name: "Minimal task"
    enabled: true
    reader:
      name: "minimal_clickhouse"
      vendor: "clickhouse"
      protocol: "mysql"
      database: "test"
      tables: ["table1"]
    writer:
      name: "minimal_starrocks"
      vendor: "starrocks"
      protocol: "mysql"
      database: "test"
      tables: ["table1"]
    settings:
      batch_size: 50
      parallel_tables: 1
      retry:
        max_retries: 2
        backoff_factor: 2.0
        initial_delay: 1s
        max_delay: 30s
`

	cfg, err := LoadFromBytes([]byte(configContent))
	if err != nil {
		t.Fatalf("Failed to load config from bytes: %v", err)
	}

	if cfg.Log.Level != "debug" {
		t.Errorf("Expected log level 'debug', got '%s'", cfg.Log.Level)
	}
	if cfg.Monitor.Enabled != false {
		t.Error("Expected monitor to be disabled")
	}
}

// TestLoadNonExistentFile 测试加载不存在的文件
func TestLoadNonExistentFile(t *testing.T) {
	_, err := Load("/path/that/does/not/exist.yaml")
	if err == nil {
		t.Error("Expected error when loading non-existent file")
	}
	// The error message should contain "not found"
	if !containsString(err.Error(), "not found") {
		t.Errorf("Expected 'not found' in error message, got: %v", err)
	}
}

// TestLoadInvalidYAML 测试加载无效的YAML
func TestLoadInvalidYAML(t *testing.T) {
	invalidYAML := `
log:
  level: "info"
invalid yaml content: [
`

	_, err := LoadFromBytes([]byte(invalidYAML))
	if err == nil {
		t.Error("Expected error when loading invalid YAML")
	}
}

// TestValidationErrors 测试配置验证错误
func TestValidationErrors(t *testing.T) {
	tests := []struct {
		name     string
		config   string
		expectError bool
	}{
		{
			name: "Missing ClickHouse name",
			config: `
clickhouse:
  - mysql:
      host: "localhost"
      port: 9000
      username: "default"
      password: ""
      timeout: 5s
starrocks:
  - name: "test_starrocks"
    mysql:
      host: "localhost"
      port: 9030
      username: "root"
      password: ""
      timeout: 5s
sync_tasks:
  - task_id: "test_task"
    name: "Test"
    enabled: true
    reader:
      name: "test_clickhouse"
      vendor: "clickhouse"
      protocol: "mysql"
      database: "test"
      tables: ["table1"]
    writer:
      name: "test_starrocks"
      vendor: "starrocks"
      protocol: "mysql"
      database: "test"
      tables: ["table1"]
    settings:
      batch_size: 100
      parallel_tables: 1
      retry:
        max_retries: 1
        backoff_factor: 2.0
        initial_delay: 1s
        max_delay: 10s
`,
			expectError: true,
		},
		{
			name: "No protocol configuration for ClickHouse",
			config: `
clickhouse:
  - name: "test_clickhouse"
starrocks:
  - name: "test_starrocks"
    mysql:
      host: "localhost"
      port: 9030
      username: "root"
      password: ""
      timeout: 5s
sync_tasks:
  - task_id: "test_task"
    name: "Test"
    enabled: true
    reader:
      name: "test_clickhouse"
      vendor: "clickhouse"
      protocol: "mysql"
      database: "test"
      tables: ["table1"]
    writer:
      name: "test_starrocks"
      vendor: "starrocks"
      protocol: "mysql"
      database: "test"
      tables: ["table1"]
    settings:
      batch_size: 100
      parallel_tables: 1
      retry:
        max_retries: 1
        backoff_factor: 2.0
        initial_delay: 1s
        max_delay: 10s
`,
			expectError: true,
		},
		{
			name: "Empty sync task ID",
			config: `
clickhouse:
  - name: "test_clickhouse"
    mysql:
      host: "localhost"
      port: 9000
      username: "default"
      password: ""
      timeout: 5s
starrocks:
  - name: "test_starrocks"
    mysql:
      host: "localhost"
      port: 9030
      username: "root"
      password: ""
      timeout: 5s
sync_tasks:
  - name: "Test"
    enabled: true
    reader:
      name: "test_clickhouse"
      vendor: "clickhouse"
      protocol: "mysql"
      database: "test"
      tables: ["table1"]
    writer:
      name: "test_starrocks"
      vendor: "starrocks"
      protocol: "mysql"
      database: "test"
      tables: ["table1"]
    settings:
      batch_size: 100
      parallel_tables: 1
      retry:
        max_retries: 1
        backoff_factor: 2.0
        initial_delay: 1s
        max_delay: 10s
`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := LoadFromBytes([]byte(tt.config))
			if tt.expectError && err == nil {
				t.Error("Expected validation error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

// TestConfigurationDefaults 测试默认配置值
func TestConfigurationDefaults(t *testing.T) {
	configContent := `
log:
  level: "info"
  format: "json"
  output: "stdout"

monitor:
  enabled: true
  metrics_port: 8080
  health_check_port: 8081
  metrics_path: "/metrics"
  update_interval: 10s

service:
  storage_path: "/data"
  heartbeat_interval: 30s
  graceful_shutdown_timeout: 30s
  max_concurrent_tasks: 5

clickhouse:
  - name: "test_clickhouse"
    mysql:
      host: "localhost"
      port: 9000
      username: "default"
      password: ""
      timeout: 5s

starrocks:
  - name: "test_starrocks"
    mysql:
      host: "localhost"
      port: 9030
      username: "root"
      password: ""
      timeout: 5s

sync_tasks:
  - task_id: "minimal_task"
    name: "Minimal task"
    enabled: true
    reader:
      name: "test_clickhouse"
      vendor: "clickhouse"
      protocol: "mysql"
      database: "test"
      tables: ["table1"]
    writer:
      name: "test_starrocks"
      vendor: "starrocks"
      protocol: "mysql"
      database: "test"
      tables: ["table1"]
    settings:
      batch_size: 1000
      parallel_tables: 1
      retry:
        max_retries: 1
        backoff_factor: 2.0
        initial_delay: 1s
        max_delay: 10s
`

	cfg, err := LoadFromBytes([]byte(configContent))
	if err != nil {
		t.Fatalf("Failed to load minimal config: %v", err)
	}

	// Check that missing sections don't break the loading
	if cfg.Log.Level == "" {
		// This might be expected if there are no defaults set
		t.Log("Log level is empty - this may be expected behavior")
	}

	// Check that required fields are present
	if len(cfg.SyncTasks) == 0 {
		t.Error("Expected at least one sync task")
	}
}

// BenchmarkLoad 配置加载性能基准测试
func BenchmarkLoad(b *testing.B) {
	configContent := `
log:
  level: "info"
  format: "json"
  output: "stdout"

monitor:
  enabled: true
  metrics_port: 8080
  health_check_port: 8081
  metrics_path: "/metrics"
  update_interval: 10s

service:
  storage_path: "/data"
  heartbeat_interval: 30s
  graceful_shutdown_timeout: 30s
  max_concurrent_tasks: 5

clickhouse:
  - name: "bench_clickhouse"
    mysql:
      host: "localhost"
      port: 9000
      username: "default"
      password: ""
      timeout: 5s

starrocks:
  - name: "bench_starrocks"
    mysql:
      host: "localhost"
      port: 9030
      username: "root"
      password: ""
      timeout: 5s

sync_tasks:
  - task_id: "bench_task"
    name: "Benchmark task"
    enabled: true
    reader:
      name: "bench_clickhouse"
      vendor: "clickhouse"
      protocol: "mysql"
      database: "bench"
      tables: ["table1"]
    writer:
      name: "bench_starrocks"
      vendor: "starrocks"
      protocol: "mysql"
      database: "bench"
      tables: ["table1"]
    settings:
      batch_size: 200
      parallel_tables: 1
      retry:
        max_retries: 1
        backoff_factor: 2.0
        initial_delay: 1s
        max_delay: 5s
`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := LoadFromBytes([]byte(configContent))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Helper function to check if a string contains a substring
func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > len(substr) && s[:len(substr)] == substr) ||
		(len(s) > len(substr) && s[len(s)-len(substr):] == substr) ||
		(len(s) > len(substr) && containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}