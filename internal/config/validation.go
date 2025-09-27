package config

import (
	"fmt"
	"strings"
	"time"
)

// validate 验证配置的完整性和有效性
func validate(config *Config) error {
	if err := validateDatabases(config); err != nil {
		return fmt.Errorf("database validation failed: %w", err)
	}

	if err := validateSyncTasks(config); err != nil {
		return fmt.Errorf("sync tasks validation failed: %w", err)
	}

	if err := validateMonitor(config); err != nil {
		return fmt.Errorf("monitor validation failed: %w", err)
	}

	if err := validateLog(config); err != nil {
		return fmt.Errorf("log validation failed: %w", err)
	}

	if err := validateService(config); err != nil {
		return fmt.Errorf("service validation failed: %w", err)
	}

	return nil
}

// validateDatabases 验证数据库配置
func validateDatabases(config *Config) error {
	// 检查ClickHouse配置
	names := make(map[string]bool)
	for _, ch := range config.ClickHouse {
		if ch.Name == "" {
			return fmt.Errorf("clickhouse name cannot be empty")
		}
		if names[ch.Name] {
			return fmt.Errorf("duplicate clickhouse name: %s", ch.Name)
		}
		names[ch.Name] = true

		// 至少需要一个协议配置
		if ch.MySQL == nil && ch.HTTP == nil {
			return fmt.Errorf("clickhouse %s must have at least one protocol configuration", ch.Name)
		}

		if ch.MySQL != nil {
			if err := validateMySQLConfig(ch.MySQL); err != nil {
				return fmt.Errorf("clickhouse %s mysql config: %w", ch.Name, err)
			}
		}

		if ch.HTTP != nil {
			if err := validateHTTPConfig(ch.HTTP); err != nil {
				return fmt.Errorf("clickhouse %s http config: %w", ch.Name, err)
			}
		}
	}

	// 检查StarRocks配置
	for _, sr := range config.StarRocks {
		if sr.Name == "" {
			return fmt.Errorf("starrocks name cannot be empty")
		}
		if names[sr.Name] {
			return fmt.Errorf("duplicate database name: %s", sr.Name)
		}
		names[sr.Name] = true

		// 至少需要一个协议配置
		if sr.MySQL == nil && sr.FlightSQL == nil && sr.HTTP == nil {
			return fmt.Errorf("starrocks %s must have at least one protocol configuration", sr.Name)
		}

		if sr.MySQL != nil {
			if err := validateMySQLConfig(sr.MySQL); err != nil {
				return fmt.Errorf("starrocks %s mysql config: %w", sr.Name, err)
			}
		}

		if sr.FlightSQL != nil {
			if err := validateFlightSQLConfig(sr.FlightSQL); err != nil {
				return fmt.Errorf("starrocks %s flightsql config: %w", sr.Name, err)
			}
		}

		if sr.HTTP != nil {
			if err := validateHTTPConfig(sr.HTTP); err != nil {
				return fmt.Errorf("starrocks %s http config: %w", sr.Name, err)
			}
		}
	}

	return nil
}

// validateMySQLConfig 验证MySQL配置
func validateMySQLConfig(config *MySQLConfig) error {
	if config.Host == "" {
		return fmt.Errorf("mysql host cannot be empty")
	}
	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("mysql port must be between 1 and 65535")
	}
	if config.Username == "" {
		return fmt.Errorf("mysql username cannot be empty")
	}
	if config.Timeout <= 0 {
		return fmt.Errorf("mysql timeout must be positive")
	}
	return nil
}

// validateHTTPConfig 验证HTTP配置
func validateHTTPConfig(config *HTTPConfig) error {
	if config.Host == "" {
		return fmt.Errorf("http host cannot be empty")
	}
	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("http port must be between 1 and 65535")
	}
	if config.Username == "" {
		return fmt.Errorf("http username cannot be empty")
	}
	if config.Timeout <= 0 {
		return fmt.Errorf("http timeout must be positive")
	}
	return nil
}

// validateFlightSQLConfig 验证FlightSQL配置
func validateFlightSQLConfig(config *FlightSQLConfig) error {
	if config.Host == "" {
		return fmt.Errorf("flightsql host cannot be empty")
	}
	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("flightsql port must be between 1 and 65535")
	}
	if config.Username == "" {
		return fmt.Errorf("flightsql username cannot be empty")
	}
	if config.Timeout <= 0 {
		return fmt.Errorf("flightsql timeout must be positive")
	}

	// 验证TLS配置
	if config.TLS.Enabled {
		if config.TLS.CertFile == "" || config.TLS.KeyFile == "" {
			return fmt.Errorf("flightsql tls cert_file and key_file are required when tls is enabled")
		}
	}

	return nil
}

// validateSyncTasks 验证同步任务配置
func validateSyncTasks(config *Config) error {
	taskIDs := make(map[string]bool)
	allDBNames := make(map[string]bool)

	// 收集所有数据库名称
	for _, ch := range config.ClickHouse {
		allDBNames[ch.Name] = true
	}
	for _, sr := range config.StarRocks {
		allDBNames[sr.Name] = true
	}

	for _, task := range config.SyncTasks {
		if task.TaskID == "" {
			return fmt.Errorf("task_id cannot be empty")
		}
		if taskIDs[task.TaskID] {
			return fmt.Errorf("duplicate task_id: %s", task.TaskID)
		}
		taskIDs[task.TaskID] = true

		if task.Name == "" {
			return fmt.Errorf("task name cannot be empty")
		}

		// 验证Reader配置
		if err := validateDataSource(&task.Reader, "reader", allDBNames); err != nil {
			return fmt.Errorf("task %s: %w", task.TaskID, err)
		}

		// 验证Writer配置
		if err := validateDataSource(&task.Writer, "writer", allDBNames); err != nil {
			return fmt.Errorf("task %s: %w", task.TaskID, err)
		}

		// 验证表数量匹配
		if len(task.Reader.Tables) != len(task.Writer.Tables) {
			return fmt.Errorf("task %s: reader tables count (%d) must match writer tables count (%d)",
				task.TaskID, len(task.Reader.Tables), len(task.Writer.Tables))
		}

		// 验证同步设置
		if err := validateSyncSettings(&task.Settings, task.TaskID); err != nil {
			return err
		}
	}

	return nil
}

// validateDataSource 验证数据源配置
func validateDataSource(source *DataSourceConfig, sourceType string, allDBNames map[string]bool) error {
	if source.Name == "" {
		return fmt.Errorf("%s name cannot be empty", sourceType)
	}
	if !allDBNames[source.Name] {
		return fmt.Errorf("%s references unknown database: %s", sourceType, source.Name)
	}

	// 验证vendor字段
	if source.Vendor == "" {
		return fmt.Errorf("%s vendor cannot be empty", sourceType)
	}

	if source.Vendor != "clickhouse" && source.Vendor != "starrocks" {
		return fmt.Errorf("%s vendor must be either 'clickhouse' or 'starrocks', got '%s'", sourceType, source.Vendor)
	}

	if source.Protocol == "" {
		return fmt.Errorf("%s protocol cannot be empty", sourceType)
	}

	validProtocols := []string{"mysql", "flightsql", "http"}
	valid := false
	for _, protocol := range validProtocols {
		if source.Protocol == protocol {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("%s protocol must be one of: %s", sourceType, strings.Join(validProtocols, ", "))
	}

	// 验证vendor与protocol的匹配关系
	switch source.Vendor {
	case "clickhouse":
		if source.Protocol != "mysql" && source.Protocol != "http" {
			return fmt.Errorf("ClickHouse vendor supports 'mysql' and 'http' protocols, got '%s'", source.Protocol)
		}
	case "starrocks":
		if source.Protocol != "mysql" && source.Protocol != "flightsql" && source.Protocol != "http" {
			return fmt.Errorf("StarRocks vendor supports 'mysql', 'flightsql' and 'http' protocols, got '%s'", source.Protocol)
		}
	}

	if source.Database == "" {
		return fmt.Errorf("%s database cannot be empty", sourceType)
	}
	if len(source.Tables) == 0 {
		return fmt.Errorf("%s must specify at least one table", sourceType)
	}

	return nil
}

// validateSyncSettings 验证同步设置
func validateSyncSettings(settings *SyncSettingsConfig, taskID string) error {
	if settings.BatchSize <= 0 {
		return fmt.Errorf("task %s: batch_size must be positive", taskID)
	}

	if settings.ParallelTables <= 0 {
		return fmt.Errorf("task %s: parallel_tables must be positive", taskID)
	}

	// 验证重试配置
	if settings.Retry.MaxRetries < 0 {
		return fmt.Errorf("task %s: max_retries cannot be negative", taskID)
	}
	if settings.Retry.InitialDelay <= 0 {
		return fmt.Errorf("task %s: initial_delay must be positive", taskID)
	}
	if settings.Retry.MaxDelay <= 0 {
		return fmt.Errorf("task %s: max_delay must be positive", taskID)
	}
	if settings.Retry.BackoffFactor <= 1.0 {
		return fmt.Errorf("task %s: backoff_factor must be greater than 1.0", taskID)
	}

	// 验证时间格式
	if settings.DataRange.StartTime != "" {
		if _, err := time.Parse("2006-01-02 15:04:05", settings.DataRange.StartTime); err != nil {
			return fmt.Errorf("task %s: invalid start_time format, use 'YYYY-MM-DD HH:MM:SS'", taskID)
		}
	}
	if settings.DataRange.EndTime != "" {
		if _, err := time.Parse("2006-01-02 15:04:05", settings.DataRange.EndTime); err != nil {
			return fmt.Errorf("task %s: invalid end_time format, use 'YYYY-MM-DD HH:MM:SS'", taskID)
		}
	}

	return nil
}

// validateMonitor 验证监控配置
func validateMonitor(config *Config) error {
	if config.Monitor.Enabled {
		if config.Monitor.MetricsPort <= 0 || config.Monitor.MetricsPort > 65535 {
			return fmt.Errorf("metrics_port must be between 1 and 65535")
		}
		if config.Monitor.HealthCheckPort <= 0 || config.Monitor.HealthCheckPort > 65535 {
			return fmt.Errorf("health_check_port must be between 1 and 65535")
		}
		if config.Monitor.MetricsPath == "" {
			return fmt.Errorf("metrics_path cannot be empty when monitoring is enabled")
		}
		if config.Monitor.UpdateInterval <= 0 {
			return fmt.Errorf("update_interval must be positive")
		}
	}
	return nil
}

// validateLog 验证日志配置
func validateLog(config *Config) error {
	validLevels := []string{"debug", "info", "warn", "error"}
	valid := false
	for _, level := range validLevels {
		if config.Log.Level == level {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("log level must be one of: %s", strings.Join(validLevels, ", "))
	}

	validFormats := []string{"json", "text"}
	valid = false
	for _, format := range validFormats {
		if config.Log.Format == format {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("log format must be one of: %s", strings.Join(validFormats, ", "))
	}

	validOutputs := []string{"stdout", "file"}
	valid = false
	for _, output := range validOutputs {
		if config.Log.Output == output {
			valid = true
			break
		}
	}
	if !valid {
		return fmt.Errorf("log output must be one of: %s", strings.Join(validOutputs, ", "))
	}

	if config.Log.Output == "file" && config.Log.FilePath == "" {
		return fmt.Errorf("file_path is required when log output is 'file'")
	}

	return nil
}

// validateService 验证服务配置
func validateService(config *Config) error {
	if config.Service.StoragePath == "" {
		return fmt.Errorf("storage_path cannot be empty")
	}
	if config.Service.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat_interval must be positive")
	}
	if config.Service.GracefulShutdownTimeout <= 0 {
		return fmt.Errorf("graceful_shutdown_timeout must be positive")
	}
	if config.Service.MaxConcurrentTasks <= 0 {
		return fmt.Errorf("max_concurrent_tasks must be positive")
	}
	return nil
}