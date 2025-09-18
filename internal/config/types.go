package config

import (
	"fmt"
	"time"
)

// 数据库配置
type DatabaseConfig struct {
	Host     string `yaml:"host" mapstructure:"host"`
	Port     int    `yaml:"port" mapstructure:"port"`
	Username string `yaml:"username" mapstructure:"username"`
	Password string `yaml:"password" mapstructure:"password"`
	Database string `yaml:"database" mapstructure:"database"`
}

// Flight SQL 认证配置
type FlightSQLAuthConfig struct {
	Username string `yaml:"username" mapstructure:"username"`
	Password string `yaml:"password" mapstructure:"password"`
}

// ClickHouse 特定配置
type ClickHouseConfig struct {
	DatabaseConfig `yaml:",inline" mapstructure:",squash"`

	// ArrowStream 配置
	BatchSize        int           `yaml:"batch_size" mapstructure:"batch_size"`               // Arrow 批次大小
	CompressionType  string        `yaml:"compression_type" mapstructure:"compression_type"`   // 压缩类型 (lz4, gzip, none)

	// ClickHouse 特定参数（用于TCP连接和元数据查询）
	MaxBlockSize    int           `yaml:"max_block_size" mapstructure:"max_block_size"`
	ReadTimeout     time.Duration `yaml:"read_timeout" mapstructure:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout" mapstructure:"write_timeout"`
	MaxIdleConns    int           `yaml:"max_idle_conns" mapstructure:"max_idle_conns"`
	MaxOpenConns    int           `yaml:"max_open_conns" mapstructure:"max_open_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime" mapstructure:"conn_max_lifetime"`
}

// StarRocks 特定配置
type StarRocksConfig struct {
	DatabaseConfig `yaml:",inline" mapstructure:",squash"`

	// Flight SQL 配置
	FlightSQLEndpoint string              `yaml:"flight_sql_endpoint" mapstructure:"flight_sql_endpoint"` // Flight SQL 服务端点
	FlightSQLPort     int                 `yaml:"flight_sql_port" mapstructure:"flight_sql_port"`         // Flight SQL 端口 (默认 9090)
	FlightSQLAuth     FlightSQLAuthConfig `yaml:"flight_sql_auth" mapstructure:"flight_sql_auth"`         // Flight SQL 认证配置
	UseTLS           bool                 `yaml:"use_tls" mapstructure:"use_tls"`                         // 是否使用 TLS
	TLSCertFile      string               `yaml:"tls_cert_file" mapstructure:"tls_cert_file"`             // TLS 证书文件
	TLSKeyFile       string               `yaml:"tls_key_file" mapstructure:"tls_key_file"`               // TLS 密钥文件
	TLSCAFile        string               `yaml:"tls_ca_file" mapstructure:"tls_ca_file"`                 // TLS CA 文件

	// Arrow Flight 配置
	FlightTimeout    time.Duration `yaml:"flight_timeout" mapstructure:"flight_timeout"`       // Flight 超时时间
	BatchSize        int           `yaml:"batch_size" mapstructure:"batch_size"`               // Arrow 批次大小
	CompressionType  string        `yaml:"compression_type" mapstructure:"compression_type"`   // 压缩类型 (none, gzip, lz4, zstd)
	MaxMessageSize   int           `yaml:"max_message_size" mapstructure:"max_message_size"`   // gRPC 最大消息大小 (MB)

	// 连接池配置
	MaxIdleConns    int           `yaml:"max_idle_conns" mapstructure:"max_idle_conns"`
	MaxOpenConns    int           `yaml:"max_open_conns" mapstructure:"max_open_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime" mapstructure:"conn_max_lifetime"`
}

// 时间窗口配置
type TimeWindow struct {
	StartTime string `yaml:"start_time" mapstructure:"start_time"` // 格式: "15:04"
	EndTime   string `yaml:"end_time" mapstructure:"end_time"`     // 格式: "15:04"
}

// 数据范围配置
type DataRange struct {
	TimeColumn string `yaml:"time_column" mapstructure:"time_column"` // 时间列名
	StartTime  string `yaml:"start_time" mapstructure:"start_time"`   // 开始时间 (格式: "2006-01-02 15:04:05")
	EndTime    string `yaml:"end_time" mapstructure:"end_time"`       // 结束时间 (格式: "2006-01-02 15:04:05")
	Where      string `yaml:"where" mapstructure:"where"`             // 额外的 WHERE 条件
}

// GetStartTime 解析开始时间
func (dr *DataRange) GetStartTime() (*time.Time, error) {
	if dr.StartTime == "" {
		return nil, nil
	}
	t, err := time.Parse("2006-01-02 15:04:05", dr.StartTime)
	if err != nil {
		return nil, fmt.Errorf("invalid start_time format: %w", err)
	}
	return &t, nil
}

// GetEndTime 解析结束时间
func (dr *DataRange) GetEndTime() (*time.Time, error) {
	if dr.EndTime == "" {
		return nil, nil
	}
	t, err := time.Parse("2006-01-02 15:04:05", dr.EndTime)
	if err != nil {
		return nil, fmt.Errorf("invalid end_time format: %w", err)
	}
	return &t, nil
}

// 流量控制配置
type RateLimitConfig struct {
	MaxBytesPerSecond int `yaml:"max_bytes_per_second" mapstructure:"max_bytes_per_second"` // 每秒最大字节数
	MaxRowsPerSecond  int `yaml:"max_rows_per_second" mapstructure:"max_rows_per_second"`   // 每秒最大行数
	BurstSize         int `yaml:"burst_size" mapstructure:"burst_size"`                     // 突发大小
}

// 并发控制配置
type ConcurrencyConfig struct {
	MaxWorkers       int `yaml:"max_workers" mapstructure:"max_workers"`             // 最大工作协程数
	MaxConcurrentTasks int `yaml:"max_concurrent_tasks" mapstructure:"max_concurrent_tasks"` // 最大并发任务数
	BatchSize        int `yaml:"batch_size" mapstructure:"batch_size"`               // 批处理大小
}

// 重试配置
type RetryConfig struct {
	MaxRetries    int           `yaml:"max_retries" mapstructure:"max_retries"`       // 最大重试次数
	InitialDelay  time.Duration `yaml:"initial_delay" mapstructure:"initial_delay"`   // 初始延迟
	MaxDelay      time.Duration `yaml:"max_delay" mapstructure:"max_delay"`           // 最大延迟
	BackoffFactor float64       `yaml:"backoff_factor" mapstructure:"backoff_factor"` // 退避因子
}

// 数据完整性校验配置
type ValidateConfig struct {
	Enabled         bool     `yaml:"enabled" mapstructure:"enabled"`                   // 是否启用校验
	CheckRowCount   bool     `yaml:"check_row_count" mapstructure:"check_row_count"`   // 检查行数
	CheckChecksum   bool     `yaml:"check_checksum" mapstructure:"check_checksum"`     // 检查校验和
	CheckColumns    []string `yaml:"check_columns" mapstructure:"check_columns"`       // 检查的列
	TolerancePercent float64  `yaml:"tolerance_percent" mapstructure:"tolerance_percent"` // 容错百分比
}

// 类型转换规则配置
type TypeConversionRule struct {
	SourceType string `yaml:"source_type" mapstructure:"source_type"` // 源数据类型
	TargetType string `yaml:"target_type" mapstructure:"target_type"` // 目标数据类型
	Expression string `yaml:"expression" mapstructure:"expression"`   // 转换表达式（可选）
}

// 字段转换配置
type FieldTransformConfig struct {
	Column           string               `yaml:"column" mapstructure:"column"`                       // 列名
	TypeConversion   *TypeConversionRule  `yaml:"type_conversion" mapstructure:"type_conversion"`     // 类型转换
	DefaultValue     interface{}          `yaml:"default_value" mapstructure:"default_value"`         // 默认值
	Required         bool                 `yaml:"required" mapstructure:"required"`                   // 是否必需
	Validation       *ValidationRule      `yaml:"validation" mapstructure:"validation"`               // 验证规则
}

// 验证规则配置
type ValidationRule struct {
	MinValue   interface{} `yaml:"min_value" mapstructure:"min_value"`     // 最小值
	MaxValue   interface{} `yaml:"max_value" mapstructure:"max_value"`     // 最大值
	Pattern    string      `yaml:"pattern" mapstructure:"pattern"`         // 正则表达式模式
	AllowNull  bool        `yaml:"allow_null" mapstructure:"allow_null"`   // 是否允许空值
}

// 数据转换配置
type DataTransformConfig struct {
	Enabled         bool                   `yaml:"enabled" mapstructure:"enabled"`                   // 是否启用数据转换
	FieldTransforms []FieldTransformConfig `yaml:"field_transforms" mapstructure:"field_transforms"` // 字段转换配置
	GlobalRules     []TypeConversionRule   `yaml:"global_rules" mapstructure:"global_rules"`         // 全局类型转换规则
}

// 同步任务配置
type SyncTaskConfig struct {
	TaskID           string              `yaml:"task_id" mapstructure:"task_id"`                       // 任务唯一标识
	Name             string              `yaml:"name" mapstructure:"name"`                             // 任务名称
	Description      string              `yaml:"description" mapstructure:"description"`               // 任务描述
	Enabled          bool                `yaml:"enabled" mapstructure:"enabled"`                       // 是否启用
	SourceTable      string              `yaml:"source_table" mapstructure:"source_table"`             // 源表名
	TargetTable      string              `yaml:"target_table" mapstructure:"target_table"`             // 目标表名
	DataRange        DataRange           `yaml:"data_range" mapstructure:"data_range"`                 // 数据范围
	TimeWindow       *TimeWindow         `yaml:"time_window" mapstructure:"time_window"`               // 时间窗口
	RateLimit        RateLimitConfig     `yaml:"rate_limit" mapstructure:"rate_limit"`                 // 流量控制
	Concurrency      ConcurrencyConfig   `yaml:"concurrency" mapstructure:"concurrency"`               // 并发控制
	Retry            RetryConfig         `yaml:"retry" mapstructure:"retry"`                           // 重试配置
	Validate         ValidateConfig      `yaml:"validate" mapstructure:"validate"`                     // 数据校验
	ColumnMapping    map[string]string   `yaml:"column_mapping" mapstructure:"column_mapping"`         // 列映射
	DataTransform    DataTransformConfig `yaml:"data_transform" mapstructure:"data_transform"`         // 数据转换配置
	Priority         int                 `yaml:"priority" mapstructure:"priority"`                     // 任务优先级
	CronExpression   string              `yaml:"cron_expression" mapstructure:"cron_expression"`       // Cron 表达式
}

// 监控配置
type MonitorConfig struct {
	Enabled         bool          `yaml:"enabled" mapstructure:"enabled"`                   // 是否启用监控
	MetricsPort     int           `yaml:"metrics_port" mapstructure:"metrics_port"`         // Prometheus 指标端口
	MetricsPath     string        `yaml:"metrics_path" mapstructure:"metrics_path"`         // 指标路径
	UpdateInterval  time.Duration `yaml:"update_interval" mapstructure:"update_interval"`   // 更新间隔
	HealthCheckPort int           `yaml:"health_check_port" mapstructure:"health_check_port"` // 健康检查端口
}

// Kubernetes 配置
type KubernetesConfig struct {
	Enabled        bool   `yaml:"enabled" mapstructure:"enabled"`               // 是否启用 K8s 集成
	Namespace      string `yaml:"namespace" mapstructure:"namespace"`           // 命名空间
	ConfigMapName  string `yaml:"configmap_name" mapstructure:"configmap_name"` // ConfigMap 名称
	CRDName        string `yaml:"crd_name" mapstructure:"crd_name"`             // CRD 名称
	LabelSelector  string `yaml:"label_selector" mapstructure:"label_selector"` // 标签选择器
	PodName        string `yaml:"pod_name" mapstructure:"pod_name"`             // Pod 名称
}

// 日志配置
type LogConfig struct {
	Level      string `yaml:"level" mapstructure:"level"`           // 日志级别
	Format     string `yaml:"format" mapstructure:"format"`         // 日志格式 (json/text)
	Output     string `yaml:"output" mapstructure:"output"`         // 输出目标 (stdout/file)
	FilePath   string `yaml:"file_path" mapstructure:"file_path"`   // 日志文件路径
	MaxSize    int    `yaml:"max_size" mapstructure:"max_size"`     // 最大文件大小 (MB)
	MaxBackups int    `yaml:"max_backups" mapstructure:"max_backups"` // 最大备份文件数
	MaxAge     int    `yaml:"max_age" mapstructure:"max_age"`       // 最大保留天数
}

// 主配置结构
type Config struct {
	// 数据库连接配置
	ClickHouse ClickHouseConfig `yaml:"clickhouse" mapstructure:"clickhouse"`
	StarRocks  StarRocksConfig  `yaml:"starrocks" mapstructure:"starrocks"`

	// 全局配置
	GlobalRateLimit    RateLimitConfig   `yaml:"global_rate_limit" mapstructure:"global_rate_limit"`
	GlobalConcurrency  ConcurrencyConfig `yaml:"global_concurrency" mapstructure:"global_concurrency"`
	GlobalRetry        RetryConfig       `yaml:"global_retry" mapstructure:"global_retry"`

	// 任务配置
	SyncTasks []SyncTaskConfig `yaml:"sync_tasks" mapstructure:"sync_tasks"`

	// 系统配置
	Monitor    MonitorConfig    `yaml:"monitor" mapstructure:"monitor"`
	Kubernetes KubernetesConfig `yaml:"kubernetes" mapstructure:"kubernetes"`
	Log        LogConfig        `yaml:"log" mapstructure:"log"`

	// 其他配置
	StoragePath     string        `yaml:"storage_path" mapstructure:"storage_path"`         // 状态存储路径
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval" mapstructure:"heartbeat_interval"` // 心跳间隔
	GracefulShutdownTimeout time.Duration `yaml:"graceful_shutdown_timeout" mapstructure:"graceful_shutdown_timeout"` // 优雅关闭超时
}

// 默认配置值
func DefaultConfig() *Config {
	return &Config{
		ClickHouse: ClickHouseConfig{
			DatabaseConfig: DatabaseConfig{
				Port: 9000,
			},
			BatchSize:        10000,
			CompressionType:  "lz4",
			MaxBlockSize:     100000,
			ReadTimeout:      30 * time.Second,
			WriteTimeout:     30 * time.Second,
			MaxIdleConns:     10,
			MaxOpenConns:     100,
			ConnMaxLifetime:  time.Hour,
		},
		StarRocks: StarRocksConfig{
			DatabaseConfig: DatabaseConfig{
				Port: 9030,
			},
			FlightSQLEndpoint: "localhost",
			FlightSQLPort:     9090,
			UseTLS:           false,
			FlightTimeout:    30 * time.Second,
			BatchSize:        10000,
			CompressionType:  "lz4",
			MaxMessageSize:   100, // 100MB
			MaxIdleConns:     10,
			MaxOpenConns:     100,
			ConnMaxLifetime:  time.Hour,
		},
		GlobalRateLimit: RateLimitConfig{
			MaxBytesPerSecond: 100 * 1024 * 1024, // 100MB/s
			MaxRowsPerSecond:  100000,
			BurstSize:         1000,
		},
		GlobalConcurrency: ConcurrencyConfig{
			MaxWorkers:         10,
			MaxConcurrentTasks: 5,
			BatchSize:          10000,
		},
		GlobalRetry: RetryConfig{
			MaxRetries:    3,
			InitialDelay:  time.Second,
			MaxDelay:      30 * time.Second,
			BackoffFactor: 2.0,
		},
		Monitor: MonitorConfig{
			Enabled:         true,
			MetricsPort:     8080,
			MetricsPath:     "/metrics",
			UpdateInterval:  10 * time.Second,
			HealthCheckPort: 8081,
		},
		Kubernetes: KubernetesConfig{
			Enabled:       false,
			Namespace:     "default",
			ConfigMapName: "ck2sr-config",
			CRDName:       "ck2sr-tasks",
		},
		Log: LogConfig{
			Level:      "info",
			Format:     "json",
			Output:     "stdout",
			MaxSize:    100,
			MaxBackups: 10,
			MaxAge:     30,
		},
		StoragePath:             "./data",
		HeartbeatInterval:       30 * time.Second,
		GracefulShutdownTimeout: 30 * time.Second,
	}
}

// 验证配置
func (c *Config) Validate() error {
	// 验证 ClickHouse 配置
	if c.ClickHouse.Host == "" {
		return fmt.Errorf("clickhouse.host is required")
	}
	// ClickHouse 的 username 可以为空，默认使用 "default"

	// 验证 StarRocks 配置
	if c.StarRocks.Host == "" {
		return fmt.Errorf("starrocks.host is required")
	}
	// StarRocks 的 username 可以为空，某些环境可能不需要认证

	// 验证同步任务配置
	taskIDs := make(map[string]bool)
	for i, task := range c.SyncTasks {
		if task.TaskID == "" {
			return fmt.Errorf("sync_tasks[%d].task_id is required", i)
		}
		if taskIDs[task.TaskID] {
			return fmt.Errorf("duplicate task_id: %s", task.TaskID)
		}
		taskIDs[task.TaskID] = true

		if task.SourceTable == "" {
			return fmt.Errorf("sync_tasks[%d].source_table is required", i)
		}
		if task.TargetTable == "" {
			return fmt.Errorf("sync_tasks[%d].target_table is required", i)
		}

		// 验证时间窗口
		if task.TimeWindow != nil {
			if err := validateTimeWindow(task.TimeWindow); err != nil {
				return fmt.Errorf("sync_tasks[%d].time_window: %w", i, err)
			}
		}
	}

	return nil
}

// 验证时间窗口配置
func validateTimeWindow(tw *TimeWindow) error {
	if tw.StartTime == "" || tw.EndTime == "" {
		return fmt.Errorf("start_time and end_time are required")
	}

	_, err := time.Parse("15:04", tw.StartTime)
	if err != nil {
		return fmt.Errorf("invalid start_time format: %w", err)
	}

	_, err = time.Parse("15:04", tw.EndTime)
	if err != nil {
		return fmt.Errorf("invalid end_time format: %w", err)
	}

	return nil
}

// 检查当前时间是否在时间窗口内
func (tw *TimeWindow) IsInWindow(now time.Time) bool {
	if tw == nil {
		return true // 没有时间窗口限制
	}

	start, _ := time.Parse("15:04", tw.StartTime)
	end, _ := time.Parse("15:04", tw.EndTime)

	currentTime := now.Format("15:04")
	current, _ := time.Parse("15:04", currentTime)

	// 跨天的情况
	if start.After(end) {
		return current.After(start) || current.Before(end) || current.Equal(start) || current.Equal(end)
	}

	// 同一天的情况
	return (current.After(start) || current.Equal(start)) && (current.Before(end) || current.Equal(end))
}

// 合并任务配置和全局配置
func (c *Config) MergeTaskConfig(task *SyncTaskConfig) *SyncTaskConfig {
	merged := *task

	// 合并流量控制配置
	if task.RateLimit.MaxBytesPerSecond == 0 {
		merged.RateLimit.MaxBytesPerSecond = c.GlobalRateLimit.MaxBytesPerSecond
	}
	if task.RateLimit.MaxRowsPerSecond == 0 {
		merged.RateLimit.MaxRowsPerSecond = c.GlobalRateLimit.MaxRowsPerSecond
	}
	if task.RateLimit.BurstSize == 0 {
		merged.RateLimit.BurstSize = c.GlobalRateLimit.BurstSize
	}

	// 合并并发控制配置
	if task.Concurrency.MaxWorkers == 0 {
		merged.Concurrency.MaxWorkers = c.GlobalConcurrency.MaxWorkers
	}
	if task.Concurrency.BatchSize == 0 {
		merged.Concurrency.BatchSize = c.GlobalConcurrency.BatchSize
	}

	// 合并重试配置
	if task.Retry.MaxRetries == 0 {
		merged.Retry.MaxRetries = c.GlobalRetry.MaxRetries
	}
	if task.Retry.InitialDelay == 0 {
		merged.Retry.InitialDelay = c.GlobalRetry.InitialDelay
	}
	if task.Retry.MaxDelay == 0 {
		merged.Retry.MaxDelay = c.GlobalRetry.MaxDelay
	}
	if task.Retry.BackoffFactor == 0 {
		merged.Retry.BackoffFactor = c.GlobalRetry.BackoffFactor
	}

	return &merged
}