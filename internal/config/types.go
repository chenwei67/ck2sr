package config

import (
	"time"
)

// Config 主配置结构
type Config struct {
	ClickHouse []ClickHouseConfig `yaml:"clickhouse"`
	StarRocks  []StarRocksConfig  `yaml:"starrocks"`
	SyncTasks  []SyncTaskConfig   `yaml:"sync_tasks"`
	Monitor    MonitorConfig      `yaml:"monitor"`
	Log        LogConfig          `yaml:"log"`
	Service    ServiceConfig      `yaml:"service"`
}

// ClickHouse 数据库配置
type ClickHouseConfig struct {
	Name  string      `yaml:"name"`
	MySQL *MySQLConfig `yaml:"mysql,omitempty"`
	HTTP  *HTTPConfig  `yaml:"http,omitempty"`
}

// StarRocks 数据库配置
type StarRocksConfig struct {
	Name      string           `yaml:"name"`
	MySQL     *MySQLConfig     `yaml:"mysql,omitempty"`
	FlightSQL *FlightSQLConfig `yaml:"flightsql,omitempty"`
	HTTP      *HTTPConfig      `yaml:"http,omitempty"`
}

// MySQL协议配置
type MySQLConfig struct {
	Host     string        `yaml:"host"`
	Port     int           `yaml:"port"`
	Username string        `yaml:"username"`
	Password string        `yaml:"password"`
	Timeout  time.Duration `yaml:"timeout"`
}

// HTTP协议配置
type HTTPConfig struct {
	Host     string        `yaml:"host"`
	Port     int           `yaml:"port"`
	Username string        `yaml:"username"`
	Password string        `yaml:"password"`
	Timeout  time.Duration `yaml:"timeout"`
}

// FlightSQL协议配置
type FlightSQLConfig struct {
	Host     string        `yaml:"host"`
	Port     int           `yaml:"port"`
	Username string        `yaml:"username"`
	Password string        `yaml:"password"`
	Timeout  time.Duration `yaml:"timeout"`
	TLS      TLSConfig     `yaml:"tls"`
}

// TLS配置
type TLSConfig struct {
	Enabled    bool   `yaml:"enabled"`
	CertFile   string `yaml:"cert_file"`
	KeyFile    string `yaml:"key_file"`
	CAFile     string `yaml:"ca_file"`
	SkipVerify bool   `yaml:"skip_verify"`
}

// 同步任务配置
type SyncTaskConfig struct {
	TaskID   string              `yaml:"task_id"`
	Name     string              `yaml:"name"`
	Enabled  bool                `yaml:"enabled"`
	Priority int                 `yaml:"priority"`
	Reader   DataSourceConfig    `yaml:"reader"`
	Writer   DataSourceConfig    `yaml:"writer"`
	Settings SyncSettingsConfig  `yaml:"settings"`
}

// 数据源配置
type DataSourceConfig struct {
	Name     string   `yaml:"name"`
	Vendor   string   `yaml:"vendor"`   // 新增：厂商配置 (starrocks/clickhouse)
	Protocol string   `yaml:"protocol"`
	Database string   `yaml:"database"`
	Tables   []string `yaml:"tables"`
}

// 同步设置配置
type SyncSettingsConfig struct {
	DataRange             DataRangeConfig              `yaml:"data_range"`
	TimeWindow            TimeWindowConfig             `yaml:"time_window"`
	RateLimit             RateLimitConfig              `yaml:"rate_limit"`
	Retry                 RetryConfig                  `yaml:"retry"`
	ColumnMapping         map[string]string            `yaml:"column_mapping"`
	TableSpecificSettings map[string]TableSpecificConfig `yaml:"table_specific_settings"`
	BatchSize             int                          `yaml:"batch_size"`
	CSVFormat             CSVFormatConfig              `yaml:"csv_format"`
	ParallelTables        int                          `yaml:"parallel_tables"`
}

// 数据范围配置
type DataRangeConfig struct {
	TimeColumn string `yaml:"time_column"`
	StartTime  string `yaml:"start_time"`
	EndTime    string `yaml:"end_time"`
}

// 时间窗口配置
type TimeWindowConfig struct {
	StartTime string `yaml:"start_time"`
	EndTime   string `yaml:"end_time"`
}

// 流量控制配置
type RateLimitConfig struct {
	MaxBytesPerSecond int64 `yaml:"max_bytes_per_second"`
	MaxRowsPerSecond  int64 `yaml:"max_rows_per_second"`
	BurstSize         int   `yaml:"burst_size"`
}

// 重试配置
type RetryConfig struct {
	MaxRetries     int           `yaml:"max_retries"`
	InitialDelay   time.Duration `yaml:"initial_delay"`
	MaxDelay       time.Duration `yaml:"max_delay"`
	BackoffFactor  float64       `yaml:"backoff_factor"`
}

// 表特定配置
type TableSpecificConfig struct {
	TimeColumn    string            `yaml:"time_column,omitempty"`
	ColumnMapping map[string]string `yaml:"column_mapping,omitempty"`
	BatchSize     int               `yaml:"batch_size,omitempty"`
}

// CSV格式配置
type CSVFormatConfig struct {
	Delimiter  string `yaml:"delimiter"`
	QuoteChar  string `yaml:"quote_char"`
	EscapeChar string `yaml:"escape_char"`
	NullValue  string `yaml:"null_value"`
	Header     bool   `yaml:"header"`
}

// 监控配置
type MonitorConfig struct {
	Enabled           bool          `yaml:"enabled"`
	MetricsPort       int           `yaml:"metrics_port"`
	MetricsPath       string        `yaml:"metrics_path"`
	UpdateInterval    time.Duration `yaml:"update_interval"`
	HealthCheckPort   int           `yaml:"health_check_port"`
}

// 日志配置
type LogConfig struct {
	Level       string `yaml:"level"`
	Format      string `yaml:"format"`
	Output      string `yaml:"output"`
	FilePath    string `yaml:"file_path"`
	MaxSize     int    `yaml:"max_size"`
	MaxBackups  int    `yaml:"max_backups"`
	MaxAge      int    `yaml:"max_age"`
	Compress    bool   `yaml:"compress"`
}

// 服务配置
type ServiceConfig struct {
	StoragePath              string        `yaml:"storage_path"`
	HeartbeatInterval        time.Duration `yaml:"heartbeat_interval"`
	GracefulShutdownTimeout  time.Duration `yaml:"graceful_shutdown_timeout"`
	MaxConcurrentTasks       int           `yaml:"max_concurrent_tasks"`
}