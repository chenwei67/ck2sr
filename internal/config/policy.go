package config

import (
	"time"
)

// PolicyConfig 策略配置
type PolicyConfig struct {
	Transfer TransferPolicyConfig `yaml:"transfer"`
	Schedule SchedulePolicyConfig `yaml:"schedule"`
	HTTP     HTTPPolicyConfig     `yaml:"http"`
	Filter   FilterPolicyConfig   `yaml:"filter"`
	Retry    RetryPolicyConfig    `yaml:"retry"`
}

// TransferPolicyConfig 传输策略配置
type TransferPolicyConfig struct {
	BatchSize             int           `yaml:"batch_size"`               // 按条数攒批（全局默认）
	BatchBytes            int64         `yaml:"batch_bytes"`              // 按数据大小攒批（字节数，全局默认）
	BatchInterval         time.Duration `yaml:"batch_interval"`
	ProgressReportEvery   int           `yaml:"progress_report_every"`
	ProgressReportTimeout time.Duration `yaml:"progress_report_timeout"`
	RateLimitSleep        time.Duration `yaml:"rate_limit_sleep"`
	WriterConcurrency     int           `yaml:"writer_concurrency"`       // 新增：Writer并发数量
}

// SchedulePolicyConfig 调度策略配置
type SchedulePolicyConfig struct {
	CheckInterval     time.Duration `yaml:"check_interval"`
	RetryInterval     time.Duration `yaml:"retry_interval"`
	MaxConcurrentTask int           `yaml:"max_concurrent_task"`
}

// HTTPPolicyConfig HTTP策略配置
type HTTPPolicyConfig struct {
	Timeout            time.Duration `yaml:"timeout"`
	IdleConnTimeout    time.Duration `yaml:"idle_conn_timeout"`
	TLSHandshakeTimeout time.Duration `yaml:"tls_handshake_timeout"`
	MaxIdleConns       int           `yaml:"max_idle_conns"`
	MaxConnsPerHost    int           `yaml:"max_conns_per_host"`
}

// FilterPolicyConfig 过滤策略配置
type FilterPolicyConfig struct {
	ExcludeColumns []string               `yaml:"exclude_columns"`
	FixedValues    map[string]interface{} `yaml:"fixed_values"`
}

// RetryPolicyConfig 重试策略配置
type RetryPolicyConfig struct {
	MaxRetries    int           `yaml:"max_retries"`
	InitialDelay  time.Duration `yaml:"initial_delay"`
	MaxDelay      time.Duration `yaml:"max_delay"`
	BackoffFactor float64       `yaml:"backoff_factor"`
}

// DefaultPolicyConfig 默认策略配置
func DefaultPolicyConfig() *PolicyConfig {
	return &PolicyConfig{
		Transfer: TransferPolicyConfig{
			BatchSize:             10000,
			BatchBytes:            0,              // 默认不启用字节数限制
			BatchInterval:         5 * time.Second,
			ProgressReportEvery:   10,
			ProgressReportTimeout: 10 * time.Second,
			RateLimitSleep:        10 * time.Millisecond,
			WriterConcurrency:     3,              // 默认3个并发Writer
		},
		Schedule: SchedulePolicyConfig{
			CheckInterval:     1 * time.Minute,
			RetryInterval:     5 * time.Minute,
			MaxConcurrentTask: 2,
		},
		HTTP: HTTPPolicyConfig{
			Timeout:             30 * time.Second,
			IdleConnTimeout:     60 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
			MaxIdleConns:        100,
			MaxConnsPerHost:     10,
		},
		Filter: FilterPolicyConfig{
			ExcludeColumns: []string{},
			FixedValues:    map[string]interface{}{},
		},
		Retry: RetryPolicyConfig{
			MaxRetries:    3,
			InitialDelay:  2 * time.Second,
			MaxDelay:      60 * time.Second,
			BackoffFactor: 2.0,
		},
	}
}