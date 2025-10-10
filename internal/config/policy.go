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
	ProgressReportIntervalSec int           `yaml:"progress_report_interval_sec"` // 进度日志输出间隔（秒）
	RateLimitSleep            time.Duration `yaml:"rate_limit_sleep"`             // 批次间限速休眠时间
	WriterConcurrency         int           `yaml:"writer_concurrency"`           // Writer并发数量
}

// SchedulePolicyConfig 调度策略配置
type SchedulePolicyConfig struct {
	CheckInterval     time.Duration `yaml:"check_interval"`      // 定时任务扫描间隔
	RetryInterval     time.Duration `yaml:"retry_interval"`      // 失败重试等待时间
	RetryTimes        int           `yaml:"retry_times"`         // 最大重试次数：0=不重试，-1=无限重试，N=最多重试N次
	MaxConcurrentTask int           `yaml:"max_concurrent_task"` // 最大并发任务数
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

// RetryPolicyConfig 重试策略配置（统一Reader、Writer、Scheduler的重试策略）
type RetryPolicyConfig struct {
	MaxAttempts        int           `yaml:"max_attempts"`        // 最大重试次数
	InitialBackoff     time.Duration `yaml:"initial_backoff"`     // 初始退避时间
	MaxBackoff         time.Duration `yaml:"max_backoff"`         // 最大退避时间
	BackoffMultiplier  float64       `yaml:"backoff_multiplier"`  // 退避倍数（指数退避）
	Jitter             bool          `yaml:"jitter"`              // 随机抖动，避免惊群效应
}

// DefaultPolicyConfig 默认策略配置
func DefaultPolicyConfig() *PolicyConfig {
	return &PolicyConfig{
		Transfer: TransferPolicyConfig{
			ProgressReportIntervalSec: 10,                      // 每10秒输出一次进度
			RateLimitSleep:            1 * time.Millisecond,    // 1ms限速
			WriterConcurrency:         1,                       // 默认1个并发Writer
		},
		Schedule: SchedulePolicyConfig{
			CheckInterval:     10 * time.Second,                // 每10秒检查一次
			RetryInterval:     10 * time.Second,                // 失败后10秒重试
			RetryTimes:        3,                               // 最多重试3次
			MaxConcurrentTask: 1,                               // 默认1个并发任务
		},
		HTTP: HTTPPolicyConfig{
			Timeout:             30 * time.Second,
			IdleConnTimeout:     60 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
			MaxIdleConns:        100,
			MaxConnsPerHost:     100,
		},
		Filter: FilterPolicyConfig{
			ExcludeColumns: []string{},
			FixedValues:    map[string]interface{}{},
		},
		Retry: RetryPolicyConfig{
			MaxAttempts:       3,                               // 最多重试3次
			InitialBackoff:    1 * time.Second,                 // 初始1秒退避
			MaxBackoff:        60 * time.Second,                // 最大60秒退避
			BackoffMultiplier: 2.0,                             // 指数退避倍数2.0
			Jitter:            true,                            // 启用随机抖动
		},
	}
}