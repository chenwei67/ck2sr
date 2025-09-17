package scheduler

import (
	"context"
	"time"

	"github.com/ck2sr/ck2sr/internal/config"
)

// ScheduleType 调度类型
type ScheduleType string

const (
	ScheduleTypeImmediate ScheduleType = "immediate" // 立即执行
	ScheduleTypeCron      ScheduleType = "cron"      // Cron 表达式
	ScheduleTypeInterval  ScheduleType = "interval"  // 间隔执行
	ScheduleTypeOneTime   ScheduleType = "onetime"   // 一次性执行
)

// ScheduleStatus 调度状态
type ScheduleStatus string

const (
	ScheduleStatusActive    ScheduleStatus = "active"    // 活跃
	ScheduleStatusInactive  ScheduleStatus = "inactive"  // 非活跃
	ScheduleStatusCompleted ScheduleStatus = "completed" // 已完成
	ScheduleStatusFailed    ScheduleStatus = "failed"    // 失败
)

// Schedule 调度规则
type Schedule struct {
	ID          string        `json:"id"`           // 调度 ID
	TaskID      string        `json:"task_id"`      // 任务 ID
	Type        ScheduleType  `json:"type"`         // 调度类型
	Expression  string        `json:"expression"`   // Cron 表达式或间隔时间
	Status      ScheduleStatus `json:"status"`      // 调度状态
	NextRunTime *time.Time    `json:"next_run_time,omitempty"` // 下次运行时间
	LastRunTime *time.Time    `json:"last_run_time,omitempty"` // 上次运行时间
	RunCount    int64         `json:"run_count"`    // 运行次数
	MaxRuns     int64         `json:"max_runs"`     // 最大运行次数（0 表示无限制）
	TimeWindow  *config.TimeWindow `json:"time_window,omitempty"` // 时间窗口限制
	CreatedAt   time.Time     `json:"created_at"`   // 创建时间
	UpdatedAt   time.Time     `json:"updated_at"`   // 更新时间
}

// ScheduleEvent 调度事件
type ScheduleEvent struct {
	Type      string    `json:"type"`        // 事件类型
	ScheduleID string   `json:"schedule_id"` // 调度 ID
	TaskID    string    `json:"task_id"`     // 任务 ID
	Timestamp time.Time `json:"timestamp"`   // 时间戳
	Message   string    `json:"message"`     // 消息
	Error     string    `json:"error,omitempty"` // 错误信息
}

// SchedulerConfig 调度器配置
type SchedulerConfig struct {
	CheckInterval     time.Duration `yaml:"check_interval" json:"check_interval"`         // 检查间隔
	MaxConcurrentTasks int          `yaml:"max_concurrent_tasks" json:"max_concurrent_tasks"` // 最大并发任务数
	EventBufferSize   int           `yaml:"event_buffer_size" json:"event_buffer_size"`   // 事件缓冲区大小
	RetryAttempts     int           `yaml:"retry_attempts" json:"retry_attempts"`         // 重试次数
	RetryInterval     time.Duration `yaml:"retry_interval" json:"retry_interval"`         // 重试间隔
}

// Scheduler 调度器接口
type Scheduler interface {
	// Start 启动调度器
	Start(ctx context.Context) error

	// Stop 停止调度器
	Stop(ctx context.Context) error

	// AddSchedule 添加调度规则
	AddSchedule(schedule *Schedule) error

	// RemoveSchedule 移除调度规则
	RemoveSchedule(scheduleID string) error

	// UpdateSchedule 更新调度规则
	UpdateSchedule(schedule *Schedule) error

	// GetSchedule 获取调度规则
	GetSchedule(scheduleID string) (*Schedule, error)

	// GetAllSchedules 获取所有调度规则
	GetAllSchedules() ([]*Schedule, error)

	// GetSchedulesByTask 根据任务 ID 获取调度规则
	GetSchedulesByTask(taskID string) ([]*Schedule, error)

	// EnableSchedule 启用调度规则
	EnableSchedule(scheduleID string) error

	// DisableSchedule 禁用调度规则
	DisableSchedule(scheduleID string) error

	// TriggerSchedule 手动触发调度
	TriggerSchedule(scheduleID string) error

	// GetEvents 获取调度事件
	GetEvents(limit int) ([]*ScheduleEvent, error)

	// GetStatistics 获取调度器统计信息
	GetStatistics() map[string]interface{}

	// IsRunning 检查调度器是否正在运行
	IsRunning() bool
}

// ScheduleParser 调度表达式解析器接口
type ScheduleParser interface {
	// Parse 解析调度表达式
	Parse(expression string, scheduleType ScheduleType) (ScheduleExpression, error)

	// Validate 验证调度表达式
	Validate(expression string, scheduleType ScheduleType) error
}

// ScheduleExpression 调度表达式接口
type ScheduleExpression interface {
	// Next 计算下次执行时间
	Next(from time.Time) *time.Time

	// IsValid 检查表达式是否有效
	IsValid() bool

	// String 返回表达式字符串
	String() string
}

// TaskExecutor 任务执行器接口
type TaskExecutor interface {
	// Execute 执行任务
	Execute(ctx context.Context, taskID string) error

	// CanExecute 检查是否可以执行任务
	CanExecute(taskID string) bool

	// GetRunningCount 获取正在运行的任务数量
	GetRunningCount() int

	// GetMaxConcurrent 获取最大并发数
	GetMaxConcurrent() int
}

// EventListener 事件监听器接口
type EventListener interface {
	// OnScheduleTriggered 当调度被触发时
	OnScheduleTriggered(ctx context.Context, schedule *Schedule) error

	// OnTaskStarted 当任务开始时
	OnTaskStarted(ctx context.Context, taskID string) error

	// OnTaskCompleted 当任务完成时
	OnTaskCompleted(ctx context.Context, taskID string, duration time.Duration) error

	// OnTaskFailed 当任务失败时
	OnTaskFailed(ctx context.Context, taskID string, err error) error

	// OnScheduleAdded 当添加调度规则时
	OnScheduleAdded(ctx context.Context, schedule *Schedule) error

	// OnScheduleRemoved 当移除调度规则时
	OnScheduleRemoved(ctx context.Context, scheduleID string) error
}

// SchedulePersistence 调度持久化接口
type SchedulePersistence interface {
	// SaveSchedule 保存调度规则
	SaveSchedule(ctx context.Context, schedule *Schedule) error

	// LoadSchedule 加载调度规则
	LoadSchedule(ctx context.Context, scheduleID string) (*Schedule, error)

	// LoadAllSchedules 加载所有调度规则
	LoadAllSchedules(ctx context.Context) ([]*Schedule, error)

	// DeleteSchedule 删除调度规则
	DeleteSchedule(ctx context.Context, scheduleID string) error

	// SaveEvent 保存调度事件
	SaveEvent(ctx context.Context, event *ScheduleEvent) error

	// LoadEvents 加载调度事件
	LoadEvents(ctx context.Context, limit int) ([]*ScheduleEvent, error)

	// Initialize 初始化持久化存储
	Initialize(ctx context.Context) error

	// Close 关闭持久化存储
	Close() error
}