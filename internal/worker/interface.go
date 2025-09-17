package worker

import (
	"context"
	"time"

	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/ck2sr/ck2sr/internal/pipeline"
	"github.com/ck2sr/ck2sr/internal/storage"
)

// WorkerStatus 工作单元状态
type WorkerStatus string

const (
	WorkerStatusIdle    WorkerStatus = "idle"    // 空闲
	WorkerStatusRunning WorkerStatus = "running" // 运行中
	WorkerStatusPaused  WorkerStatus = "paused"  // 已暂停
	WorkerStatusStopped WorkerStatus = "stopped" // 已停止
	WorkerStatusError   WorkerStatus = "error"   // 错误状态
)

// WorkerStats 工作单元统计信息
type WorkerStats struct {
	WorkerID        string        `json:"worker_id"`
	TaskID          string        `json:"task_id"`
	Status          WorkerStatus  `json:"status"`
	StartTime       *time.Time    `json:"start_time,omitempty"`
	EndTime         *time.Time    `json:"end_time,omitempty"`
	LastUpdateTime  time.Time     `json:"last_update_time"`
	ProcessedRows   int64         `json:"processed_rows"`
	ProcessedBytes  int64         `json:"processed_bytes"`
	ErrorCount      int64         `json:"error_count"`
	CurrentBatch    int64         `json:"current_batch"`
	TotalBatches    int64         `json:"total_batches"`
	BytesPerSecond  float64       `json:"bytes_per_second"`
	RowsPerSecond   float64       `json:"rows_per_second"`
	ErrorMessage    string        `json:"error_message,omitempty"`
}

// SyncProgress 同步进度信息
type SyncProgress struct {
	TaskID          string        `json:"task_id"`
	TotalRows       int64         `json:"total_rows"`
	ProcessedRows   int64         `json:"processed_rows"`
	TotalBytes      int64         `json:"total_bytes"`
	ProcessedBytes  int64         `json:"processed_bytes"`
	Percentage      float64       `json:"percentage"`
	ElapsedTime     time.Duration `json:"elapsed_time"`
	EstimatedTime   time.Duration `json:"estimated_time"`
	CurrentRate     float64       `json:"current_rate"`
	AverageRate     float64       `json:"average_rate"`
	ActiveWorkers   int           `json:"active_workers"`
	ErrorCount      int64         `json:"error_count"`
}

// Worker 数据同步工作单元接口
type Worker interface {
	// GetID 获取工作单元 ID
	GetID() string

	// GetTaskID 获取任务 ID
	GetTaskID() string

	// GetStatus 获取工作单元状态
	GetStatus() WorkerStatus

	// Start 启动工作单元
	Start(ctx context.Context) error

	// Stop 停止工作单元
	Stop(ctx context.Context) error

	// Pause 暂停工作单元
	Pause(ctx context.Context) error

	// Resume 恢复工作单元
	Resume(ctx context.Context) error

	// GetStats 获取统计信息
	GetStats() *WorkerStats

	// GetProgress 获取进度信息
	GetProgress() *SyncProgress

	// IsRunning 检查是否正在运行
	IsRunning() bool

	// CanResume 检查是否可以恢复
	CanResume() bool
}

// SyncTask 同步任务接口
type SyncTask interface {
	// GetID 获取任务 ID
	GetID() string

	// GetConfig 获取任务配置
	GetConfig() *config.SyncTaskConfig

	// AddWorker 添加工作单元
	AddWorker(worker Worker) error

	// RemoveWorker 移除工作单元
	RemoveWorker(workerID string) error

	// GetWorkers 获取所有工作单元
	GetWorkers() []Worker

	// Start 启动任务
	Start(ctx context.Context) error

	// Stop 停止任务
	Stop(ctx context.Context) error

	// Pause 暂停任务
	Pause(ctx context.Context) error

	// Resume 恢复任务
	Resume(ctx context.Context) error

	// GetProgress 获取任务进度
	GetProgress() *SyncProgress

	// GetStatus 获取任务状态
	GetStatus() storage.TaskStatus

	// UpdateProgress 更新进度
	UpdateProgress() error

	// Validate 验证数据完整性
	Validate(ctx context.Context) error
}

// WorkerManager 工作单元管理器接口
type WorkerManager interface {
	// CreateWorker 创建工作单元
	CreateWorker(taskConfig *config.SyncTaskConfig) (Worker, error)

	// GetWorker 获取工作单元
	GetWorker(workerID string) (Worker, error)

	// GetWorkersByTask 根据任务 ID 获取工作单元
	GetWorkersByTask(taskID string) []Worker

	// GetAllWorkers 获取所有工作单元
	GetAllWorkers() []Worker

	// RemoveWorker 移除工作单元
	RemoveWorker(workerID string) error

	// GetStats 获取管理器统计信息
	GetStats() map[string]*WorkerStats

	// Shutdown 关闭管理器
	Shutdown(ctx context.Context) error
}

// TaskManager 任务管理器接口
type TaskManager interface {
	// CreateTask 创建同步任务
	CreateTask(taskConfig *config.SyncTaskConfig) (SyncTask, error)

	// GetTask 获取同步任务
	GetTask(taskID string) (SyncTask, error)

	// GetAllTasks 获取所有任务
	GetAllTasks() []SyncTask

	// RemoveTask 移除任务
	RemoveTask(taskID string) error

	// StartTask 启动任务
	StartTask(ctx context.Context, taskID string) error

	// StopTask 停止任务
	StopTask(ctx context.Context, taskID string) error

	// PauseTask 暂停任务
	PauseTask(ctx context.Context, taskID string) error

	// ResumeTask 恢复任务
	ResumeTask(ctx context.Context, taskID string) error

	// GetProgress 获取任务进度
	GetProgress(taskID string) (*SyncProgress, error)

	// GetAllProgress 获取所有任务进度
	GetAllProgress() map[string]*SyncProgress

	// UpdateProgress 更新所有任务进度
	UpdateProgress() error

	// Shutdown 关闭管理器
	Shutdown(ctx context.Context) error
}

// WorkerFactory 工作单元工厂接口
type WorkerFactory interface {
	// CreateWorker 创建工作单元
	CreateWorker(taskConfig *config.SyncTaskConfig) (Worker, error)

	// GetSupportedTypes 获取支持的工作单元类型
	GetSupportedTypes() []string
}

// Checkpoint 检查点信息
type Checkpoint struct {
	TaskID     string    `json:"task_id"`
	WorkerID   string    `json:"worker_id"`
	Offset     int64     `json:"offset"`
	BatchID    int64     `json:"batch_id"`
	Timestamp  time.Time `json:"timestamp"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

// ValidateResult 数据完整性验证结果
type ValidateResult struct {
	TaskID          string    `json:"task_id"`
	Valid           bool      `json:"valid"`
	SourceRows      int64     `json:"source_rows"`
	TargetRows      int64     `json:"target_rows"`
	SourceChecksum  string    `json:"source_checksum"`
	TargetChecksum  string    `json:"target_checksum"`
	MissingRows     int64     `json:"missing_rows"`
	ExtraRows       int64     `json:"extra_rows"`
	ValidationTime  time.Time `json:"validation_time"`
	ErrorMessage    string    `json:"error_message,omitempty"`
}