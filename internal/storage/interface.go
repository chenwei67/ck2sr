package storage

import (
	"context"
	"time"
)

// TaskStatus 任务状态枚举
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"   // 等待执行
	TaskStatusRunning   TaskStatus = "running"   // 正在执行
	TaskStatusPaused    TaskStatus = "paused"    // 已暂停
	TaskStatusCompleted TaskStatus = "completed" // 已完成
	TaskStatusFailed    TaskStatus = "failed"    // 执行失败
	TaskStatusCancelled TaskStatus = "cancelled" // 已取消
)

// TaskState 任务状态信息
type TaskState struct {
	TaskID      string      `json:"task_id"`
	Status      TaskStatus  `json:"status"`
	StartTime   *time.Time  `json:"start_time,omitempty"`
	EndTime     *time.Time  `json:"end_time,omitempty"`
	UpdateTime  time.Time   `json:"update_time"`

	// 进度信息
	TotalRows      int64   `json:"total_rows"`       // 总行数
	ProcessedRows  int64   `json:"processed_rows"`   // 已处理行数
	TotalBytes     int64   `json:"total_bytes"`      // 总字节数
	ProcessedBytes int64   `json:"processed_bytes"`  // 已处理字节数

	// 断点续传信息
	LastOffset     int64            `json:"last_offset"`      // 最后处理的偏移量
	Checkpoint     map[string]interface{} `json:"checkpoint"`       // 检查点数据

	// 错误信息
	ErrorMessage   string   `json:"error_message,omitempty"`
	RetryCount     int      `json:"retry_count"`

	// 性能指标
	BytesPerSecond float64  `json:"bytes_per_second"` // 处理速度
	RowsPerSecond  float64  `json:"rows_per_second"`  // 行处理速度

	// 元数据
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
}

// Progress 进度信息
type Progress struct {
	Percentage      float64        `json:"percentage"`       // 完成百分比
	ElapsedTime     time.Duration  `json:"elapsed_time"`     // 已耗时
	EstimatedTime   time.Duration  `json:"estimated_time"`   // 预计剩余时间
	CurrentRate     float64        `json:"current_rate"`     // 当前处理速率
	AverageRate     float64        `json:"average_rate"`     // 平均处理速率
}

// GetProgress 计算任务进度
func (ts *TaskState) GetProgress() *Progress {
	progress := &Progress{}

	// 计算完成百分比
	if ts.TotalRows > 0 {
		progress.Percentage = float64(ts.ProcessedRows) / float64(ts.TotalRows) * 100
	} else if ts.TotalBytes > 0 {
		progress.Percentage = float64(ts.ProcessedBytes) / float64(ts.TotalBytes) * 100
	}

	// 计算已耗时
	if ts.StartTime != nil {
		if ts.EndTime != nil {
			progress.ElapsedTime = ts.EndTime.Sub(*ts.StartTime)
		} else {
			progress.ElapsedTime = time.Since(*ts.StartTime)
		}

		// 计算平均速率
		if progress.ElapsedTime.Seconds() > 0 {
			progress.AverageRate = float64(ts.ProcessedBytes) / progress.ElapsedTime.Seconds()
		}

		// 计算预计剩余时间
		if ts.ProcessedRows > 0 && ts.TotalRows > 0 && progress.AverageRate > 0 {
			remainingBytes := ts.TotalBytes - ts.ProcessedBytes
			if remainingBytes > 0 {
				progress.EstimatedTime = time.Duration(float64(remainingBytes)/progress.AverageRate) * time.Second
			}
		}
	}

	// 设置当前速率
	progress.CurrentRate = ts.BytesPerSecond

	return progress
}

// IsRunning 检查任务是否正在运行
func (ts *TaskState) IsRunning() bool {
	return ts.Status == TaskStatusRunning
}

// IsFinished 检查任务是否已完成（包括成功、失败、取消）
func (ts *TaskState) IsFinished() bool {
	return ts.Status == TaskStatusCompleted ||
		   ts.Status == TaskStatusFailed ||
		   ts.Status == TaskStatusCancelled
}

// CanResume 检查任务是否可以恢复
func (ts *TaskState) CanResume() bool {
	return ts.Status == TaskStatusPaused || ts.Status == TaskStatusFailed
}

// Storage 存储接口
type Storage interface {
	// 保存任务状态
	SaveTaskState(ctx context.Context, state *TaskState) error

	// 获取任务状态
	GetTaskState(ctx context.Context, taskID string) (*TaskState, error)

	// 获取所有任务状态
	GetAllTaskStates(ctx context.Context) ([]*TaskState, error)

	// 获取指定状态的任务
	GetTasksByStatus(ctx context.Context, status TaskStatus) ([]*TaskState, error)

	// 删除任务状态
	DeleteTaskState(ctx context.Context, taskID string) error

	// 清理过期任务状态
	CleanupExpiredTasks(ctx context.Context, expireTime time.Duration) error

	// 更新任务进度
	UpdateProgress(ctx context.Context, taskID string, processedRows, processedBytes int64) error

	// 保存检查点
	SaveCheckpoint(ctx context.Context, taskID string, checkpoint map[string]interface{}) error

	// 获取检查点
	GetCheckpoint(ctx context.Context, taskID string) (map[string]interface{}, error)

	// 初始化存储
	Initialize(ctx context.Context) error

	// 关闭存储
	Close() error
}

// StorageConfig 存储配置
type StorageConfig struct {
	Type       string                 `yaml:"type" json:"type"`             // 存储类型 (file, k8s, etcd)
	Path       string                 `yaml:"path" json:"path"`             // 存储路径
	Options    map[string]interface{} `yaml:"options" json:"options"`       // 额外选项
}

// TaskFilter 任务过滤器
type TaskFilter struct {
	TaskIDs    []string      `json:"task_ids,omitempty"`
	Statuses   []TaskStatus  `json:"statuses,omitempty"`
	StartTime  *time.Time    `json:"start_time,omitempty"`
	EndTime    *time.Time    `json:"end_time,omitempty"`
	Limit      int           `json:"limit,omitempty"`
	Offset     int           `json:"offset,omitempty"`
}

// StorageMetrics 存储指标
type StorageMetrics struct {
	TotalTasks      int64     `json:"total_tasks"`
	RunningTasks    int64     `json:"running_tasks"`
	CompletedTasks  int64     `json:"completed_tasks"`
	FailedTasks     int64     `json:"failed_tasks"`
	LastUpdateTime  time.Time `json:"last_update_time"`
}

// ExtendedStorage 扩展存储接口
type ExtendedStorage interface {
	Storage

	// 按条件查询任务
	QueryTasks(ctx context.Context, filter *TaskFilter) ([]*TaskState, error)

	// 批量操作
	BatchSaveTaskStates(ctx context.Context, states []*TaskState) error
	BatchDeleteTaskStates(ctx context.Context, taskIDs []string) error

	// 获取存储指标
	GetMetrics(ctx context.Context) (*StorageMetrics, error)

	// 备份和恢复
	Backup(ctx context.Context, backupPath string) error
	Restore(ctx context.Context, backupPath string) error

	// 健康检查
	HealthCheck(ctx context.Context) error
}