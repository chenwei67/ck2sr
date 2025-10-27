package storage

import (
	"time"
)

// TaskStatus 任务状态
type TaskStatus string

const (
	TaskStatusIdle    TaskStatus = "idle"
	TaskStatusRunning TaskStatus = "running"
	TaskStatusSuccess TaskStatus = "success"
	TaskStatusFailed  TaskStatus = "failed"
	TaskStatusPaused  TaskStatus = "paused" // 时间窗口外暂停状态
)

// TaskState 任务状态
// 对应文件：task_${task_id}.json
type TaskState struct {
	TaskID        string     `json:"task_id"`              // 任务唯一标识符
	Status        TaskStatus `json:"status"`               // 任务执行状态（FSM状态）
	ScheduleTimes int64      `json:"schedule_times"`       // 调度执行计数器
	FailedTimes   int64      `json:"failed_times"`         // 失败计数器
	StartedAt     time.Time  `json:"started_at"`           // 首次调度时间戳
	LastRunTime   time.Time  `json:"last_run_time"`        // 最近调度时间戳
	UpdatedAt     time.Time  `json:"updated_at"`           // 状态更新时间戳
	FinishedAt    time.Time  `json:"finished_at"`          // 任务完成时间戳
	LastError     string     `json:"last_error,omitempty"` // 最后错误信息（可选）
}

// SyncProgress 同步进度
// 对应文件：progress_${task_id}_${table}.json
type SyncProgress struct {
	TaskID        string    `json:"task_id"`         // 任务ID
	Status        string    `json:"status"`          // 进度状态（running, completed, failed）
	SourceDB      string    `json:"source_db"`       // 源数据库名
	SourceTable   string    `json:"source_table"`    // 源表名
	TargetDB      string    `json:"target_db"`       // 目标数据库名
	TargetTable   string    `json:"target_table"`    // 目标表名
	TotalRows     uint64    `json:"total_rows"`      // 本次需要同步的总行数
	SyncedRows    uint64    `json:"synced_rows"`     // 已同步完成的行数（断点偏移量）
	SyncedBytes   int64     `json:"synced_bytes"`    // 已同步完成的字节数
	StartSyncTime time.Time `json:"start_sync_time"` // 本次同步开始时间
	EndSyncTime   time.Time `json:"end_sync_time"`   // 本次同步结束时间
	LastSyncTime  time.Time `json:"last_sync_time"`  // 最后一次更新时间
	Progress      float64   `json:"progress"`        // 百分比（0.00-100.00）
}

// TableSyncProgress 表同步进度（兼容性类型）
type TableSyncProgress struct {
	TaskID        string    `json:"task_id"`
	TableName     string    `json:"table_name"`
	Offset        int64     `json:"offset"`
	ProcessedRows uint64    `json:"processed_rows"`
	TotalRows     uint64    `json:"total_rows"`
	Status        string    `json:"status"`
	LastSyncTime  time.Time `json:"last_sync_time"`
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time"`
	Progress      float64   `json:"progress"`
	ErrorCount    int       `json:"error_count"`
	LastError     string    `json:"last_error"`
}
