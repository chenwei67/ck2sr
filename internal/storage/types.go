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
)

// TaskState 任务状态
type TaskState struct {
	TaskID       string     `json:"task_id"`
	Status       TaskStatus `json:"status"`
	LastRunTime  time.Time  `json:"last_run_time"`
	TotalRuns    int64      `json:"total_runs"`
	SuccessRuns  int64      `json:"success_runs"`
	FailedRuns   int64      `json:"failed_runs"`
	LastError    string     `json:"last_error,omitempty"`
	TotalRows    int64      `json:"total_rows"`
	TotalBytes   int64      `json:"total_bytes"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

// SyncProgress 同步进度
type SyncProgress struct {
	TaskID        string    `json:"task_id"`
	SourceTable   string    `json:"source_table"`
	TargetTable   string    `json:"target_table"`
	TotalRows     int64     `json:"total_rows"`       // 本次需要同步的总行数
	SyncedRows    int64     `json:"synced_rows"`      // 已同步完成的行数
	SyncedBytes   int64     `json:"synced_bytes"`     // 已同步完成的字节数
	StartSyncTime time.Time `json:"start_sync_time"`  // 本次同步开始时间
	LastSyncTime  time.Time `json:"last_sync_time"`   // 最后一次更新时间
	Progress      float64   `json:"progress"`         // 百分比
}

// TableSyncProgress 表同步进度（兼容性类型）
type TableSyncProgress struct {
	TaskID        string    `json:"task_id"`
	TableName     string    `json:"table_name"`
	Offset        int64     `json:"offset"`
	ProcessedRows int64     `json:"processed_rows"`
	TotalRows     int64     `json:"total_rows"`
	Status        string    `json:"status"`
	LastSyncTime  time.Time `json:"last_sync_time"`
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time"`
	Progress      float64   `json:"progress"`
	ErrorCount    int       `json:"error_count"`
	LastError     string    `json:"last_error"`
}