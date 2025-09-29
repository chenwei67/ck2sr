package storage

import (
	"context"
)

// Storage 存储接口
type Storage interface {
	// SaveTaskState 保存任务状态
	SaveTaskState(ctx context.Context, state *TaskState) error

	// LoadTaskState 加载任务状态
	LoadTaskState(ctx context.Context, taskID string) (*TaskState, error)

	// SaveSyncProgress 保存同步进度
	SaveSyncProgress(ctx context.Context, progress *SyncProgress) error

	// LoadSyncProgress 加载同步进度
	LoadSyncProgress(ctx context.Context, taskID string, table string) (*SyncProgress, error)

	// ListTaskStates 列出所有任务状态
	ListTaskStates(ctx context.Context) ([]*TaskState, error)

	// DeleteTaskState 删除任务状态
	DeleteTaskState(ctx context.Context, taskID string) error

	// Close 关闭存储
	Close() error

	// SaveOffset 保存偏移量（兼容性方法）
	SaveOffset(taskID, tableName string, offset int64) error

	// LoadOffset 加载偏移量（兼容性方法）
	LoadOffset(taskID, tableName string) (int64, error)

	// SaveProgress 保存表同步进度（兼容性方法）
	SaveProgress(taskID, tableName string, progress *TableSyncProgress) error

	// LoadProgress 加载表同步进度（兼容性方法）
	LoadProgress(taskID, tableName string) (*TableSyncProgress, error)
}