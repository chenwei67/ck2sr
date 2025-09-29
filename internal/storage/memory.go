package storage

import (
	"context"
	"fmt"
	"sync"
)

// MemoryStorage 内存存储实现（用于测试）
type MemoryStorage struct {
	taskStates    map[string]*TaskState
	syncProgress  map[string]*SyncProgress
	tableProgress map[string]*TableSyncProgress // 为 sync 包的 TableSyncProgress 提供支持
	offsets       map[string]int64              // 偏移量存储
	mu            sync.RWMutex
}

// NewMemoryStorage 创建新的内存存储实例
func NewMemoryStorage() Storage {
	return &MemoryStorage{
		taskStates:    make(map[string]*TaskState),
		syncProgress:  make(map[string]*SyncProgress),
		tableProgress: make(map[string]*TableSyncProgress),
		offsets:       make(map[string]int64),
	}
}

// SaveTaskState 保存任务状态
func (m *MemoryStorage) SaveTaskState(ctx context.Context, state *TaskState) error {
	if state == nil {
		return fmt.Errorf("task state cannot be nil")
	}
	if state.TaskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 创建副本以避免并发修改
	stateCopy := *state
	m.taskStates[state.TaskID] = &stateCopy

	return nil
}

// LoadTaskState 加载任务状态
func (m *MemoryStorage) LoadTaskState(ctx context.Context, taskID string) (*TaskState, error) {
	if taskID == "" {
		return nil, fmt.Errorf("task ID cannot be empty")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	state, exists := m.taskStates[taskID]
	if !exists {
		return nil, fmt.Errorf("task state not found for task ID: %s", taskID)
	}

	// 返回副本以避免并发修改
	stateCopy := *state
	return &stateCopy, nil
}

// SaveSyncProgress 保存同步进度
func (m *MemoryStorage) SaveSyncProgress(ctx context.Context, progress *SyncProgress) error {
	if progress == nil {
		return fmt.Errorf("sync progress cannot be nil")
	}
	if progress.TaskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}
	if progress.SourceTable == "" {
		return fmt.Errorf("source table cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := progress.TaskID + ":" + progress.SourceTable
	progressCopy := *progress
	m.syncProgress[key] = &progressCopy

	return nil
}

// LoadSyncProgress 加载同步进度
func (m *MemoryStorage) LoadSyncProgress(ctx context.Context, taskID string, table string) (*SyncProgress, error) {
	if taskID == "" {
		return nil, fmt.Errorf("task ID cannot be empty")
	}
	if table == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	key := taskID + ":" + table
	progress, exists := m.syncProgress[key]
	if !exists {
		return nil, fmt.Errorf("sync progress not found for task ID: %s, table: %s", taskID, table)
	}

	progressCopy := *progress
	return &progressCopy, nil
}

// ListTaskStates 列出所有任务状态
func (m *MemoryStorage) ListTaskStates(ctx context.Context) ([]*TaskState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	states := make([]*TaskState, 0, len(m.taskStates))
	for _, state := range m.taskStates {
		stateCopy := *state
		states = append(states, &stateCopy)
	}

	return states, nil
}

// DeleteTaskState 删除任务状态
func (m *MemoryStorage) DeleteTaskState(ctx context.Context, taskID string) error {
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.taskStates[taskID]; !exists {
		return fmt.Errorf("task state not found for task ID: %s", taskID)
	}

	delete(m.taskStates, taskID)
	return nil
}

// SaveOffset 保存偏移量（为兼容性提供）
func (m *MemoryStorage) SaveOffset(taskID, tableName string, offset int64) error {
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := taskID + ":" + tableName
	m.offsets[key] = offset

	return nil
}

// LoadOffset 加载偏移量（为兼容性提供）
func (m *MemoryStorage) LoadOffset(taskID, tableName string) (int64, error) {
	if taskID == "" {
		return 0, fmt.Errorf("task ID cannot be empty")
	}
	if tableName == "" {
		return 0, fmt.Errorf("table name cannot be empty")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	key := taskID + ":" + tableName
	offset, exists := m.offsets[key]
	if !exists {
		return 0, nil // 返回0作为默认偏移量
	}

	return offset, nil
}

// SaveProgress 保存表同步进度（为兼容性提供）
func (m *MemoryStorage) SaveProgress(taskID, tableName string, progress *TableSyncProgress) error {
	if progress == nil {
		return fmt.Errorf("progress cannot be nil")
	}
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := taskID + ":" + tableName
	progressCopy := *progress
	m.tableProgress[key] = &progressCopy

	return nil
}

// LoadProgress 加载表同步进度（为兼容性提供）
func (m *MemoryStorage) LoadProgress(taskID, tableName string) (*TableSyncProgress, error) {
	if taskID == "" {
		return nil, fmt.Errorf("task ID cannot be empty")
	}
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	key := taskID + ":" + tableName
	progress, exists := m.tableProgress[key]
	if !exists {
		// 返回默认进度而不是错误
		return &TableSyncProgress{
			TaskID:        taskID,
			TableName:     tableName,
			Offset:        0,
			ProcessedRows: 0,
			Status:        "pending",
		}, nil
	}

	progressCopy := *progress
	return &progressCopy, nil
}

// Close 关闭存储
func (m *MemoryStorage) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 清理所有数据
	m.taskStates = make(map[string]*TaskState)
	m.syncProgress = make(map[string]*SyncProgress)
	m.tableProgress = make(map[string]*TableSyncProgress)
	m.offsets = make(map[string]int64)

	return nil
}