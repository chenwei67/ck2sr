package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FileStorage 文件存储实现
type FileStorage struct {
	storagePath string
	mu          sync.RWMutex
}

// NewFileStorage 创建文件存储
func NewFileStorage(storagePath string) (*FileStorage, error) {
	// 确保存储目录存在
	if err := os.MkdirAll(storagePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	return &FileStorage{
		storagePath: storagePath,
	}, nil
}

// SaveTaskState 保存任务状态
func (f *FileStorage) SaveTaskState(ctx context.Context, state *TaskState) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	filePath := filepath.Join(f.storagePath, fmt.Sprintf("task_%s.json", state.TaskID))

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal task state: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write task state: %w", err)
	}

	return nil
}

// LoadTaskState 加载任务状态
func (f *FileStorage) LoadTaskState(ctx context.Context, taskID string) (*TaskState, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	filePath := filepath.Join(f.storagePath, fmt.Sprintf("task_%s.json", taskID))

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // 文件不存在返回nil
		}
		return nil, fmt.Errorf("failed to read task state: %w", err)
	}

	var state TaskState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal task state: %w", err)
	}

	return &state, nil
}

// SaveSyncProgress 保存同步进度
func (f *FileStorage) SaveSyncProgress(ctx context.Context, progress *SyncProgress) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	filePath := filepath.Join(f.storagePath, fmt.Sprintf("progress_%s_%s.json",
		progress.TaskID, progress.SourceTable))

	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal sync progress: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write sync progress: %w", err)
	}

	return nil
}

// LoadSyncProgress 加载同步进度
func (f *FileStorage) LoadSyncProgress(ctx context.Context, taskID string, table string) (*SyncProgress, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	filePath := filepath.Join(f.storagePath, fmt.Sprintf("progress_%s_%s.json", taskID, table))

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read sync progress: %w", err)
	}

	var progress SyncProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return nil, fmt.Errorf("failed to unmarshal sync progress: %w", err)
	}

	return &progress, nil
}

// ListTaskStates 列出所有任务状态
func (f *FileStorage) ListTaskStates(ctx context.Context) ([]*TaskState, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	pattern := filepath.Join(f.storagePath, "task_*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list task files: %w", err)
	}

	var states []*TaskState
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}

		var state TaskState
		if err := json.Unmarshal(data, &state); err != nil {
			continue
		}

		states = append(states, &state)
	}

	return states, nil
}

// DeleteTaskState 删除任务状态
func (f *FileStorage) DeleteTaskState(ctx context.Context, taskID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	filePath := filepath.Join(f.storagePath, fmt.Sprintf("task_%s.json", taskID))

	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete task state: %w", err)
	}

	return nil
}

// Close 关闭存储
func (f *FileStorage) Close() error {
	return nil
}

// SaveOffset 保存偏移量（兼容性方法）
func (f *FileStorage) SaveOffset(taskID, tableName string, offset int64) error {
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	offsetDir := filepath.Join(f.storagePath, "offsets")
	if err := os.MkdirAll(offsetDir, 0755); err != nil {
		return fmt.Errorf("failed to create offsets directory: %w", err)
	}

	filePath := filepath.Join(offsetDir, fmt.Sprintf("%s_%s.json", taskID, tableName))

	data := map[string]interface{}{
		"task_id":    taskID,
		"table_name": tableName,
		"offset":     offset,
	}

	jsonData, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal offset: %w", err)
	}

	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write offset: %w", err)
	}

	return nil
}

// LoadOffset 加载偏移量（兼容性方法）
func (f *FileStorage) LoadOffset(taskID, tableName string) (int64, error) {
	if taskID == "" {
		return 0, fmt.Errorf("task ID cannot be empty")
	}
	if tableName == "" {
		return 0, fmt.Errorf("table name cannot be empty")
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	filePath := filepath.Join(f.storagePath, "offsets", fmt.Sprintf("%s_%s.json", taskID, tableName))

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // 返回0作为默认偏移量
		}
		return 0, fmt.Errorf("failed to read offset: %w", err)
	}

	var offsetData map[string]interface{}
	if err := json.Unmarshal(data, &offsetData); err != nil {
		return 0, fmt.Errorf("failed to unmarshal offset: %w", err)
	}

	offset, ok := offsetData["offset"].(float64)
	if !ok {
		return 0, fmt.Errorf("invalid offset format")
	}

	return int64(offset), nil
}

// SaveProgress 保存表同步进度（兼容性方法）
func (f *FileStorage) SaveProgress(taskID, tableName string, progress *TableSyncProgress) error {
	if progress == nil {
		return fmt.Errorf("progress cannot be nil")
	}
	if taskID == "" {
		return fmt.Errorf("task ID cannot be empty")
	}
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	progressDir := filepath.Join(f.storagePath, "progress")
	if err := os.MkdirAll(progressDir, 0755); err != nil {
		return fmt.Errorf("failed to create progress directory: %w", err)
	}

	filePath := filepath.Join(progressDir, fmt.Sprintf("%s_%s.json", taskID, tableName))

	data, err := json.MarshalIndent(progress, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal progress: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write progress: %w", err)
	}

	return nil
}

// LoadProgress 加载表同步进度（兼容性方法）
func (f *FileStorage) LoadProgress(taskID, tableName string) (*TableSyncProgress, error) {
	if taskID == "" {
		return nil, fmt.Errorf("task ID cannot be empty")
	}
	if tableName == "" {
		return nil, fmt.Errorf("table name cannot be empty")
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	filePath := filepath.Join(f.storagePath, "progress", fmt.Sprintf("%s_%s.json", taskID, tableName))

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// 返回默认进度而不是错误
			return &TableSyncProgress{
				TaskID:        taskID,
				TableName:     tableName,
				Offset:        0,
				ProcessedRows: 0,
				Status:        "pending",
			}, nil
		}
		return nil, fmt.Errorf("failed to read progress: %w", err)
	}

	var progress TableSyncProgress
	if err := json.Unmarshal(data, &progress); err != nil {
		return nil, fmt.Errorf("failed to unmarshal progress: %w", err)
	}

	return &progress, nil
}