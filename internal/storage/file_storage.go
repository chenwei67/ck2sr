package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// FileStorage 文件系统存储实现
type FileStorage struct {
	basePath string
	logger   *logrus.Logger
	mutex    sync.RWMutex
}

// NewFileStorage 创建文件存储实例
func NewFileStorage(basePath string, logger *logrus.Logger) (*FileStorage, error) {
	if logger == nil {
		logger = logrus.New()
	}

	// 确保存储目录存在
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	storage := &FileStorage{
		basePath: basePath,
		logger:   logger,
	}

	return storage, nil
}

// Initialize 初始化存储
func (fs *FileStorage) Initialize(ctx context.Context) error {
	// 创建任务状态目录
	taskDir := filepath.Join(fs.basePath, "tasks")
	if err := os.MkdirAll(taskDir, 0755); err != nil {
		return fmt.Errorf("failed to create tasks directory: %w", err)
	}

	// 创建检查点目录
	checkpointDir := filepath.Join(fs.basePath, "checkpoints")
	if err := os.MkdirAll(checkpointDir, 0755); err != nil {
		return fmt.Errorf("failed to create checkpoints directory: %w", err)
	}

	fs.logger.Infof("FileStorage initialized at: %s", fs.basePath)
	return nil
}

// Close 关闭存储
func (fs *FileStorage) Close() error {
	// 文件存储无需特殊关闭操作
	return nil
}

// HealthCheck 健康检查
func (fs *FileStorage) HealthCheck(ctx context.Context) error {
	// 检查基础路径是否可访问
	if _, err := os.Stat(fs.basePath); os.IsNotExist(err) {
		return fmt.Errorf("storage base path does not exist: %s", fs.basePath)
	}

	// 检查是否可写
	testFile := filepath.Join(fs.basePath, ".health_check")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return fmt.Errorf("storage path is not writable: %w", err)
	}

	// 清理测试文件
	os.Remove(testFile)
	return nil
}

// SaveTaskState 保存任务状态
func (fs *FileStorage) SaveTaskState(ctx context.Context, state *TaskState) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	state.UpdateTime = time.Now()

	filePath := fs.getTaskFilePath(state.TaskID)
	if err := fs.saveJSON(filePath, state); err != nil {
		return fmt.Errorf("failed to save task state: %w", err)
	}

	fs.logger.Debugf("Saved task state for task %s", state.TaskID)
	return nil
}

// GetTaskState 获取任务状态
func (fs *FileStorage) GetTaskState(ctx context.Context, taskID string) (*TaskState, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	filePath := fs.getTaskFilePath(taskID)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("task state not found: %s", taskID)
	}

	var state TaskState
	if err := fs.loadJSON(filePath, &state); err != nil {
		return nil, fmt.Errorf("failed to load task state: %w", err)
	}

	return &state, nil
}

// GetAllTaskStates 获取所有任务状态
func (fs *FileStorage) GetAllTaskStates(ctx context.Context) ([]*TaskState, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	taskDir := filepath.Join(fs.basePath, "tasks")
	var states []*TaskState

	err := filepath.WalkDir(taskDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}

		var state TaskState
		if err := fs.loadJSON(path, &state); err != nil {
			fs.logger.Warnf("Failed to load task state from %s: %v", path, err)
			return nil // 跳过损坏的文件
		}

		states = append(states, &state)
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk task directory: %w", err)
	}

	return states, nil
}

// GetTasksByStatus 获取指定状态的任务
func (fs *FileStorage) GetTasksByStatus(ctx context.Context, status TaskStatus) ([]*TaskState, error) {
	allStates, err := fs.GetAllTaskStates(ctx)
	if err != nil {
		return nil, err
	}

	var filteredStates []*TaskState
	for _, state := range allStates {
		if state.Status == status {
			filteredStates = append(filteredStates, state)
		}
	}

	return filteredStates, nil
}

// DeleteTaskState 删除任务状态
func (fs *FileStorage) DeleteTaskState(ctx context.Context, taskID string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	// 删除任务状态文件
	taskFilePath := fs.getTaskFilePath(taskID)
	if err := os.Remove(taskFilePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete task state: %w", err)
	}

	// 删除检查点文件
	checkpointFilePath := fs.getCheckpointFilePath(taskID)
	if err := os.Remove(checkpointFilePath); err != nil && !os.IsNotExist(err) {
		fs.logger.Warnf("Failed to delete checkpoint file: %v", err)
	}

	fs.logger.Debugf("Deleted task state for task %s", taskID)
	return nil
}

// CleanupExpiredTasks 清理过期任务状态
func (fs *FileStorage) CleanupExpiredTasks(ctx context.Context, expireTime time.Duration) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	cutoffTime := time.Now().Add(-expireTime)
	taskDir := filepath.Join(fs.basePath, "tasks")

	var deletedCount int
	err := filepath.WalkDir(taskDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}

		// 检查文件修改时间
		info, err := d.Info()
		if err != nil {
			fs.logger.Warnf("Failed to get file info for %s: %v", path, err)
			return nil
		}

		if info.ModTime().Before(cutoffTime) {
			// 加载任务状态检查是否已完成
			var state TaskState
			if err := fs.loadJSON(path, &state); err != nil {
				fs.logger.Warnf("Failed to load task state from %s: %v", path, err)
				return nil
			}

			// 只删除已完成的任务
			if state.IsFinished() {
				if err := os.Remove(path); err != nil {
					fs.logger.Warnf("Failed to delete expired task file %s: %v", path, err)
				} else {
					deletedCount++
					fs.logger.Debugf("Deleted expired task: %s", state.TaskID)

					// 删除对应的检查点文件
					checkpointPath := fs.getCheckpointFilePath(state.TaskID)
					os.Remove(checkpointPath) // 忽略错误
				}
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to cleanup expired tasks: %w", err)
	}

	if deletedCount > 0 {
		fs.logger.Infof("Cleaned up %d expired tasks", deletedCount)
	}

	return nil
}

// UpdateProgress 更新任务进度
func (fs *FileStorage) UpdateProgress(ctx context.Context, taskID string, processedRows, processedBytes int64) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	state, err := fs.getTaskStateUnlocked(taskID)
	if err != nil {
		return err
	}

	state.ProcessedRows = processedRows
	state.ProcessedBytes = processedBytes
	state.UpdateTime = time.Now()

	// 计算处理速度
	if state.StartTime != nil {
		elapsed := time.Since(*state.StartTime).Seconds()
		if elapsed > 0 {
			state.BytesPerSecond = float64(processedBytes) / elapsed
			state.RowsPerSecond = float64(processedRows) / elapsed
		}
	}

	filePath := fs.getTaskFilePath(taskID)
	return fs.saveJSON(filePath, state)
}

// SaveCheckpoint 保存检查点
func (fs *FileStorage) SaveCheckpoint(ctx context.Context, taskID string, checkpoint map[string]interface{}) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	filePath := fs.getCheckpointFilePath(taskID)
	if err := fs.saveJSON(filePath, checkpoint); err != nil {
		return fmt.Errorf("failed to save checkpoint: %w", err)
	}

	// 同时更新任务状态中的检查点
	if state, err := fs.getTaskStateUnlocked(taskID); err == nil {
		state.Checkpoint = checkpoint
		state.UpdateTime = time.Now()
		taskFilePath := fs.getTaskFilePath(taskID)
		fs.saveJSON(taskFilePath, state)
	}

	return nil
}

// GetCheckpoint 获取检查点
func (fs *FileStorage) GetCheckpoint(ctx context.Context, taskID string) (map[string]interface{}, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	filePath := fs.getCheckpointFilePath(taskID)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, nil // 检查点不存在
	}

	var checkpoint map[string]interface{}
	if err := fs.loadJSON(filePath, &checkpoint); err != nil {
		return nil, fmt.Errorf("failed to load checkpoint: %w", err)
	}

	return checkpoint, nil
}

// getTaskStateUnlocked 获取任务状态（无锁版本）
func (fs *FileStorage) getTaskStateUnlocked(taskID string) (*TaskState, error) {
	filePath := fs.getTaskFilePath(taskID)
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("task state not found: %s", taskID)
	}

	var state TaskState
	if err := fs.loadJSON(filePath, &state); err != nil {
		return nil, fmt.Errorf("failed to load task state: %w", err)
	}

	return &state, nil
}

// getTaskFilePath 获取任务文件路径
func (fs *FileStorage) getTaskFilePath(taskID string) string {
	return filepath.Join(fs.basePath, "tasks", fmt.Sprintf("%s.json", taskID))
}

// getCheckpointFilePath 获取检查点文件路径
func (fs *FileStorage) getCheckpointFilePath(taskID string) string {
	return filepath.Join(fs.basePath, "checkpoints", fmt.Sprintf("%s.json", taskID))
}

// saveJSON 保存 JSON 文件
func (fs *FileStorage) saveJSON(filePath string, data interface{}) error {
	// 确保目录存在
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// 写入临时文件
	tmpPath := filePath + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(data)
	file.Close()

	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	// 原子性地移动文件
	if err := os.Rename(tmpPath, filePath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to move temp file: %w", err)
	}

	return nil
}

// loadJSON 加载 JSON 文件
func (fs *FileStorage) loadJSON(filePath string, data interface{}) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	if err := decoder.Decode(data); err != nil {
		return fmt.Errorf("failed to decode JSON: %w", err)
	}

	return nil
}