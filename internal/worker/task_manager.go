package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/ck2sr/ck2sr/internal/pipeline"
	"github.com/ck2sr/ck2sr/internal/storage"
	"github.com/ck2sr/ck2sr/pkg/clickhouse"
	"github.com/ck2sr/ck2sr/pkg/starrocks"
)

// TaskManagerImpl 任务管理器实现
type TaskManagerImpl struct {
	tasks      map[string]SyncTask
	tasksMutex sync.RWMutex
	logger     *logrus.Logger

	// 依赖组件
	chClient      *clickhouse.Client
	srClient      *starrocks.Client
	pipelineFactory *pipeline.ProcessorFactory
	storage       storage.Storage

	// 配置
	config *config.Config

	// 监控和清理
	stopChan   chan struct{}
	cleanupInterval time.Duration
}

// NewTaskManager 创建任务管理器
func NewTaskManager(
	config *config.Config,
	chClient *clickhouse.Client,
	srClient *starrocks.Client,
	storage storage.Storage,
	logger *logrus.Logger,
) *TaskManagerImpl {
	if logger == nil {
		logger = logrus.New()
	}

	tm := &TaskManagerImpl{
		tasks:           make(map[string]SyncTask),
		logger:          logger,
		chClient:        chClient,
		srClient:        srClient,
		pipelineFactory: pipeline.NewProcessorFactory(logger),
		storage:         storage,
		config:          config,
		stopChan:        make(chan struct{}),
		cleanupInterval: 5 * time.Minute,
	}

	// 启动后台监控
	go tm.monitorLoop()

	return tm
}

// CreateTask 创建同步任务
func (tm *TaskManagerImpl) CreateTask(taskConfig *config.SyncTaskConfig) (SyncTask, error) {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()

	if _, exists := tm.tasks[taskConfig.TaskID]; exists {
		return nil, fmt.Errorf("task %s already exists", taskConfig.TaskID)
	}

	// 创建数据处理管道
	syncPipeline := pipeline.NewSimplePipeline(tm.logger)

	// 配置处理器（如果有的话）
	// 这里可以根据任务配置创建列映射、数据过滤等处理器
	if len(taskConfig.ColumnMapping) > 0 {
		// 创建列映射处理器
		columnMappingConfig := &pipeline.ProcessorConfig{
			Name:    "column_mapping",
			Type:    "column_mapping",
			Enabled: true,
			Parameters: map[string]interface{}{
				"mapping": taskConfig.ColumnMapping,
			},
			Order: 0,
		}

		processor := pipeline.NewColumnMappingProcessor(tm.logger)
		if err := processor.Configure(columnMappingConfig); err != nil {
			return nil, fmt.Errorf("failed to configure column mapping processor: %w", err)
		}

		if err := syncPipeline.AddProcessor(processor); err != nil {
			return nil, fmt.Errorf("failed to add column mapping processor: %w", err)
		}
	}

	// 创建同步任务
	task := NewSyncTask(
		taskConfig,
		tm.chClient,
		tm.srClient,
		syncPipeline,
		tm.storage,
		tm.logger,
	)

	tm.tasks[taskConfig.TaskID] = task
	tm.logger.Infof("Created task: %s", taskConfig.TaskID)

	return task, nil
}

// GetTask 获取同步任务
func (tm *TaskManagerImpl) GetTask(taskID string) (SyncTask, error) {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	task, exists := tm.tasks[taskID]
	if !exists {
		return nil, fmt.Errorf("task not found: %s", taskID)
	}

	return task, nil
}

// GetAllTasks 获取所有任务
func (tm *TaskManagerImpl) GetAllTasks() []SyncTask {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	tasks := make([]SyncTask, 0, len(tm.tasks))
	for _, task := range tm.tasks {
		tasks = append(tasks, task)
	}

	return tasks
}

// RemoveTask 移除任务
func (tm *TaskManagerImpl) RemoveTask(taskID string) error {
	tm.tasksMutex.Lock()
	defer tm.tasksMutex.Unlock()

	task, exists := tm.tasks[taskID]
	if !exists {
		return fmt.Errorf("task not found: %s", taskID)
	}

	// 如果任务正在运行，先停止它
	if task.GetStatus() == storage.TaskStatusRunning || task.GetStatus() == storage.TaskStatusPaused {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := task.Stop(ctx); err != nil {
			tm.logger.Warnf("Failed to stop task %s: %v", taskID, err)
		}
	}

	delete(tm.tasks, taskID)
	tm.logger.Infof("Removed task: %s", taskID)

	return nil
}

// StartTask 启动任务
func (tm *TaskManagerImpl) StartTask(ctx context.Context, taskID string) error {
	task, err := tm.GetTask(taskID)
	if err != nil {
		return err
	}

	return task.Start(ctx)
}

// StopTask 停止任务
func (tm *TaskManagerImpl) StopTask(ctx context.Context, taskID string) error {
	task, err := tm.GetTask(taskID)
	if err != nil {
		return err
	}

	return task.Stop(ctx)
}

// PauseTask 暂停任务
func (tm *TaskManagerImpl) PauseTask(ctx context.Context, taskID string) error {
	task, err := tm.GetTask(taskID)
	if err != nil {
		return err
	}

	return task.Pause(ctx)
}

// ResumeTask 恢复任务
func (tm *TaskManagerImpl) ResumeTask(ctx context.Context, taskID string) error {
	task, err := tm.GetTask(taskID)
	if err != nil {
		return err
	}

	return task.Resume(ctx)
}

// GetProgress 获取任务进度
func (tm *TaskManagerImpl) GetProgress(taskID string) (*SyncProgress, error) {
	task, err := tm.GetTask(taskID)
	if err != nil {
		return nil, err
	}

	progress := task.GetProgress()
	return progress, nil
}

// GetAllProgress 获取所有任务进度
func (tm *TaskManagerImpl) GetAllProgress() map[string]*SyncProgress {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	progress := make(map[string]*SyncProgress)
	for taskID, task := range tm.tasks {
		progress[taskID] = task.GetProgress()
	}

	return progress
}

// UpdateProgress 更新所有任务进度
func (tm *TaskManagerImpl) UpdateProgress() error {
	tm.tasksMutex.RLock()
	tasks := make([]SyncTask, 0, len(tm.tasks))
	for _, task := range tm.tasks {
		tasks = append(tasks, task)
	}
	tm.tasksMutex.RUnlock()

	var lastErr error
	for _, task := range tasks {
		if err := task.UpdateProgress(); err != nil {
			tm.logger.Warnf("Failed to update progress for task %s: %v", task.GetID(), err)
			lastErr = err
		}
	}

	return lastErr
}

// Shutdown 关闭管理器
func (tm *TaskManagerImpl) Shutdown(ctx context.Context) error {
	tm.logger.Info("Shutting down task manager...")

	// 停止监控循环
	close(tm.stopChan)

	// 停止所有正在运行的任务
	tm.tasksMutex.RLock()
	tasks := make([]SyncTask, 0, len(tm.tasks))
	for _, task := range tm.tasks {
		tasks = append(tasks, task)
	}
	tm.tasksMutex.RUnlock()

	var wg sync.WaitGroup
	for _, task := range tasks {
		if task.GetStatus() == storage.TaskStatusRunning || task.GetStatus() == storage.TaskStatusPaused {
			wg.Add(1)
			go func(t SyncTask) {
				defer wg.Done()
				if err := t.Stop(ctx); err != nil {
					tm.logger.Warnf("Failed to stop task %s: %v", t.GetID(), err)
				}
			}(task)
		}
	}

	// 等待所有任务停止
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		tm.logger.Info("All tasks stopped")
	case <-ctx.Done():
		tm.logger.Warn("Shutdown timeout, some tasks may still be running")
	}

	tm.logger.Info("Task manager shutdown complete")
	return nil
}

// LoadTasksFromConfig 从配置加载任务
func (tm *TaskManagerImpl) LoadTasksFromConfig() error {
	for _, taskConfig := range tm.config.SyncTasks {
		if !taskConfig.Enabled {
			continue
		}

		// 合并全局配置
		mergedConfig := tm.config.MergeTaskConfig(&taskConfig)

		if _, err := tm.CreateTask(mergedConfig); err != nil {
			tm.logger.Errorf("Failed to create task %s: %v", taskConfig.TaskID, err)
			return err
		}
	}

	tm.logger.Infof("Loaded %d tasks from configuration", len(tm.tasks))
	return nil
}

// StartScheduledTasks 启动计划任务
func (tm *TaskManagerImpl) StartScheduledTasks(ctx context.Context) error {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	for _, task := range tm.tasks {
		config := task.GetConfig()

		// 检查是否是计划任务
		if config.CronExpression != "" {
			// TODO: 实现 cron 调度
			tm.logger.Infof("Cron scheduling not implemented for task %s", task.GetID())
			continue
		}

		// 检查时间窗口
		if config.TimeWindow != nil && !config.TimeWindow.IsInWindow(time.Now()) {
			tm.logger.Infof("Task %s is outside time window, skipping", task.GetID())
			continue
		}

		// 启动任务
		if err := task.Start(ctx); err != nil {
			tm.logger.Errorf("Failed to start task %s: %v", task.GetID(), err)
		}
	}

	return nil
}

// monitorLoop 监控循环
func (tm *TaskManagerImpl) monitorLoop() {
	ticker := time.NewTicker(tm.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tm.stopChan:
			return
		case <-ticker.C:
			tm.performMaintenance()
		}
	}
}

// performMaintenance 执行维护任务
func (tm *TaskManagerImpl) performMaintenance() {
	tm.logger.Debug("Performing maintenance tasks...")

	// 更新所有任务进度
	tm.UpdateProgress()

	// 检查完成的任务
	tm.tasksMutex.RLock()
	completedTasks := make([]string, 0)
	for taskID, task := range tm.tasks {
		status := task.GetStatus()
		if status == storage.TaskStatusCompleted || status == storage.TaskStatusFailed {
			completedTasks = append(completedTasks, taskID)
		}
	}
	tm.tasksMutex.RUnlock()

	// 清理完成的任务（可选，根据配置决定）
	for _, taskID := range completedTasks {
		task, _ := tm.GetTask(taskID)
		config := task.GetConfig()

		// 如果配置了验证，执行数据完整性验证
		if config.Validate.Enabled && task.GetStatus() == storage.TaskStatusCompleted {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			if err := task.Validate(ctx); err != nil {
				tm.logger.Errorf("Data validation failed for task %s: %v", taskID, err)
			} else {
				tm.logger.Infof("Data validation passed for task %s", taskID)
			}
			cancel()
		}
	}

	tm.logger.Debug("Maintenance tasks completed")
}

// GetStatistics 获取管理器统计信息
func (tm *TaskManagerImpl) GetStatistics() map[string]interface{} {
	tm.tasksMutex.RLock()
	defer tm.tasksMutex.RUnlock()

	stats := map[string]interface{}{
		"total_tasks":     len(tm.tasks),
		"running_tasks":   0,
		"paused_tasks":    0,
		"completed_tasks": 0,
		"failed_tasks":    0,
		"pending_tasks":   0,
	}

	for _, task := range tm.tasks {
		switch task.GetStatus() {
		case storage.TaskStatusRunning:
			stats["running_tasks"] = stats["running_tasks"].(int) + 1
		case storage.TaskStatusPaused:
			stats["paused_tasks"] = stats["paused_tasks"].(int) + 1
		case storage.TaskStatusCompleted:
			stats["completed_tasks"] = stats["completed_tasks"].(int) + 1
		case storage.TaskStatusFailed:
			stats["failed_tasks"] = stats["failed_tasks"].(int) + 1
		case storage.TaskStatusPending:
			stats["pending_tasks"] = stats["pending_tasks"].(int) + 1
		}
	}

	return stats
}

// HealthCheck 健康检查
func (tm *TaskManagerImpl) HealthCheck(ctx context.Context) error {
	// 检查依赖组件
	if err := tm.chClient.TestConnection(ctx); err != nil {
		return fmt.Errorf("clickhouse connection failed: %w", err)
	}

	if err := tm.srClient.TestConnection(ctx); err != nil {
		return fmt.Errorf("starrocks connection failed: %w", err)
	}

	if err := tm.storage.HealthCheck(ctx); err != nil {
		return fmt.Errorf("storage health check failed: %w", err)
	}

	return nil
}