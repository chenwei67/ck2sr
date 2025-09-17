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

// SyncTaskImpl 同步任务实现
type SyncTaskImpl struct {
	id         string
	config     *config.SyncTaskConfig
	workers    map[string]Worker
	workersMux sync.RWMutex
	status     storage.TaskStatus
	statusMux  sync.RWMutex
	storage    storage.Storage
	logger     *logrus.Logger

	// 依赖组件
	chClient *clickhouse.Client
	srClient *starrocks.Client
	pipeline pipeline.Pipeline

	// 统计信息
	startTime  *time.Time
	endTime    *time.Time
	progress   *SyncProgress
}

// NewSyncTask 创建新的同步任务
func NewSyncTask(
	taskConfig *config.SyncTaskConfig,
	chClient *clickhouse.Client,
	srClient *starrocks.Client,
	pipeline pipeline.Pipeline,
	storageInstance storage.Storage,
	logger *logrus.Logger,
) *SyncTaskImpl {
	if logger == nil {
		logger = logrus.New()
	}

	return &SyncTaskImpl{
		id:       taskConfig.TaskID,
		config:   taskConfig,
		workers:  make(map[string]Worker),
		status:   storage.TaskStatusPending,
		storage:  storageInstance,
		logger:   logger,
		chClient: chClient,
		srClient: srClient,
		pipeline: pipeline,
		progress: &SyncProgress{
			TaskID: taskConfig.TaskID,
		},
	}
}

// GetID 获取任务 ID
func (t *SyncTaskImpl) GetID() string {
	return t.id
}

// GetConfig 获取任务配置
func (t *SyncTaskImpl) GetConfig() *config.SyncTaskConfig {
	return t.config
}

// AddWorker 添加工作单元
func (t *SyncTaskImpl) AddWorker(worker Worker) error {
	t.workersMux.Lock()
	defer t.workersMux.Unlock()

	if _, exists := t.workers[worker.GetID()]; exists {
		return fmt.Errorf("worker %s already exists", worker.GetID())
	}

	if worker.GetTaskID() != t.id {
		return fmt.Errorf("worker task ID mismatch: expected %s, got %s", t.id, worker.GetTaskID())
	}

	t.workers[worker.GetID()] = worker
	t.logger.Infof("Added worker %s to task %s", worker.GetID(), t.id)
	return nil
}

// RemoveWorker 移除工作单元
func (t *SyncTaskImpl) RemoveWorker(workerID string) error {
	t.workersMux.Lock()
	defer t.workersMux.Unlock()

	worker, exists := t.workers[workerID]
	if !exists {
		return fmt.Errorf("worker %s not found", workerID)
	}

	// 如果工作单元正在运行，先停止它
	if worker.IsRunning() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := worker.Stop(ctx); err != nil {
			t.logger.Warnf("Failed to stop worker %s: %v", workerID, err)
		}
	}

	delete(t.workers, workerID)
	t.logger.Infof("Removed worker %s from task %s", workerID, t.id)
	return nil
}

// GetWorkers 获取所有工作单元
func (t *SyncTaskImpl) GetWorkers() []Worker {
	t.workersMux.RLock()
	defer t.workersMux.RUnlock()

	workers := make([]Worker, 0, len(t.workers))
	for _, worker := range t.workers {
		workers = append(workers, worker)
	}
	return workers
}

// Start 启动任务
func (t *SyncTaskImpl) Start(ctx context.Context) error {
	t.statusMux.Lock()
	defer t.statusMux.Unlock()

	if t.status == storage.TaskStatusRunning {
		return fmt.Errorf("task %s is already running", t.id)
	}

	// 检查时间窗口
	if t.config.TimeWindow != nil && !t.config.TimeWindow.IsInWindow(time.Now()) {
		return fmt.Errorf("task %s is outside time window", t.id)
	}

	// 如果没有工作单元，创建默认工作单元
	if len(t.workers) == 0 {
		if err := t.createDefaultWorkers(); err != nil {
			return fmt.Errorf("failed to create default workers: %w", err)
		}
	}

	// 启动所有工作单元
	var startErr error
	for _, worker := range t.workers {
		if err := worker.Start(ctx); err != nil {
			t.logger.Errorf("Failed to start worker %s: %v", worker.GetID(), err)
			startErr = err
			break
		}
	}

	if startErr != nil {
		// 如果启动失败，停止已启动的工作单元
		t.stopAllWorkers(ctx)
		return startErr
	}

	t.status = storage.TaskStatusRunning
	now := time.Now()
	t.startTime = &now

	// 保存任务状态
	t.saveTaskState()

	t.logger.Infof("Task %s started with %d workers", t.id, len(t.workers))
	return nil
}

// Stop 停止任务
func (t *SyncTaskImpl) Stop(ctx context.Context) error {
	t.statusMux.Lock()
	defer t.statusMux.Unlock()

	if t.status != storage.TaskStatusRunning && t.status != storage.TaskStatusPaused {
		return nil
	}

	t.stopAllWorkers(ctx)

	t.status = storage.TaskStatusCompleted
	now := time.Now()
	t.endTime = &now

	// 保存任务状态
	t.saveTaskState()

	t.logger.Infof("Task %s stopped", t.id)
	return nil
}

// Pause 暂停任务
func (t *SyncTaskImpl) Pause(ctx context.Context) error {
	t.statusMux.Lock()
	defer t.statusMux.Unlock()

	if t.status != storage.TaskStatusRunning {
		return fmt.Errorf("task %s is not running", t.id)
	}

	// 暂停所有工作单元
	for _, worker := range t.workers {
		if err := worker.Pause(ctx); err != nil {
			t.logger.Warnf("Failed to pause worker %s: %v", worker.GetID(), err)
		}
	}

	t.status = storage.TaskStatusPaused

	// 保存任务状态
	t.saveTaskState()

	t.logger.Infof("Task %s paused", t.id)
	return nil
}

// Resume 恢复任务
func (t *SyncTaskImpl) Resume(ctx context.Context) error {
	t.statusMux.Lock()
	defer t.statusMux.Unlock()

	if t.status != storage.TaskStatusPaused {
		return fmt.Errorf("task %s is not paused", t.id)
	}

	// 检查时间窗口
	if t.config.TimeWindow != nil && !t.config.TimeWindow.IsInWindow(time.Now()) {
		return fmt.Errorf("task %s is outside time window", t.id)
	}

	// 恢复所有工作单元
	for _, worker := range t.workers {
		if err := worker.Resume(ctx); err != nil {
			t.logger.Warnf("Failed to resume worker %s: %v", worker.GetID(), err)
		}
	}

	t.status = storage.TaskStatusRunning

	// 保存任务状态
	t.saveTaskState()

	t.logger.Infof("Task %s resumed", t.id)
	return nil
}

// GetProgress 获取任务进度
func (t *SyncTaskImpl) GetProgress() *SyncProgress {
	t.workersMux.RLock()
	defer t.workersMux.RUnlock()

	// 聚合所有工作单元的进度
	totalProgress := &SyncProgress{
		TaskID:        t.id,
		ActiveWorkers: 0,
	}

	for _, worker := range t.workers {
		workerProgress := worker.GetProgress()

		totalProgress.TotalRows += workerProgress.TotalRows
		totalProgress.ProcessedRows += workerProgress.ProcessedRows
		totalProgress.TotalBytes += workerProgress.TotalBytes
		totalProgress.ProcessedBytes += workerProgress.ProcessedBytes
		totalProgress.ErrorCount += workerProgress.ErrorCount

		if worker.IsRunning() {
			totalProgress.ActiveWorkers++
		}

		// 取最大的已用时间
		if workerProgress.ElapsedTime > totalProgress.ElapsedTime {
			totalProgress.ElapsedTime = workerProgress.ElapsedTime
		}
	}

	// 计算总体进度百分比
	if totalProgress.TotalRows > 0 {
		totalProgress.Percentage = float64(totalProgress.ProcessedRows) / float64(totalProgress.TotalRows) * 100
	}

	// 计算平均速率
	if totalProgress.ElapsedTime.Seconds() > 0 {
		totalProgress.AverageRate = float64(totalProgress.ProcessedBytes) / totalProgress.ElapsedTime.Seconds()
	}

	// 计算当前速率（所有工作单元的当前速率之和）
	for _, worker := range t.workers {
		workerProgress := worker.GetProgress()
		totalProgress.CurrentRate += workerProgress.CurrentRate
	}

	// 估算剩余时间
	if totalProgress.ProcessedRows > 0 && totalProgress.TotalRows > 0 && totalProgress.AverageRate > 0 {
		remainingBytes := totalProgress.TotalBytes - totalProgress.ProcessedBytes
		if remainingBytes > 0 {
			totalProgress.EstimatedTime = time.Duration(float64(remainingBytes)/totalProgress.AverageRate) * time.Second
		}
	}

	return totalProgress
}

// GetStatus 获取任务状态
func (t *SyncTaskImpl) GetStatus() storage.TaskStatus {
	t.statusMux.RLock()
	defer t.statusMux.RUnlock()
	return t.status
}

// UpdateProgress 更新进度
func (t *SyncTaskImpl) UpdateProgress() error {
	progress := t.GetProgress()

	// 更新存储中的任务状态
	err := t.storage.UpdateProgress(context.Background(), t.id, progress.ProcessedRows, progress.ProcessedBytes)
	if err != nil {
		t.logger.Warnf("Failed to update progress in storage: %v", err)
	}

	return err
}

// Validate 验证数据完整性
func (t *SyncTaskImpl) Validate(ctx context.Context) error {
	if !t.config.Validate.Enabled {
		return nil
	}

	t.logger.Infof("Starting data validation for task %s", t.id)

	// 构建 WHERE 条件
	whereClause := ""
	if t.config.DataRange.Where != "" {
		whereClause = t.config.DataRange.Where
	}

	// 验证行数
	if t.config.Validate.CheckRowCount {
		sourceRows, err := t.chClient.CountRows(ctx, t.config.SourceTable, whereClause)
		if err != nil {
			return fmt.Errorf("failed to count source rows: %w", err)
		}

		targetRows, err := t.srClient.CountRows(ctx, t.config.TargetTable, whereClause)
		if err != nil {
			return fmt.Errorf("failed to count target rows: %w", err)
		}

		diff := float64(abs(sourceRows-targetRows)) / float64(sourceRows) * 100
		if diff > t.config.Validate.TolerancePercent {
			return fmt.Errorf("row count validation failed: source=%d, target=%d, diff=%.2f%%",
				sourceRows, targetRows, diff)
		}

		t.logger.Infof("Row count validation passed: source=%d, target=%d", sourceRows, targetRows)
	}

	// 验证校验和
	if t.config.Validate.CheckChecksum {
		sourceChecksum, err := t.chClient.CalculateChecksum(ctx, t.config.SourceTable, t.config.Validate.CheckColumns, whereClause)
		if err != nil {
			return fmt.Errorf("failed to calculate source checksum: %w", err)
		}

		targetChecksum, err := t.srClient.CalculateChecksum(ctx, t.config.TargetTable, t.config.Validate.CheckColumns, whereClause)
		if err != nil {
			return fmt.Errorf("failed to calculate target checksum: %w", err)
		}

		if sourceChecksum != targetChecksum {
			return fmt.Errorf("checksum validation failed: source=%s, target=%s", sourceChecksum, targetChecksum)
		}

		t.logger.Infof("Checksum validation passed: %s", sourceChecksum)
	}

	t.logger.Infof("Data validation completed for task %s", t.id)
	return nil
}

// createDefaultWorkers 创建默认工作单元
func (t *SyncTaskImpl) createDefaultWorkers() error {
	maxWorkers := t.config.Concurrency.MaxWorkers
	if maxWorkers <= 0 {
		maxWorkers = 1
	}

	for i := 0; i < maxWorkers; i++ {
		worker := NewSyncWorker(
			t.config,
			t.chClient,
			t.srClient,
			t.pipeline,
			t.storage,
			t.logger,
		)

		if err := t.AddWorker(worker); err != nil {
			return fmt.Errorf("failed to add worker %d: %w", i, err)
		}
	}

	return nil
}

// stopAllWorkers 停止所有工作单元
func (t *SyncTaskImpl) stopAllWorkers(ctx context.Context) {
	t.workersMux.RLock()
	workers := make([]Worker, 0, len(t.workers))
	for _, worker := range t.workers {
		workers = append(workers, worker)
	}
	t.workersMux.RUnlock()

	// 并行停止所有工作单元
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go func(w Worker) {
			defer wg.Done()
			if err := w.Stop(ctx); err != nil {
				t.logger.Warnf("Failed to stop worker %s: %v", w.GetID(), err)
			}
		}(worker)
	}

	// 等待所有工作单元停止，或超时
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.logger.Debugf("All workers stopped for task %s", t.id)
	case <-ctx.Done():
		t.logger.Warnf("Stop workers timeout for task %s", t.id)
	}
}

// saveTaskState 保存任务状态
func (t *SyncTaskImpl) saveTaskState() {
	progress := t.GetProgress()

	taskState := &storage.TaskState{
		TaskID:         t.id,
		Status:         t.status,
		StartTime:      t.startTime,
		EndTime:        t.endTime,
		UpdateTime:     time.Now(),
		TotalRows:      progress.TotalRows,
		ProcessedRows:  progress.ProcessedRows,
		TotalBytes:     progress.TotalBytes,
		ProcessedBytes: progress.ProcessedBytes,
		BytesPerSecond: progress.CurrentRate,
		RowsPerSecond:  float64(progress.ProcessedRows) / progress.ElapsedTime.Seconds(),
	}

	if err := t.storage.SaveTaskState(context.Background(), taskState); err != nil {
		t.logger.Warnf("Failed to save task state: %v", err)
	}
}

// abs 计算绝对值
func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}