package scheduler

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/internal/storage"
	"github.com/sunkaimr/ck2sr/pkg/utils"
)

type TaskStatus string

const (
	TaskStatusPending TaskStatus = "pending"
	TaskStatusRunning TaskStatus = "running"
	TaskStatusSuccess TaskStatus = "success"
	TaskStatusFailed  TaskStatus = "failed"
	TaskStatusSkipped TaskStatus = "skipped"
)

type TaskExecutor interface {
	Execute(ctx context.Context) error
	GetTaskID() string
	GetTaskConfig() *config.SyncTaskConfig
}

type Scheduler struct {
	config  *config.PolicyConfig
	storage storage.Storage
	logger  *logrus.Logger
	tasks   map[string]TaskExecutor
	running map[string]context.CancelFunc
	mu      sync.RWMutex
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// 两阶段调度相关
	phase1Complete bool             // Phase 1 是否完成
	taskResults    map[string]error // 任务执行结果
	resultsMu      sync.RWMutex
}

func NewScheduler(cfg *config.PolicyConfig, store storage.Storage, logger *logrus.Logger) *Scheduler {
	return &Scheduler{
		config:      cfg,
		storage:     store,
		logger:      logger,
		tasks:       make(map[string]TaskExecutor),
		running:     make(map[string]context.CancelFunc),
		stopCh:      make(chan struct{}),
		taskResults: make(map[string]error),
	}
}

func (s *Scheduler) RegisterTask(task TaskExecutor) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	taskID := task.GetTaskID()
	if _, exists := s.tasks[taskID]; exists {
		return fmt.Errorf("task %s already registered", taskID)
	}

	s.tasks[taskID] = task
	s.logger.Infof("Registered task: %s", taskID)
	return nil
}

func (s *Scheduler) UnregisterTask(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.tasks[taskID]; !exists {
		return fmt.Errorf("task %s not found", taskID)
	}

	delete(s.tasks, taskID)
	s.logger.Infof("Unregistered task: %s", taskID)
	return nil
}

func (s *Scheduler) Start(ctx context.Context) error {
	s.logger.Info("Scheduler starting with two-phase scheduling algorithm...")

	// 检查时间窗口配置
	timeWindowCfg := s.config.Schedule.TimeWindow
	if timeWindowCfg.Enabled {
		s.logger.Infof("Time window policy enabled: %s",
			utils.FormatTimeWindow(timeWindowCfg.StartTime, timeWindowCfg.EndTime))

		// 检查当前是否在时间窗口内
		inWindow, err := utils.IsInTimeWindow(timeWindowCfg.StartTime, timeWindowCfg.EndTime)
		if err != nil {
			return fmt.Errorf("failed to check time window: %w", err)
		}

		if !inWindow {
			// 不在时间窗口内，等待
			s.logger.Info("Current time is outside the time window, waiting...")
			if err := s.waitForTimeWindow(ctx); err != nil {
				return fmt.Errorf("failed to wait for time window: %w", err)
			}
		}
	}

	// Phase 1: 执行所有未执行的任务（idle状态或无状态）
	s.logger.Info("Phase 1: Executing all pending tasks...")
	if err := s.executePhase1(ctx); err != nil {
		s.logger.Errorf("Phase 1 execution failed: %v", err)
		return err
	}
	s.phase1Complete = true
	s.logger.Info("Phase 1 completed")

	// Phase 2: 根据RetryTimes配置重试失败的任务
	if s.config.Schedule.RetryTimes != 0 {
		s.logger.Info("Phase 2: Retrying failed tasks...")
		if err := s.executePhase2(ctx); err != nil {
			s.logger.Errorf("Phase 2 execution failed: %v", err)
			return err
		}
		s.logger.Info("Phase 2 completed")
	} else {
		s.logger.Info("Phase 2 skipped (RetryTimes = 0)")
	}

	// 生成退出码
	exitCode := s.generateExitCode()
	s.logger.Infof("Scheduler completed with exit code: %d", exitCode)

	return nil
}

func (s *Scheduler) Stop() error {
	s.logger.Info("Scheduler stopping...")

	close(s.stopCh)

	s.mu.Lock()
	for taskID, cancel := range s.running {
		s.logger.Infof("Cancelling running task: %s", taskID)
		cancel()
	}
	s.mu.Unlock()

	s.wg.Wait()

	s.logger.Info("Scheduler stopped")
	return nil
}

// executePhase1 执行Phase 1：执行所有未执行的任务（idle状态或无状态）
func (s *Scheduler) executePhase1(ctx context.Context) error {
	s.mu.RLock()
	tasks := make(map[string]TaskExecutor, len(s.tasks))
	for k, v := range s.tasks {
		tasks[k] = v
	}
	s.mu.RUnlock()

	// 筛选需要在Phase 1执行的任务（状态为idle、paused或无状态）
	pendingTasks := make(map[string]TaskExecutor)
	for taskID, task := range tasks {
		if !task.GetTaskConfig().Enabled {
			s.logger.Debugf("Task %s is disabled, skipping", taskID)
			continue
		}

		state, err := s.storage.LoadTaskState(ctx, taskID)
		if err != nil || state == nil || state.Status == storage.TaskStatusIdle || state.Status == storage.TaskStatusRunning || state.Status == storage.TaskStatusPaused {
			// 无状态、idle状态、running状态或paused状态的任务需要在Phase 1执行
			// paused状态表示之前因时间窗口暂停，需要恢复执行
			if state != nil && state.Status == storage.TaskStatusPaused {
				s.logger.Infof("Phase 1: Task %s was paused, will resume execution", taskID)
				// 将paused状态恢复为idle，准备重新执行
				state.Status = storage.TaskStatusIdle
				if err := s.storage.SaveTaskState(ctx, state); err != nil {
					s.logger.Errorf("Failed to update paused task state: %v", err)
				}
			}
			pendingTasks[taskID] = task
			s.logger.Infof("Phase 1: Task %s will be executed (status: %v)", taskID, state)
		} else if state.Status == storage.TaskStatusSuccess {
			s.logger.Infof("Phase 1: Task %s already completed, skipping", taskID)
			s.resultsMu.Lock()
			s.taskResults[taskID] = nil // 已成功的任务
			s.resultsMu.Unlock()
		} else if state.Status == storage.TaskStatusFailed {
			s.logger.Infof("Phase 1: Task %s previously failed, will retry in Phase 2", taskID)
			s.resultsMu.Lock()
			s.taskResults[taskID] = fmt.Errorf("previously failed") // 标记为失败，等待Phase 2重试
			s.resultsMu.Unlock()
		}
	}

	if len(pendingTasks) == 0 {
		s.logger.Info("Phase 1: No pending tasks to execute")
		return nil
	}

	// 使用信号量控制并发度
	semaphore := make(chan struct{}, s.config.Schedule.MaxConcurrentTask)
	var phase1WG sync.WaitGroup

	for taskID, task := range pendingTasks {
		phase1WG.Add(1)
		go func(id string, t TaskExecutor) {
			defer phase1WG.Done()

			// 获取信号量
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			err := s.executeTaskSync(ctx, t)
			s.resultsMu.Lock()
			s.taskResults[id] = err
			s.resultsMu.Unlock()

			if err != nil {
				s.logger.Errorf("Phase 1: Task %s failed: %v", id, err)
			} else {
				s.logger.Infof("Phase 1: Task %s completed successfully", id)
			}
		}(taskID, task)
	}

	phase1WG.Wait()
	s.logger.Infof("Phase 1: Completed %d tasks", len(pendingTasks))
	return nil
}

// executePhase2 执行Phase 2：重试失败的任务
func (s *Scheduler) executePhase2(ctx context.Context) error {
	retryTimes := s.config.Schedule.RetryTimes
	retryInterval := s.config.Schedule.RetryInterval

	// -1表示无限重试，用一个大数字代替
	maxRetries := retryTimes
	if retryTimes == -1 {
		maxRetries = math.MaxInt64 // 实际上是无限重试
	}

	for attempt := 0; attempt < maxRetries; attempt++ {
		// 收集需要重试的失败任务
		s.resultsMu.RLock()
		failedTasks := make(map[string]TaskExecutor)
		s.mu.RLock()
		for taskID, err := range s.taskResults {
			if err != nil {
				if task, exists := s.tasks[taskID]; exists {
					failedTasks[taskID] = task
				}
			}
		}
		s.mu.RUnlock()
		s.resultsMu.RUnlock()

		if len(failedTasks) == 0 {
			s.logger.Info("Phase 2: No failed tasks to retry")
			break
		}

		s.logger.Infof("Phase 2: Retry attempt %d/%d for %d failed tasks", attempt+1, maxRetries, len(failedTasks))

		// 等待重试间隔
		if attempt > 0 {
			s.logger.Infof("Phase 2: Waiting %v before retry...", retryInterval)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryInterval):
			}
		}

		// 使用信号量控制并发度
		semaphore := make(chan struct{}, s.config.Schedule.MaxConcurrentTask)
		var phase2WG sync.WaitGroup

		for taskID, task := range failedTasks {
			phase2WG.Add(1)
			go func(id string, t TaskExecutor) {
				defer phase2WG.Done()

				// 获取信号量
				semaphore <- struct{}{}
				defer func() { <-semaphore }()

				err := s.executeTaskSync(ctx, t)
				s.resultsMu.Lock()
				s.taskResults[id] = err
				s.resultsMu.Unlock()

				if err != nil {
					s.logger.Errorf("Phase 2: Task %s retry failed: %v", id, err)
				} else {
					s.logger.Infof("Phase 2: Task %s retry succeeded", id)
				}
			}(taskID, task)
		}

		phase2WG.Wait()
	}

	// 统计最终结果
	s.resultsMu.RLock()
	failedCount := 0
	for _, err := range s.taskResults {
		if err != nil {
			failedCount++
		}
	}
	s.resultsMu.RUnlock()

	if failedCount > 0 {
		s.logger.Warnf("Phase 2: Completed with %d failed tasks", failedCount)
	} else {
		s.logger.Info("Phase 2: All tasks completed successfully")
	}

	return nil
}

// executeTaskSync 同步执行任务（阻塞直到任务完成）
func (s *Scheduler) executeTaskSync(ctx context.Context, task TaskExecutor) error {
	taskID := task.GetTaskID()
	s.logger.Infof("Executing task: %s", taskID)

	// 尝试加载之前的状态
	existingState, _ := s.storage.LoadTaskState(ctx, taskID)

	state := &storage.TaskState{
		TaskID:        taskID,
		Status:        storage.TaskStatusRunning,
		LastRunTime:   time.Now(),
		UpdatedAt:     time.Now(),
		ScheduleTimes: 0,
		FailedTimes:   0,
	}

	// 如果有之前的状态，保留计数器和首次启动时间
	if existingState != nil {
		state.ScheduleTimes = existingState.ScheduleTimes
		state.FailedTimes = existingState.FailedTimes
		state.StartedAt = existingState.StartedAt
	}

	// 首次执行时设置 StartedAt
	if state.StartedAt.IsZero() {
		state.StartedAt = time.Now()
	}

	if err := s.storage.SaveTaskState(ctx, state); err != nil {
		s.logger.Errorf("Failed to save task state: %v", err)
		return err
	}

	// 此次阻塞式执行
	err := task.Execute(ctx)

	state.UpdatedAt = time.Now()
	state.FinishedAt = time.Now()
	if err != nil {
		s.logger.Errorf("Task %s failed: %v", taskID, err)
		state.Status = storage.TaskStatusFailed
		state.LastError = err.Error()
		state.FailedTimes++
	} else {
		s.logger.Infof("Task %s completed successfully", taskID)
		state.Status = storage.TaskStatusSuccess
	}
	state.ScheduleTimes++

	if err := s.storage.SaveTaskState(ctx, state); err != nil {
		s.logger.Errorf("Failed to save final task state: %v", err)
	}

	return err
}

// generateExitCode 生成进程退出码
// 0: 所有任务成功
// 1: 存在失败任务
func (s *Scheduler) generateExitCode() int {
	s.resultsMu.RLock()
	defer s.resultsMu.RUnlock()

	for taskID, err := range s.taskResults {
		if err != nil {
			s.logger.Errorf("Task %s failed: %v", taskID, err)
			return 1
		}
	}

	s.logger.Info("All tasks completed successfully")
	return 0
}

// GetExitCode 获取进程退出码（供外部调用）
func (s *Scheduler) GetExitCode() int {
	return s.generateExitCode()
}

func (s *Scheduler) GetTaskStatus(taskID string) (TaskStatus, error) {
	s.mu.RLock()
	_, isRunning := s.running[taskID]
	s.mu.RUnlock()

	if isRunning {
		return TaskStatusRunning, nil
	}

	state, err := s.storage.LoadTaskState(context.Background(), taskID)
	if err != nil {
		return "", fmt.Errorf("failed to load task state: %w", err)
	}

	if state == nil {
		return TaskStatusPending, nil
	}

	return TaskStatus(state.Status), nil
}

func (s *Scheduler) ListTasks() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	taskIDs := make([]string, 0, len(s.tasks))
	for taskID := range s.tasks {
		taskIDs = append(taskIDs, taskID)
	}
	return taskIDs
}

// waitForTimeWindow 等待进入时间窗口
// 使用轮询机制，每隔 check_interval 检查一次是否进入时间窗口
func (s *Scheduler) waitForTimeWindow(ctx context.Context) error {
	timeWindowCfg := s.config.Schedule.TimeWindow
	checkInterval := s.config.Schedule.CheckInterval

	// 计算初始等待时间
	waitDuration, err := utils.CalculateWaitDuration(timeWindowCfg.StartTime, timeWindowCfg.EndTime)
	if err != nil {
		return fmt.Errorf("failed to calculate wait duration: %w", err)
	}

	s.logger.Infof("Waiting for time window to open (estimated: %v)", waitDuration)

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Time window wait cancelled by context")
			return ctx.Err()
		case <-ticker.C:
			// 检查是否进入时间窗口
			inWindow, err := utils.IsInTimeWindow(timeWindowCfg.StartTime, timeWindowCfg.EndTime)
			if err != nil {
				s.logger.Errorf("Failed to check time window: %v", err)
				continue
			}

			if inWindow {
				s.logger.Info("Entered time window, resuming scheduler")
				return nil
			}

			s.logger.Debugf("Still outside time window, checking again in %v", checkInterval)
		}
	}
}

// checkTimeWindowDuringExecution 在任务执行期间检查时间窗口
// 如果超出时间窗口，暂停所有运行中的任务
func (s *Scheduler) checkTimeWindowDuringExecution(ctx context.Context) bool {
	timeWindowCfg := s.config.Schedule.TimeWindow
	if !timeWindowCfg.Enabled {
		return true // 未启用时间窗口，始终允许执行
	}

	inWindow, err := utils.IsInTimeWindow(timeWindowCfg.StartTime, timeWindowCfg.EndTime)
	if err != nil {
		s.logger.Errorf("Failed to check time window: %v", err)
		return true // 发生错误时继续执行
	}

	if !inWindow {
		s.logger.Warn("Time window closed during execution, pausing tasks")
		// 发送暂停信号到所有运行中的任务
		s.pauseAllRunningTasks()
		return false
	}

	return true
}

// pauseAllRunningTasks 暂停所有运行中的任务
func (s *Scheduler) pauseAllRunningTasks() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for taskID, cancel := range s.running {
		s.logger.Infof("Pausing running task due to time window: %s", taskID)
		cancel()

		// 更新任务状态为 paused
		if state, err := s.storage.LoadTaskState(context.Background(), taskID); err == nil && state != nil {
			state.Status = storage.TaskStatusPaused
			state.UpdatedAt = time.Now()
			if err := s.storage.SaveTaskState(context.Background(), state); err != nil {
				s.logger.Errorf("Failed to save paused state for task %s: %v", taskID, err)
			}
		}
	}

	// 清空 running 映射
	s.running = make(map[string]context.CancelFunc)
}
