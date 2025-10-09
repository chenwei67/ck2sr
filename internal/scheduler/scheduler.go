package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/internal/storage"
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
	once    sync.Once
}

func NewScheduler(cfg *config.PolicyConfig, store storage.Storage, logger *logrus.Logger) *Scheduler {
	return &Scheduler{
		config:  cfg,
		storage: store,
		logger:  logger,
		tasks:   make(map[string]TaskExecutor),
		running: make(map[string]context.CancelFunc),
		stopCh:  make(chan struct{}),
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
	s.logger.Info("Scheduler starting...")

	s.wg.Add(1)
	// 主动调度一次
	s.once.Do(func() {
		s.checkAndScheduleTasks(ctx)
	})

	// 周期性调度
	go s.schedulerLoop(ctx)

	s.logger.Info("Scheduler started")
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

func (s *Scheduler) schedulerLoop(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.config.Schedule.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.checkAndScheduleTasks(ctx)
		}
	}
}

func (s *Scheduler) checkAndScheduleTasks(ctx context.Context) {
	s.mu.RLock()
	tasks := make(map[string]TaskExecutor, len(s.tasks))
	for k, v := range s.tasks {
		tasks[k] = v
	}
	runningCount := len(s.running)
	s.mu.RUnlock()

	if runningCount >= s.config.Schedule.MaxConcurrentTask {
		return
	}

	for taskID, task := range tasks {
		s.mu.RLock()
		_, isRunning := s.running[taskID]
		s.mu.RUnlock()

		if isRunning {
			continue
		}

		if !task.GetTaskConfig().Enabled {
			s.logger.Debugf("Task %s is disabled, skipping", taskID)
			continue
		}

		if s.shouldExecuteTask(ctx, taskID) {
			s.executeTask(ctx, task)
		}
	}
}

func (s *Scheduler) shouldExecuteTask(ctx context.Context, taskID string) bool {
	state, err := s.storage.LoadTaskState(ctx, taskID)
	if err != nil {
		s.logger.Warnf("Failed to load task state for %s: %v", taskID, err)
		return true
	}

	if state == nil {
		return true
	}

	if state.Status == storage.TaskStatusFailed {
		timeSinceLastRun := time.Since(state.LastRunTime)
		if timeSinceLastRun >= s.config.Schedule.RetryInterval {
			return true
		}
	}

	s.logger.Debugf("Task %s not eligible for execution (status: %+v)", taskID, state)
	return false
}

func (s *Scheduler) executeTask(ctx context.Context, task TaskExecutor) {
	taskID := task.GetTaskID()
	s.mu.Lock()
	if _, isRunning := s.running[taskID]; isRunning {
		s.mu.Unlock()
		return
	}

	if len(s.running) >= s.config.Schedule.MaxConcurrentTask {
		s.mu.Unlock()
		return
	}

	taskCtx, cancel := context.WithCancel(ctx)
	s.running[taskID] = cancel
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() {
			s.mu.Lock()
			delete(s.running, taskID)
			s.mu.Unlock()
		}()

		s.logger.Infof("Executing task: %s", taskID)

		state := &storage.TaskState{
			TaskID:      taskID,
			Status:      storage.TaskStatusRunning,
			LastRunTime: time.Now(),
			UpdatedAt:   time.Now(),
		}
		if err := s.storage.SaveTaskState(taskCtx, state); err != nil {
			s.logger.Errorf("Failed to save task state: %v", err)
		}

		err := task.Execute(taskCtx)

		state.UpdatedAt = time.Now()
		if err != nil {
			s.logger.Errorf("Task %s failed: %v", taskID, err)
			state.Status = storage.TaskStatusFailed
			state.LastError = err.Error()
			state.FailedRuns++
		} else {
			s.logger.Infof("Task %s completed successfully", taskID)
			state.Status = storage.TaskStatusSuccess
			state.SuccessRuns++
		}
		state.TotalRuns++

		if err := s.storage.SaveTaskState(taskCtx, state); err != nil {
			s.logger.Errorf("Failed to save final task state: %v", err)
		}
	}()
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
