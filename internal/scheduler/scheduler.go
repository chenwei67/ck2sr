package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/ck2sr/ck2sr/internal/worker"
)

// SchedulerImpl 调度器实现
type SchedulerImpl struct {
	config      *SchedulerConfig
	taskManager worker.TaskManager
	parser      ScheduleParser
	persistence SchedulePersistence
	executor    TaskExecutor
	logger      *logrus.Logger

	// 调度管理
	schedules    map[string]*Schedule
	schedulesMux sync.RWMutex

	// 运行状态
	running     bool
	runningMux  sync.RWMutex
	stopChan    chan struct{}
	eventChan   chan *ScheduleEvent
	eventBuffer []*ScheduleEvent
	eventMux    sync.RWMutex

	// 统计信息
	stats struct {
		TotalSchedules    int64
		ActiveSchedules   int64
		ExecutedSchedules int64
		FailedSchedules   int64
		LastCheckTime     time.Time
	}
	statsMux sync.RWMutex
}

// NewScheduler 创建调度器
func NewScheduler(
	config *SchedulerConfig,
	taskManager worker.TaskManager,
	persistence SchedulePersistence,
	logger *logrus.Logger,
) *SchedulerImpl {
	if logger == nil {
		logger = logrus.New()
	}

	if config == nil {
		config = &SchedulerConfig{
			CheckInterval:      time.Minute,
			MaxConcurrentTasks: 10,
			EventBufferSize:    1000,
			RetryAttempts:      3,
			RetryInterval:      time.Minute,
		}
	}

	scheduler := &SchedulerImpl{
		config:      config,
		taskManager: taskManager,
		parser:      NewScheduleParser(),
		persistence: persistence,
		executor:    NewTaskExecutor(taskManager, config.MaxConcurrentTasks, logger),
		logger:      logger,
		schedules:   make(map[string]*Schedule),
		stopChan:    make(chan struct{}),
		eventChan:   make(chan *ScheduleEvent, config.EventBufferSize),
		eventBuffer: make([]*ScheduleEvent, 0, config.EventBufferSize),
	}

	// 启动事件处理协程
	go scheduler.eventProcessor()

	return scheduler
}

// Start 启动调度器
func (s *SchedulerImpl) Start(ctx context.Context) error {
	s.runningMux.Lock()
	defer s.runningMux.Unlock()

	if s.running {
		return fmt.Errorf("scheduler is already running")
	}

	// 初始化持久化存储
	if err := s.persistence.Initialize(ctx); err != nil {
		return fmt.Errorf("failed to initialize persistence: %w", err)
	}

	// 加载已保存的调度规则
	if err := s.loadSchedules(ctx); err != nil {
		return fmt.Errorf("failed to load schedules: %w", err)
	}

	s.running = true
	s.logger.Info("Scheduler started")

	// 启动调度循环
	go s.scheduleLoop()

	s.emitEvent("scheduler_started", "", "", "Scheduler started successfully", "")

	return nil
}

// Stop 停止调度器
func (s *SchedulerImpl) Stop(ctx context.Context) error {
	s.runningMux.Lock()
	defer s.runningMux.Unlock()

	if !s.running {
		return nil
	}

	s.running = false
	close(s.stopChan)

	// 等待调度循环结束
	select {
	case <-time.After(10 * time.Second):
		s.logger.Warn("Scheduler stop timeout")
	default:
	}

	// 关闭持久化存储
	if err := s.persistence.Close(); err != nil {
		s.logger.Warnf("Failed to close persistence: %v", err)
	}

	s.logger.Info("Scheduler stopped")
	s.emitEvent("scheduler_stopped", "", "", "Scheduler stopped", "")

	return nil
}

// AddSchedule 添加调度规则
func (s *SchedulerImpl) AddSchedule(schedule *Schedule) error {
	s.schedulesMux.Lock()
	defer s.schedulesMux.Unlock()

	if schedule.ID == "" {
		schedule.ID = uuid.New().String()
	}

	// 验证调度表达式
	if err := s.parser.Validate(schedule.Expression, schedule.Type); err != nil {
		return fmt.Errorf("invalid schedule expression: %w", err)
	}

	// 计算下次运行时间
	if err := s.calculateNextRunTime(schedule); err != nil {
		return fmt.Errorf("failed to calculate next run time: %w", err)
	}

	now := time.Now()
	schedule.CreatedAt = now
	schedule.UpdatedAt = now

	// 保存到持久化存储
	if err := s.persistence.SaveSchedule(context.Background(), schedule); err != nil {
		return fmt.Errorf("failed to save schedule: %w", err)
	}

	s.schedules[schedule.ID] = schedule

	s.statsMux.Lock()
	s.stats.TotalSchedules++
	if schedule.Status == ScheduleStatusActive {
		s.stats.ActiveSchedules++
	}
	s.statsMux.Unlock()

	s.logger.Infof("Added schedule: %s for task %s", schedule.ID, schedule.TaskID)
	s.emitEvent("schedule_added", schedule.ID, schedule.TaskID, "Schedule added", "")

	return nil
}

// RemoveSchedule 移除调度规则
func (s *SchedulerImpl) RemoveSchedule(scheduleID string) error {
	s.schedulesMux.Lock()
	defer s.schedulesMux.Unlock()

	schedule, exists := s.schedules[scheduleID]
	if !exists {
		return fmt.Errorf("schedule not found: %s", scheduleID)
	}

	// 从持久化存储删除
	if err := s.persistence.DeleteSchedule(context.Background(), scheduleID); err != nil {
		return fmt.Errorf("failed to delete schedule: %w", err)
	}

	delete(s.schedules, scheduleID)

	s.statsMux.Lock()
	s.stats.TotalSchedules--
	if schedule.Status == ScheduleStatusActive {
		s.stats.ActiveSchedules--
	}
	s.statsMux.Unlock()

	s.logger.Infof("Removed schedule: %s", scheduleID)
	s.emitEvent("schedule_removed", scheduleID, schedule.TaskID, "Schedule removed", "")

	return nil
}

// UpdateSchedule 更新调度规则
func (s *SchedulerImpl) UpdateSchedule(schedule *Schedule) error {
	s.schedulesMux.Lock()
	defer s.schedulesMux.Unlock()

	existing, exists := s.schedules[schedule.ID]
	if !exists {
		return fmt.Errorf("schedule not found: %s", schedule.ID)
	}

	// 验证调度表达式
	if err := s.parser.Validate(schedule.Expression, schedule.Type); err != nil {
		return fmt.Errorf("invalid schedule expression: %w", err)
	}

	// 保留创建时间
	schedule.CreatedAt = existing.CreatedAt
	schedule.UpdatedAt = time.Now()

	// 计算下次运行时间
	if err := s.calculateNextRunTime(schedule); err != nil {
		return fmt.Errorf("failed to calculate next run time: %w", err)
	}

	// 保存到持久化存储
	if err := s.persistence.SaveSchedule(context.Background(), schedule); err != nil {
		return fmt.Errorf("failed to save schedule: %w", err)
	}

	s.schedules[schedule.ID] = schedule

	s.logger.Infof("Updated schedule: %s", schedule.ID)
	s.emitEvent("schedule_updated", schedule.ID, schedule.TaskID, "Schedule updated", "")

	return nil
}

// GetSchedule 获取调度规则
func (s *SchedulerImpl) GetSchedule(scheduleID string) (*Schedule, error) {
	s.schedulesMux.RLock()
	defer s.schedulesMux.RUnlock()

	schedule, exists := s.schedules[scheduleID]
	if !exists {
		return nil, fmt.Errorf("schedule not found: %s", scheduleID)
	}

	// 返回副本
	result := *schedule
	return &result, nil
}

// GetAllSchedules 获取所有调度规则
func (s *SchedulerImpl) GetAllSchedules() ([]*Schedule, error) {
	s.schedulesMux.RLock()
	defer s.schedulesMux.RUnlock()

	schedules := make([]*Schedule, 0, len(s.schedules))
	for _, schedule := range s.schedules {
		// 创建副本
		scheduleCopy := *schedule
		schedules = append(schedules, &scheduleCopy)
	}

	return schedules, nil
}

// GetSchedulesByTask 根据任务 ID 获取调度规则
func (s *SchedulerImpl) GetSchedulesByTask(taskID string) ([]*Schedule, error) {
	s.schedulesMux.RLock()
	defer s.schedulesMux.RUnlock()

	var schedules []*Schedule
	for _, schedule := range s.schedules {
		if schedule.TaskID == taskID {
			// 创建副本
			scheduleCopy := *schedule
			schedules = append(schedules, &scheduleCopy)
		}
	}

	return schedules, nil
}

// EnableSchedule 启用调度规则
func (s *SchedulerImpl) EnableSchedule(scheduleID string) error {
	s.schedulesMux.Lock()
	defer s.schedulesMux.Unlock()

	schedule, exists := s.schedules[scheduleID]
	if !exists {
		return fmt.Errorf("schedule not found: %s", scheduleID)
	}

	if schedule.Status == ScheduleStatusActive {
		return nil // 已经是活跃状态
	}

	schedule.Status = ScheduleStatusActive
	schedule.UpdatedAt = time.Now()

	// 重新计算下次运行时间
	if err := s.calculateNextRunTime(schedule); err != nil {
		return fmt.Errorf("failed to calculate next run time: %w", err)
	}

	// 保存到持久化存储
	if err := s.persistence.SaveSchedule(context.Background(), schedule); err != nil {
		return fmt.Errorf("failed to save schedule: %w", err)
	}

	s.statsMux.Lock()
	s.stats.ActiveSchedules++
	s.statsMux.Unlock()

	s.logger.Infof("Enabled schedule: %s", scheduleID)
	s.emitEvent("schedule_enabled", scheduleID, schedule.TaskID, "Schedule enabled", "")

	return nil
}

// DisableSchedule 禁用调度规则
func (s *SchedulerImpl) DisableSchedule(scheduleID string) error {
	s.schedulesMux.Lock()
	defer s.schedulesMux.Unlock()

	schedule, exists := s.schedules[scheduleID]
	if !exists {
		return fmt.Errorf("schedule not found: %s", scheduleID)
	}

	if schedule.Status == ScheduleStatusInactive {
		return nil // 已经是非活跃状态
	}

	schedule.Status = ScheduleStatusInactive
	schedule.UpdatedAt = time.Now()
	schedule.NextRunTime = nil

	// 保存到持久化存储
	if err := s.persistence.SaveSchedule(context.Background(), schedule); err != nil {
		return fmt.Errorf("failed to save schedule: %w", err)
	}

	s.statsMux.Lock()
	s.stats.ActiveSchedules--
	s.statsMux.Unlock()

	s.logger.Infof("Disabled schedule: %s", scheduleID)
	s.emitEvent("schedule_disabled", scheduleID, schedule.TaskID, "Schedule disabled", "")

	return nil
}

// TriggerSchedule 手动触发调度
func (s *SchedulerImpl) TriggerSchedule(scheduleID string) error {
	schedule, err := s.GetSchedule(scheduleID)
	if err != nil {
		return err
	}

	s.logger.Infof("Manually triggering schedule: %s", scheduleID)
	return s.executeSchedule(context.Background(), schedule)
}

// GetEvents 获取调度事件
func (s *SchedulerImpl) GetEvents(limit int) ([]*ScheduleEvent, error) {
	s.eventMux.RLock()
	defer s.eventMux.RUnlock()

	if limit <= 0 || limit > len(s.eventBuffer) {
		limit = len(s.eventBuffer)
	}

	events := make([]*ScheduleEvent, limit)
	copy(events, s.eventBuffer[len(s.eventBuffer)-limit:])

	return events, nil
}

// GetStatistics 获取调度器统计信息
func (s *SchedulerImpl) GetStatistics() map[string]interface{} {
	s.statsMux.RLock()
	defer s.statsMux.RUnlock()

	return map[string]interface{}{
		"total_schedules":    s.stats.TotalSchedules,
		"active_schedules":   s.stats.ActiveSchedules,
		"executed_schedules": s.stats.ExecutedSchedules,
		"failed_schedules":   s.stats.FailedSchedules,
		"last_check_time":    s.stats.LastCheckTime,
		"running_tasks":      s.executor.GetRunningCount(),
		"max_concurrent":     s.executor.GetMaxConcurrent(),
	}
}

// IsRunning 检查调度器是否正在运行
func (s *SchedulerImpl) IsRunning() bool {
	s.runningMux.RLock()
	defer s.runningMux.RUnlock()
	return s.running
}

// scheduleLoop 调度循环
func (s *SchedulerImpl) scheduleLoop() {
	ticker := time.NewTicker(s.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.checkSchedules()
		}
	}
}

// checkSchedules 检查调度规则
func (s *SchedulerImpl) checkSchedules() {
	s.statsMux.Lock()
	s.stats.LastCheckTime = time.Now()
	s.statsMux.Unlock()

	s.schedulesMux.RLock()
	schedules := make([]*Schedule, 0, len(s.schedules))
	for _, schedule := range s.schedules {
		schedules = append(schedules, schedule)
	}
	s.schedulesMux.RUnlock()

	now := time.Now()
	for _, schedule := range schedules {
		if s.shouldExecute(schedule, now) {
			go s.executeScheduleWithRetry(context.Background(), schedule)
		}
	}
}

// shouldExecute 检查是否应该执行调度
func (s *SchedulerImpl) shouldExecute(schedule *Schedule, now time.Time) bool {
	if schedule.Status != ScheduleStatusActive {
		return false
	}

	if schedule.NextRunTime == nil {
		return false
	}

	if now.Before(*schedule.NextRunTime) {
		return false
	}

	// 检查时间窗口限制
	if schedule.TimeWindow != nil && !schedule.TimeWindow.IsInWindow(now) {
		return false
	}

	// 检查最大运行次数
	if schedule.MaxRuns > 0 && schedule.RunCount >= schedule.MaxRuns {
		s.schedulesMux.Lock()
		schedule.Status = ScheduleStatusCompleted
		schedule.UpdatedAt = time.Now()
		s.persistence.SaveSchedule(context.Background(), schedule)
		s.schedulesMux.Unlock()
		return false
	}

	return true
}

// executeScheduleWithRetry 带重试的执行调度
func (s *SchedulerImpl) executeScheduleWithRetry(ctx context.Context, schedule *Schedule) {
	var lastErr error

	for attempt := 0; attempt <= s.config.RetryAttempts; attempt++ {
		if attempt > 0 {
			s.logger.Warnf("Retrying schedule %s, attempt %d/%d", schedule.ID, attempt, s.config.RetryAttempts)
			time.Sleep(s.config.RetryInterval)
		}

		if err := s.executeSchedule(ctx, schedule); err != nil {
			lastErr = err
			s.logger.Errorf("Failed to execute schedule %s: %v", schedule.ID, err)
			continue
		}

		// 执行成功
		return
	}

	// 所有重试都失败了
	s.handleScheduleFailure(schedule, lastErr)
}

// executeSchedule 执行调度
func (s *SchedulerImpl) executeSchedule(ctx context.Context, schedule *Schedule) error {
	s.logger.Infof("Executing schedule: %s for task %s", schedule.ID, schedule.TaskID)

	// 检查任务执行器是否可以执行
	if !s.executor.CanExecute(schedule.TaskID) {
		return fmt.Errorf("cannot execute task %s: executor busy", schedule.TaskID)
	}

	startTime := time.Now()

	// 执行任务
	if err := s.executor.Execute(ctx, schedule.TaskID); err != nil {
		return fmt.Errorf("task execution failed: %w", err)
	}

	duration := time.Since(startTime)

	// 更新调度信息
	s.updateScheduleAfterExecution(schedule)

	s.statsMux.Lock()
	s.stats.ExecutedSchedules++
	s.statsMux.Unlock()

	s.logger.Infof("Schedule %s executed successfully in %v", schedule.ID, duration)
	s.emitEvent("schedule_executed", schedule.ID, schedule.TaskID,
		fmt.Sprintf("Schedule executed successfully in %v", duration), "")

	return nil
}

// updateScheduleAfterExecution 执行后更新调度信息
func (s *SchedulerImpl) updateScheduleAfterExecution(schedule *Schedule) {
	s.schedulesMux.Lock()
	defer s.schedulesMux.Unlock()

	now := time.Now()
	schedule.LastRunTime = &now
	schedule.RunCount++
	schedule.UpdatedAt = now

	// 计算下次运行时间
	if err := s.calculateNextRunTime(schedule); err != nil {
		s.logger.Errorf("Failed to calculate next run time for schedule %s: %v", schedule.ID, err)
		schedule.Status = ScheduleStatusFailed
	}

	// 保存到持久化存储
	if err := s.persistence.SaveSchedule(context.Background(), schedule); err != nil {
		s.logger.Errorf("Failed to save schedule after execution: %v", err)
	}
}

// handleScheduleFailure 处理调度失败
func (s *SchedulerImpl) handleScheduleFailure(schedule *Schedule, err error) {
	s.schedulesMux.Lock()
	defer s.schedulesMux.Unlock()

	schedule.Status = ScheduleStatusFailed
	schedule.UpdatedAt = time.Now()

	s.statsMux.Lock()
	s.stats.FailedSchedules++
	if schedule.Status == ScheduleStatusActive {
		s.stats.ActiveSchedules--
	}
	s.statsMux.Unlock()

	// 保存到持久化存储
	if saveErr := s.persistence.SaveSchedule(context.Background(), schedule); saveErr != nil {
		s.logger.Errorf("Failed to save failed schedule: %v", saveErr)
	}

	s.logger.Errorf("Schedule %s failed permanently: %v", schedule.ID, err)
	s.emitEvent("schedule_failed", schedule.ID, schedule.TaskID, "Schedule failed permanently", err.Error())
}

// calculateNextRunTime 计算下次运行时间
func (s *SchedulerImpl) calculateNextRunTime(schedule *Schedule) error {
	if schedule.Status != ScheduleStatusActive {
		schedule.NextRunTime = nil
		return nil
	}

	expr, err := s.parser.Parse(schedule.Expression, schedule.Type)
	if err != nil {
		return err
	}

	from := time.Now()
	if schedule.LastRunTime != nil {
		from = *schedule.LastRunTime
	}

	nextTime := expr.Next(from)
	schedule.NextRunTime = nextTime

	return nil
}

// loadSchedules 加载调度规则
func (s *SchedulerImpl) loadSchedules(ctx context.Context) error {
	schedules, err := s.persistence.LoadAllSchedules(ctx)
	if err != nil {
		return err
	}

	s.schedulesMux.Lock()
	defer s.schedulesMux.Unlock()

	for _, schedule := range schedules {
		// 重新计算下次运行时间
		if err := s.calculateNextRunTime(schedule); err != nil {
			s.logger.Warnf("Failed to calculate next run time for schedule %s: %v", schedule.ID, err)
			continue
		}

		s.schedules[schedule.ID] = schedule

		s.statsMux.Lock()
		s.stats.TotalSchedules++
		if schedule.Status == ScheduleStatusActive {
			s.stats.ActiveSchedules++
		}
		s.statsMux.Unlock()
	}

	s.logger.Infof("Loaded %d schedules", len(schedules))
	return nil
}

// emitEvent 发送事件
func (s *SchedulerImpl) emitEvent(eventType, scheduleID, taskID, message, errorMsg string) {
	event := &ScheduleEvent{
		Type:       eventType,
		ScheduleID: scheduleID,
		TaskID:     taskID,
		Timestamp:  time.Now(),
		Message:    message,
		Error:      errorMsg,
	}

	select {
	case s.eventChan <- event:
	default:
		// 如果通道满了，丢弃事件
		s.logger.Warn("Event channel full, dropping event")
	}
}

// eventProcessor 事件处理器
func (s *SchedulerImpl) eventProcessor() {
	for event := range s.eventChan {
		// 保存事件到持久化存储
		if err := s.persistence.SaveEvent(context.Background(), event); err != nil {
			s.logger.Warnf("Failed to save event: %v", err)
		}

		// 添加到内存缓冲区
		s.eventMux.Lock()
		s.eventBuffer = append(s.eventBuffer, event)
		if len(s.eventBuffer) > s.config.EventBufferSize {
			// 保留最新的事件
			copy(s.eventBuffer, s.eventBuffer[1:])
			s.eventBuffer = s.eventBuffer[:s.config.EventBufferSize]
		}
		s.eventMux.Unlock()
	}
}