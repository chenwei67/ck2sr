package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ck2sr/ck2sr/internal/worker"
)

// TaskExecutorImpl 任务执行器实现
type TaskExecutorImpl struct {
	taskManager    worker.TaskManager
	maxConcurrent  int
	runningTasks   map[string]context.CancelFunc
	runningMutex   sync.RWMutex
	logger         *logrus.Logger
}

// NewTaskExecutor 创建任务执行器
func NewTaskExecutor(taskManager worker.TaskManager, maxConcurrent int, logger *logrus.Logger) *TaskExecutorImpl {
	if logger == nil {
		logger = logrus.New()
	}

	return &TaskExecutorImpl{
		taskManager:   taskManager,
		maxConcurrent: maxConcurrent,
		runningTasks:  make(map[string]context.CancelFunc),
		logger:        logger,
	}
}

// Execute 执行任务
func (e *TaskExecutorImpl) Execute(ctx context.Context, taskID string) error {
	e.runningMutex.Lock()
	defer e.runningMutex.Unlock()

	// 检查是否已经在运行
	if _, exists := e.runningTasks[taskID]; exists {
		return fmt.Errorf("task %s is already running", taskID)
	}

	// 检查并发限制
	if len(e.runningTasks) >= e.maxConcurrent {
		return fmt.Errorf("max concurrent tasks reached: %d", e.maxConcurrent)
	}

	// 创建取消上下文
	taskCtx, cancel := context.WithCancel(ctx)
	e.runningTasks[taskID] = cancel

	// 启动任务执行
	go func() {
		defer func() {
			e.runningMutex.Lock()
			delete(e.runningTasks, taskID)
			e.runningMutex.Unlock()
		}()

		if err := e.taskManager.StartTask(taskCtx, taskID); err != nil {
			e.logger.Errorf("Failed to start task %s: %v", taskID, err)
		}
	}()

	return nil
}

// CanExecute 检查是否可以执行任务
func (e *TaskExecutorImpl) CanExecute(taskID string) bool {
	e.runningMutex.RLock()
	defer e.runningMutex.RUnlock()

	// 检查任务是否已经在运行
	if _, exists := e.runningTasks[taskID]; exists {
		return false
	}

	// 检查并发限制
	return len(e.runningTasks) < e.maxConcurrent
}

// GetRunningCount 获取正在运行的任务数量
func (e *TaskExecutorImpl) GetRunningCount() int {
	e.runningMutex.RLock()
	defer e.runningMutex.RUnlock()
	return len(e.runningTasks)
}

// GetMaxConcurrent 获取最大并发数
func (e *TaskExecutorImpl) GetMaxConcurrent() int {
	return e.maxConcurrent
}

// StopTask 停止指定任务
func (e *TaskExecutorImpl) StopTask(taskID string) error {
	e.runningMutex.Lock()
	defer e.runningMutex.Unlock()

	cancel, exists := e.runningTasks[taskID]
	if !exists {
		return fmt.Errorf("task %s is not running", taskID)
	}

	cancel()
	delete(e.runningTasks, taskID)

	e.logger.Infof("Stopped task: %s", taskID)
	return nil
}

// StopAllTasks 停止所有任务
func (e *TaskExecutorImpl) StopAllTasks() {
	e.runningMutex.Lock()
	defer e.runningMutex.Unlock()

	for taskID, cancel := range e.runningTasks {
		cancel()
		e.logger.Infof("Stopped task: %s", taskID)
	}

	e.runningTasks = make(map[string]context.CancelFunc)
}

// FilePersistence 文件持久化实现
type FilePersistence struct {
	dataDir    string
	logger     *logrus.Logger
	mutex      sync.RWMutex
}

// NewFilePersistence 创建文件持久化
func NewFilePersistence(dataDir string, logger *logrus.Logger) *FilePersistence {
	if logger == nil {
		logger = logrus.New()
	}

	return &FilePersistence{
		dataDir: dataDir,
		logger:  logger,
	}
}

// Initialize 初始化持久化存储
func (p *FilePersistence) Initialize(ctx context.Context) error {
	// 创建数据目录
	if err := os.MkdirAll(p.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// 创建子目录
	schedulesDir := filepath.Join(p.dataDir, "schedules")
	if err := os.MkdirAll(schedulesDir, 0755); err != nil {
		return fmt.Errorf("failed to create schedules directory: %w", err)
	}

	eventsDir := filepath.Join(p.dataDir, "events")
	if err := os.MkdirAll(eventsDir, 0755); err != nil {
		return fmt.Errorf("failed to create events directory: %w", err)
	}

	p.logger.Infof("File persistence initialized at: %s", p.dataDir)
	return nil
}

// Close 关闭持久化存储
func (p *FilePersistence) Close() error {
	// 文件持久化无需特殊关闭操作
	return nil
}

// SaveSchedule 保存调度规则
func (p *FilePersistence) SaveSchedule(ctx context.Context, schedule *Schedule) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	scheduleFile := filepath.Join(p.dataDir, "schedules", fmt.Sprintf("%s.json", schedule.ID))
	return p.saveJSON(scheduleFile, schedule)
}

// LoadSchedule 加载调度规则
func (p *FilePersistence) LoadSchedule(ctx context.Context, scheduleID string) (*Schedule, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	scheduleFile := filepath.Join(p.dataDir, "schedules", fmt.Sprintf("%s.json", scheduleID))

	var schedule Schedule
	if err := p.loadJSON(scheduleFile, &schedule); err != nil {
		return nil, err
	}

	return &schedule, nil
}

// LoadAllSchedules 加载所有调度规则
func (p *FilePersistence) LoadAllSchedules(ctx context.Context) ([]*Schedule, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	schedulesDir := filepath.Join(p.dataDir, "schedules")
	entries, err := os.ReadDir(schedulesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []*Schedule{}, nil
		}
		return nil, fmt.Errorf("failed to read schedules directory: %w", err)
	}

	var schedules []*Schedule
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".json" {
			scheduleFile := filepath.Join(schedulesDir, entry.Name())

			var schedule Schedule
			if err := p.loadJSON(scheduleFile, &schedule); err != nil {
				p.logger.Warnf("Failed to load schedule file %s: %v", scheduleFile, err)
				continue
			}

			schedules = append(schedules, &schedule)
		}
	}

	return schedules, nil
}

// DeleteSchedule 删除调度规则
func (p *FilePersistence) DeleteSchedule(ctx context.Context, scheduleID string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	scheduleFile := filepath.Join(p.dataDir, "schedules", fmt.Sprintf("%s.json", scheduleID))
	if err := os.Remove(scheduleFile); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete schedule file: %w", err)
	}

	return nil
}

// SaveEvent 保存调度事件
func (p *FilePersistence) SaveEvent(ctx context.Context, event *ScheduleEvent) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// 按日期分目录存储事件
	dateStr := event.Timestamp.Format("2006-01-02")
	eventDir := filepath.Join(p.dataDir, "events", dateStr)
	if err := os.MkdirAll(eventDir, 0755); err != nil {
		return fmt.Errorf("failed to create event directory: %w", err)
	}

	// 使用时间戳作为文件名
	filename := fmt.Sprintf("%d.json", event.Timestamp.UnixNano())
	eventFile := filepath.Join(eventDir, filename)

	return p.saveJSON(eventFile, event)
}

// LoadEvents 加载调度事件
func (p *FilePersistence) LoadEvents(ctx context.Context, limit int) ([]*ScheduleEvent, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	eventsDir := filepath.Join(p.dataDir, "events")

	var events []*ScheduleEvent

	// 遍历事件目录，按日期倒序
	err := filepath.WalkDir(eventsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() && filepath.Ext(d.Name()) == ".json" {
			var event ScheduleEvent
			if err := p.loadJSON(path, &event); err != nil {
				p.logger.Warnf("Failed to load event file %s: %v", path, err)
				return nil
			}

			events = append(events, &event)
		}

		return nil
	})

	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to walk events directory: %w", err)
	}

	// 按时间倒序排序
	for i := 0; i < len(events)-1; i++ {
		for j := i + 1; j < len(events); j++ {
			if events[i].Timestamp.Before(events[j].Timestamp) {
				events[i], events[j] = events[j], events[i]
			}
		}
	}

	// 限制返回数量
	if limit > 0 && len(events) > limit {
		events = events[:limit]
	}

	return events, nil
}

// saveJSON 保存 JSON 文件
func (p *FilePersistence) saveJSON(filename string, data interface{}) error {
	// 确保目录存在
	dir := filepath.Dir(filename)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// 写入临时文件
	tmpFile := filename + ".tmp"
	file, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	err = encoder.Encode(data)
	file.Close()

	if err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	// 原子性地移动文件
	if err := os.Rename(tmpFile, filename); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to move temp file: %w", err)
	}

	return nil
}

// loadJSON 加载 JSON 文件
func (p *FilePersistence) loadJSON(filename string, data interface{}) error {
	file, err := os.Open(filename)
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

// MemoryPersistence 内存持久化实现（用于测试）
type MemoryPersistence struct {
	schedules map[string]*Schedule
	events    []*ScheduleEvent
	mutex     sync.RWMutex
}

// NewMemoryPersistence 创建内存持久化
func NewMemoryPersistence() *MemoryPersistence {
	return &MemoryPersistence{
		schedules: make(map[string]*Schedule),
		events:    make([]*ScheduleEvent, 0),
	}
}

// Initialize 初始化持久化存储
func (p *MemoryPersistence) Initialize(ctx context.Context) error {
	return nil
}

// Close 关闭持久化存储
func (p *MemoryPersistence) Close() error {
	return nil
}

// SaveSchedule 保存调度规则
func (p *MemoryPersistence) SaveSchedule(ctx context.Context, schedule *Schedule) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// 创建副本
	scheduleCopy := *schedule
	p.schedules[schedule.ID] = &scheduleCopy
	return nil
}

// LoadSchedule 加载调度规则
func (p *MemoryPersistence) LoadSchedule(ctx context.Context, scheduleID string) (*Schedule, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	schedule, exists := p.schedules[scheduleID]
	if !exists {
		return nil, fmt.Errorf("schedule not found: %s", scheduleID)
	}

	// 返回副本
	scheduleCopy := *schedule
	return &scheduleCopy, nil
}

// LoadAllSchedules 加载所有调度规则
func (p *MemoryPersistence) LoadAllSchedules(ctx context.Context) ([]*Schedule, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	schedules := make([]*Schedule, 0, len(p.schedules))
	for _, schedule := range p.schedules {
		// 创建副本
		scheduleCopy := *schedule
		schedules = append(schedules, &scheduleCopy)
	}

	return schedules, nil
}

// DeleteSchedule 删除调度规则
func (p *MemoryPersistence) DeleteSchedule(ctx context.Context, scheduleID string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	delete(p.schedules, scheduleID)
	return nil
}

// SaveEvent 保存调度事件
func (p *MemoryPersistence) SaveEvent(ctx context.Context, event *ScheduleEvent) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// 创建副本
	eventCopy := *event
	p.events = append(p.events, &eventCopy)
	return nil
}

// LoadEvents 加载调度事件
func (p *MemoryPersistence) LoadEvents(ctx context.Context, limit int) ([]*ScheduleEvent, error) {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if limit <= 0 || limit > len(p.events) {
		limit = len(p.events)
	}

	events := make([]*ScheduleEvent, limit)
	// 返回最新的事件
	start := len(p.events) - limit
	for i := 0; i < limit; i++ {
		eventCopy := *p.events[start+i]
		events[i] = &eventCopy
	}

	return events, nil
}