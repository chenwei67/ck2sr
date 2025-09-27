package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ck2sr/ck2sr/internal/config"
)

// SyncEngine 同步引擎
type SyncEngine struct {
	config    *config.Config
	tasks     map[string]*SyncTask
	running   bool
	mutex     sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	waitGroup sync.WaitGroup
}

// NewSyncEngine 创建新的同步引擎
func NewSyncEngine(config *config.Config) *SyncEngine {
	ctx, cancel := context.WithCancel(context.Background())

	return &SyncEngine{
		config: config,
		tasks:  make(map[string]*SyncTask),
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start 启动同步引擎
func (e *SyncEngine) Start() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.running {
		return fmt.Errorf("sync engine is already running")
	}

	// 初始化同步任务
	if err := e.initializeTasks(); err != nil {
		return fmt.Errorf("failed to initialize tasks: %w", err)
	}

	e.running = true

	// 启动各个同步任务
	for _, task := range e.tasks {
		if task.IsEnabled() {
			e.waitGroup.Add(1)
			go e.runTask(task)
		}
	}

	return nil
}

// Stop 停止同步引擎
func (e *SyncEngine) Stop() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if !e.running {
		return nil
	}

	e.running = false
	e.cancel()

	// 等待所有任务完成
	e.waitGroup.Wait()

	return nil
}

// IsRunning 检查引擎是否运行中
func (e *SyncEngine) IsRunning() bool {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return e.running
}

// GetTasks 获取所有任务
func (e *SyncEngine) GetTasks() map[string]*SyncTask {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	tasks := make(map[string]*SyncTask)
	for id, task := range e.tasks {
		tasks[id] = task
	}
	return tasks
}

// GetTask 根据ID获取任务
func (e *SyncEngine) GetTask(taskID string) (*SyncTask, bool) {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	task, exists := e.tasks[taskID]
	return task, exists
}

// AddTask 添加同步任务
func (e *SyncEngine) AddTask(taskConfig *config.SyncTaskConfig) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if _, exists := e.tasks[taskConfig.TaskID]; exists {
		return fmt.Errorf("task with ID %s already exists", taskConfig.TaskID)
	}

	task, err := NewSyncTask(taskConfig, e.config)
	if err != nil {
		return fmt.Errorf("failed to create task %s: %w", taskConfig.TaskID, err)
	}

	e.tasks[taskConfig.TaskID] = task

	// 如果引擎正在运行且任务启用，立即启动任务
	if e.running && task.IsEnabled() {
		e.waitGroup.Add(1)
		go e.runTask(task)
	}

	return nil
}

// RemoveTask 移除同步任务
func (e *SyncEngine) RemoveTask(taskID string) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	task, exists := e.tasks[taskID]
	if !exists {
		return fmt.Errorf("task with ID %s not found", taskID)
	}

	// 停止任务
	if err := task.Stop(); err != nil {
		return fmt.Errorf("failed to stop task %s: %w", taskID, err)
	}

	delete(e.tasks, taskID)
	return nil
}

// ExecuteTask 手动执行单个任务
func (e *SyncEngine) ExecuteTask(taskID string) error {
	e.mutex.RLock()
	task, exists := e.tasks[taskID]
	e.mutex.RUnlock()

	if !exists {
		return fmt.Errorf("task with ID %s not found", taskID)
	}

	return task.Execute(e.ctx)
}

// initializeTasks 初始化所有任务
func (e *SyncEngine) initializeTasks() error {
	tasks := e.config.GetEnabledTasks()

	for _, taskConfig := range tasks {
		task, err := NewSyncTask(&taskConfig, e.config)
		if err != nil {
			return fmt.Errorf("failed to create task %s: %w", taskConfig.TaskID, err)
		}

		e.tasks[taskConfig.TaskID] = task
	}

	return nil
}

// runTask 运行单个任务
func (e *SyncEngine) runTask(task *SyncTask) {
	defer e.waitGroup.Done()

	fmt.Printf("🚀 Starting task runner for: %s\n", task.GetName())

	for {
		select {
		case <-e.ctx.Done():
			fmt.Printf("🛑 Task runner stopped by context cancellation: %s\n", task.GetName())
			return
		default:
			// 检查时间窗口
			if !task.IsInTimeWindow() {
				// 使用带超时的sleep，以便能够响应取消信号
				select {
				case <-e.ctx.Done():
					fmt.Printf("🛑 Task runner stopped during time window wait: %s\n", task.GetName())
					return
				case <-time.After(time.Minute): // 每分钟检查一次时间窗口
					continue
				}
			}

			fmt.Printf("🔄 Executing sync task: %s\n", task.GetName())

			// 执行任务
			if err := task.Execute(e.ctx); err != nil {
				fmt.Printf("❌ Task %s execution failed: %v\n", task.GetID(), err)
			} else {
				fmt.Printf("✅ Task %s execution completed successfully\n", task.GetID())
			}

			// 等待下次执行（这里应该根据任务配置决定间隔）
			// 使用可中断的等待
			select {
			case <-e.ctx.Done():
				fmt.Printf("🛑 Task runner stopped during execution interval: %s\n", task.GetName())
				return
			case <-time.After(time.Hour): // 恢复为每小时执行一次
				// 继续下一轮循环
			}
		}
	}
}

// GetStatus 获取引擎状态
func (e *SyncEngine) GetStatus() *EngineStatus {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	status := &EngineStatus{
		Running:    e.running,
		TaskCount:  len(e.tasks),
		TaskStatus: make(map[string]*TaskStatus),
	}

	for id, task := range e.tasks {
		status.TaskStatus[id] = task.GetStatus()
	}

	return status
}

// EngineStatus 引擎状态
type EngineStatus struct {
	Running    bool                    `json:"running"`
	TaskCount  int                     `json:"task_count"`
	TaskStatus map[string]*TaskStatus  `json:"task_status"`
}