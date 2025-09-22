package tests

import (
	"context"
	"testing"
	"time"
)

func TestTaskState(t *testing.T) {
	state := &TaskState{
		TaskID:         "test_task",
		Status:         TaskStatusRunning,
		TotalRows:      1000,
		ProcessedRows:  250,
		TotalBytes:     1024000,
		ProcessedBytes: 256000,
	}

	// 测试进度计算
	progress := state.GetProgress()
	if progress.Percentage != 25.0 {
		t.Errorf("Expected progress 25%%, got %.1f%%", progress.Percentage)
	}

	// 测试状态检查
	if !state.IsRunning() {
		t.Error("State should be running")
	}

	if state.IsFinished() {
		t.Error("Running state should not be finished")
	}

	if state.CanResume() {
		t.Error("Running state should not be resumable")
	}

	// 测试暂停状态
	state.Status = TaskStatusPaused
	if state.IsRunning() {
		t.Error("Paused state should not be running")
	}

	if !state.CanResume() {
		t.Error("Paused state should be resumable")
	}

	// 测试完成状态
	state.Status = TaskStatusCompleted
	if !state.IsFinished() {
		t.Error("Completed state should be finished")
	}
}

func TestFileStorage(t *testing.T) {
	tempDir := t.TempDir()
	storage, err := NewFileStorage(tempDir, nil)
	if err != nil {
		t.Fatalf("Failed to create file storage: %v", err)
	}

	ctx := context.Background()

	// 初始化存储
	err = storage.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize storage: %v", err)
	}

	// 创建测试任务状态
	state := &TaskState{
		TaskID:         "test_task",
		Status:         TaskStatusRunning,
		TotalRows:      1000,
		ProcessedRows:  250,
		TotalBytes:     1024000,
		ProcessedBytes: 256000,
		UpdateTime:     time.Now(),
	}

	// 保存任务状态
	err = storage.SaveTaskState(ctx, state)
	if err != nil {
		t.Fatalf("Failed to save task state: %v", err)
	}

	// 获取任务状态
	retrievedState, err := storage.GetTaskState(ctx, "test_task")
	if err != nil {
		t.Fatalf("Failed to get task state: %v", err)
	}

	if retrievedState.TaskID != state.TaskID {
		t.Errorf("Expected task ID %s, got %s", state.TaskID, retrievedState.TaskID)
	}

	if retrievedState.ProcessedRows != state.ProcessedRows {
		t.Errorf("Expected processed rows %d, got %d", state.ProcessedRows, retrievedState.ProcessedRows)
	}

	// 更新进度
	err = storage.UpdateProgress(ctx, "test_task", 500, 512000)
	if err != nil {
		t.Fatalf("Failed to update progress: %v", err)
	}

	updatedState, err := storage.GetTaskState(ctx, "test_task")
	if err != nil {
		t.Fatalf("Failed to get updated task state: %v", err)
	}

	if updatedState.ProcessedRows != 500 {
		t.Errorf("Expected updated processed rows 500, got %d", updatedState.ProcessedRows)
	}

	// 保存检查点
	checkpoint := map[string]interface{}{
		"offset":    int64(100),
		"timestamp": time.Now().Unix(),
	}

	err = storage.SaveCheckpoint(ctx, "test_task", checkpoint)
	if err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	// 获取检查点
	retrievedCheckpoint, err := storage.GetCheckpoint(ctx, "test_task")
	if err != nil {
		t.Fatalf("Failed to get checkpoint: %v", err)
	}

	if retrievedCheckpoint["offset"] != float64(100) { // JSON 解析会转为 float64
		t.Errorf("Expected checkpoint offset 100, got %v", retrievedCheckpoint["offset"])
	}

	// 获取所有任务状态
	allStates, err := storage.GetAllTaskStates(ctx)
	if err != nil {
		t.Fatalf("Failed to get all task states: %v", err)
	}

	if len(allStates) != 1 {
		t.Errorf("Expected 1 task state, got %d", len(allStates))
	}

	// 按状态获取任务
	runningTasks, err := storage.GetTasksByStatus(ctx, TaskStatusRunning)
	if err != nil {
		t.Fatalf("Failed to get tasks by status: %v", err)
	}

	if len(runningTasks) != 1 {
		t.Errorf("Expected 1 running task, got %d", len(runningTasks))
	}

	// 删除任务状态
	err = storage.DeleteTaskState(ctx, "test_task")
	if err != nil {
		t.Fatalf("Failed to delete task state: %v", err)
	}

	// 验证删除
	_, err = storage.GetTaskState(ctx, "test_task")
	if err == nil {
		t.Error("Expected error when getting deleted task state")
	}

	// 关闭存储
	err = storage.Close()
	if err != nil {
		t.Fatalf("Failed to close storage: %v", err)
	}
}

func TestMemoryPersistence(t *testing.T) {
	persistence := NewMemoryPersistence()
	ctx := context.Background()

	// 初始化
	err := persistence.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize persistence: %v", err)
	}

	// 创建测试调度
	schedule := &Schedule{
		ID:          "test_schedule",
		TaskID:      "test_task",
		Type:        ScheduleTypeCron,
		Expression:  "0 2 * * *",
		Status:      ScheduleStatusActive,
		RunCount:    0,
		MaxRuns:     0,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	// 保存调度
	err = persistence.SaveSchedule(ctx, schedule)
	if err != nil {
		t.Fatalf("Failed to save schedule: %v", err)
	}

	// 加载调度
	loadedSchedule, err := persistence.LoadSchedule(ctx, "test_schedule")
	if err != nil {
		t.Fatalf("Failed to load schedule: %v", err)
	}

	if loadedSchedule.ID != schedule.ID {
		t.Errorf("Expected schedule ID %s, got %s", schedule.ID, loadedSchedule.ID)
	}

	// 加载所有调度
	allSchedules, err := persistence.LoadAllSchedules(ctx)
	if err != nil {
		t.Fatalf("Failed to load all schedules: %v", err)
	}

	if len(allSchedules) != 1 {
		t.Errorf("Expected 1 schedule, got %d", len(allSchedules))
	}

	// 保存事件
	event := &ScheduleEvent{
		Type:       "test_event",
		ScheduleID: "test_schedule",
		TaskID:     "test_task",
		Timestamp:  time.Now(),
		Message:    "Test event",
	}

	err = persistence.SaveEvent(ctx, event)
	if err != nil {
		t.Fatalf("Failed to save event: %v", err)
	}

	// 加载事件
	events, err := persistence.LoadEvents(ctx, 10)
	if err != nil {
		t.Fatalf("Failed to load events: %v", err)
	}

	if len(events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(events))
	}

	if events[0].Type != event.Type {
		t.Errorf("Expected event type %s, got %s", event.Type, events[0].Type)
	}

	// 删除调度
	err = persistence.DeleteSchedule(ctx, "test_schedule")
	if err != nil {
		t.Fatalf("Failed to delete schedule: %v", err)
	}

	// 验证删除
	_, err = persistence.LoadSchedule(ctx, "test_schedule")
	if err == nil {
		t.Error("Expected error when loading deleted schedule")
	}

	// 关闭
	err = persistence.Close()
	if err != nil {
		t.Fatalf("Failed to close persistence: %v", err)
	}
}

func TestStorageFactory(t *testing.T) {
	factory := NewStorageFactory(nil)

	// 测试支持的类型
	supportedTypes := factory.GetSupportedTypes()
	if len(supportedTypes) == 0 {
		t.Error("Factory should support at least one storage type")
	}

	// 测试创建文件存储
	fileConfig := map[string]interface{}{
		"path": t.TempDir(),
	}

	fileStorage, err := factory.CreateStorage(StorageTypeFile, fileConfig)
	if err != nil {
		t.Fatalf("Failed to create file storage: %v", err)
	}

	if fileStorage == nil {
		t.Error("File storage should not be nil")
	}

	// 测试验证配置
	err = factory.ValidateConfig(StorageTypeFile, fileConfig)
	if err != nil {
		t.Errorf("File storage config should be valid: %v", err)
	}

	// 测试无效配置
	invalidConfig := map[string]interface{}{}
	err = factory.ValidateConfig(StorageTypeFile, invalidConfig)
	if err == nil {
		t.Error("Invalid config should return error")
	}

	// 测试不支持的类型
	_, err = factory.CreateStorage("unsupported", fileConfig)
	if err == nil {
		t.Error("Unsupported storage type should return error")
	}
}

// 定义测试用的 Schedule 和 ScheduleEvent 结构体（如果不在 storage 包中）
type Schedule struct {
	ID          string    `json:"id"`
	TaskID      string    `json:"task_id"`
	Type        string    `json:"type"`
	Expression  string    `json:"expression"`
	Status      string    `json:"status"`
	NextRunTime *time.Time `json:"next_run_time,omitempty"`
	LastRunTime *time.Time `json:"last_run_time,omitempty"`
	RunCount    int64     `json:"run_count"`
	MaxRuns     int64     `json:"max_runs"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type ScheduleEvent struct {
	Type       string    `json:"type"`
	ScheduleID string    `json:"schedule_id"`
	TaskID     string    `json:"task_id"`
	Timestamp  time.Time `json:"timestamp"`
	Message    string    `json:"message"`
	Error      string    `json:"error,omitempty"`
}

const (
	ScheduleTypeCron   = "cron"
	ScheduleStatusActive = "active"
)