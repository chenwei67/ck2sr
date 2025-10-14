package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/client"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/internal/scheduler"
	"github.com/sunkaimr/ck2sr/internal/storage"
)

// TaskStatus 任务状态类型
type TaskStatus string

const (
	TaskStatusIdle      TaskStatus = "idle"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// SyncTask 同步任务
// 职责：管理多表并发同步，使用TableSyncJob实现单表同步
type SyncTask struct {
	config  *config.SyncTaskConfig
	policy  *config.PolicyConfig
	storage storage.Storage
	logger  *logrus.Logger
	status  TaskStatus

	// 客户端
	ckCliMgr *client.ClickHouseCliMgr
	srCliMgr *client.StarRocksCliMgr

	// jobs 管理多个表的同步作业，key为表名
	jobs   map[string]TableSyncJob
	jobsMu sync.RWMutex

	// 控制并发和取消
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewSyncTask 创建新的同步任务
func NewSyncTask(cfg *config.SyncTaskConfig,
	policy *config.PolicyConfig,
	store storage.Storage,
	ckCliMgr *client.ClickHouseCliMgr,
	srCliMgr *client.StarRocksCliMgr,
	logger *logrus.Logger) (*SyncTask, error) {
	ctx, cancel := context.WithCancel(context.Background())

	task := &SyncTask{
		config:   cfg,
		policy:   policy,
		storage:  store,
		ckCliMgr: ckCliMgr,
		srCliMgr: srCliMgr,
		logger:   logger,
		status:   TaskStatusIdle,
		jobs:     make(map[string]TableSyncJob),
		ctx:      ctx,
		cancel:   cancel,
	}

	// 为每个表创建TableSyncJob
	task.initializeTableJobs()

	return task, nil
}

// initializeTableJobs 初始化表同步作业
func (t *SyncTask) initializeTableJobs() {
	t.jobsMu.Lock()
	defer t.jobsMu.Unlock()

	// 确保Reader和Writer的表数量一致
	for i, tableName := range t.config.Reader.Tables {
		srcTable := tableName
		dstTable := t.config.Writer.Tables[i] // 目标表名与源表名一一对应
		job := NewTableSyncJob(t.config.TaskID, srcTable, dstTable, t.config, t.policy, t.ckCliMgr, t.srCliMgr, t.storage, t.logger)
		t.jobs[srcTable] = job
		t.logger.Infof("Created TableSyncJob from table(%s) to table(%s)", srcTable, dstTable)
	}
}

// GetStatus 获取任务状态
func (t *SyncTask) GetStatus() TaskStatus {
	return t.status
}

func (t *SyncTask) GetTaskID() string {
	return t.config.TaskID
}

func (t *SyncTask) GetTaskConfig() *config.SyncTaskConfig {
	return t.config
}

func (t *SyncTask) GetID() string {
	return t.config.TaskID
}

func (t *SyncTask) GetName() string {
	return t.config.Name
}

func (t *SyncTask) IsEnabled() bool {
	return t.config.Enabled
}

func (t *SyncTask) IsInTimeWindow() bool {
	return true
}

func (t *SyncTask) Stop() error {
	// 取消所有作业
	if t.cancel != nil {
		t.cancel()
	}

	// 停止所有TableSyncJob
	t.jobsMu.RLock()
	defer t.jobsMu.RUnlock()

	for tableName, job := range t.jobs {
		if err := job.Stop(); err != nil {
			t.logger.Errorf("Failed to stop job for table %s: %v", tableName, err)
		}
	}

	// 等待所有goroutine完成
	t.wg.Wait()

	t.status = TaskStatusCancelled
	t.logger.Infof("Sync task %s stopped", t.config.TaskID)
	return nil
}

// GetTableJob 获取指定表的同步作业
func (t *SyncTask) GetTableJob(tableName string) (TableSyncJob, bool) {
	t.jobsMu.RLock()
	defer t.jobsMu.RUnlock()
	job, exists := t.jobs[tableName]
	return job, exists
}

// GetAllTableJobs 获取所有表的同步作业
func (t *SyncTask) GetAllTableJobs() map[string]TableSyncJob {
	t.jobsMu.RLock()
	defer t.jobsMu.RUnlock()

	result := make(map[string]TableSyncJob, len(t.jobs))
	for tableName, job := range t.jobs {
		result[tableName] = job
	}
	return result
}

// GetTableProgress 获取指定表的同步进度
func (t *SyncTask) GetTableProgress(tableName string) *TableSyncProgress {
	if job, exists := t.GetTableJob(tableName); exists {
		return job.GetProgress()
	}
	return nil
}

// GetAllTableProgress 获取所有表的同步进度
func (t *SyncTask) GetAllTableProgress() map[string]*TableSyncProgress {
	t.jobsMu.RLock()
	defer t.jobsMu.RUnlock()

	result := make(map[string]*TableSyncProgress)
	for tableName, job := range t.jobs {
		result[tableName] = job.GetProgress()
	}
	return result
}

// logSummaryStats 记录总体统计信息
func (t *SyncTask) logSummaryStats(successCount, totalTables int) {
	var totalRows, totalBytes int64
	var totalDuration time.Duration
	// 同步记录最晚结束时间和最早的开始时间来统计总耗时
	var earliestStart, latestEnd time.Time

	t.jobsMu.RLock()
	for _, job := range t.jobs {
		stats := job.GetProgress()
		totalRows += stats.ProcessedRows
		totalBytes += stats.ProcessedBytes
		if stats.StartTime.Before(earliestStart) || earliestStart.IsZero() {
			earliestStart = stats.StartTime
		}
		if stats.EndTime.After(latestEnd) || latestEnd.IsZero() {
			latestEnd = stats.EndTime
		}
	}
	t.jobsMu.RUnlock()

	// 计算总耗时
	totalDuration = latestEnd.Sub(earliestStart)

	t.logger.Infof("=== Task Summary ===")
	t.logger.Infof("  Task ID: %s", t.config.TaskID)
	t.logger.Infof("  Task Name: %s", t.config.Name)
	t.logger.Infof("  Success Rate: %d/%d tables", successCount, totalTables)
	t.logger.Infof("  Total sync Rows: %d", totalRows)
	t.logger.Infof("  Total sync Bytes: %d", totalBytes)
	t.logger.Infof("  Total Duration: %v", totalDuration)
	if totalDuration.Seconds() > 0 {
		rowsPerSec := float64(totalRows) / totalDuration.Seconds()
		bytesPerSec := float64(totalBytes) / totalDuration.Seconds()
		t.logger.Infof("  Performance: %.2f rows/sec, %.2f bytes/sec", rowsPerSec, bytesPerSec)
	}
}

func (t *SyncTask) Execute(ctx context.Context) error {
	if !t.config.Enabled {
		t.logger.Infof("Task %s is disabled, skipping", t.config.TaskID)
		return nil
	}
	t.status = TaskStatusRunning

	t.logger.Infof("Starting sync task: %s", t.config.TaskID)
	t.logTaskConfiguration()

	// 并发执行表同步作业
	errChan := make(chan error, len(t.jobs))
	successCount := 0
	var lastError error

	t.jobsMu.RLock()
	totalTables := len(t.jobs)

	// 根据配置限制并发数
	maxConcurrent := t.config.Settings.ParallelTables
	if maxConcurrent <= 0 {
		maxConcurrent = 1 // 默认串行执行
	}

	semaphore := make(chan struct{}, maxConcurrent)
	t.jobsMu.RUnlock()

	// 启动所有表的同步作业
	for tableName, job := range t.jobs {
		t.wg.Add(1)
		go func(table string, syncJob TableSyncJob) {
			defer t.wg.Done()

			// 获取信号量
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// 创建带超时的执行上下文
			execCtx, execCancel := context.WithCancel(ctx)
			defer execCancel()

			// 执行表同步
			if err := syncJob.Start(execCtx); err != nil {
				t.logger.Errorf("Failed to start sync for table %s: %v", table, err)
				errChan <- fmt.Errorf("table %s: %w", table, err)
				return
			}
			// 同步成功需要返回 nil
			errChan <- nil
		}(tableName, job)
	}

	// 等待所有作业完成
	go func() {
		t.wg.Wait()
		close(errChan)
	}()

	// 阻塞式收集结果
	for err := range errChan {
		if err != nil {
			lastError = err
		} else {
			successCount++
		}
	}

	// 记录总体统计
	t.logSummaryStats(successCount, totalTables)

	// 判断任务是否失败
	if successCount != totalTables {
		t.status = TaskStatusFailed
		return fmt.Errorf("some tables sync failed, success: %d/%d, last error: %v", successCount, totalTables, lastError)
	}

	// 全部成功
	t.status = TaskStatusCompleted
	return nil
}

func (t *SyncTask) logTaskConfiguration() {
	t.logger.Infof("=== Task Configuration ===")
	t.logger.Infof("  Task ID: %s", t.config.TaskID)
	t.logger.Infof("  Task Name: %s", t.config.Name)
	t.logger.Infof("  Reader: %s (%s)", t.config.Reader.Vendor, t.config.Reader.Protocol)
	t.logger.Infof("  Writer: %s (%s)", t.config.Writer.Vendor, t.config.Writer.Protocol)
	t.logger.Infof("  Database: %s", t.config.Reader.Database)
	t.logger.Infof("  Tables: %v", t.config.Reader.Tables)
	t.logger.Infof("  Batch Size: %d", t.config.Settings.BatchSize)
	t.logger.Infof("  Batch Bytes: %d", t.config.Settings.BatchBytes)
	t.logger.Infof("  Batch Interval: %v", t.config.Settings.BatchInterval)
	t.logger.Infof("  Parallel Tables: %d", t.config.Settings.ParallelTables)
	if t.config.Settings.DataRange.StartTime != "" || t.config.Settings.DataRange.EndTime != "" {
		t.logger.Infof("  Data Range: %s - %s",
			t.config.Settings.DataRange.StartTime, t.config.Settings.DataRange.EndTime)
	}
}

var _ scheduler.TaskExecutor = (*SyncTask)(nil)
