package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/client"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/internal/storage"
)

// TableSyncJob 表级同步作业接口
// 职责：负责单个表的数据同步工作，支持进度持久化和断点续传
type TableSyncJob interface {
	// GetTableName 获取表名
	GetTableName() string

	// Start 启动表同步作业
	Start(ctx context.Context) error

	// Stop 停止表同步作业
	Stop() error

	// GetProgress 获取同步进度
	GetProgress() *TableSyncProgress

	// GetStats 获取统计信息
	GetStats() *TableSyncStats
}

// TableSyncProgress 表同步进度
type TableSyncProgress struct {
	TaskID        string    `json:"task_id"`        // 所属任务ID
	TableName     string    `json:"table_name"`     // 表名
	Offset        int64     `json:"offset"`         // 当前偏移量
	TotalRows     int64     `json:"total_rows"`     // 总行数（估算）
	ProcessedRows int64     `json:"processed_rows"` // 已处理行数
	LastSyncTime  time.Time `json:"last_sync_time"` // 最后同步时间
	Status        string    `json:"status"`         // 同步状态: pending, running, completed, failed
}

// TableSyncStats 表同步统计
type TableSyncStats struct {
	TableName      string        `json:"table_name"`
	StartTime      time.Time     `json:"start_time"`
	EndTime        time.Time     `json:"end_time"`
	Duration       time.Duration `json:"duration"`
	ProcessedRows  int64         `json:"processed_rows"`
	ProcessedBytes int64         `json:"processed_bytes"`
	ErrorCount     int64         `json:"error_count"`
	Status         string        `json:"status"`
}

// DefaultTableSyncJob 默认的表同步作业实现
type DefaultTableSyncJob struct {
	taskID    string                   // 所属任务ID
	tableName string                   // 表名，源表
	dstTable  string                   // 目标表名
	config    *config.SyncTaskConfig   // 任务配置
	policy    *config.PolicyConfig     // 策略配置
	ckCliMgr  *client.ClickHouseCliMgr // ClickHouse客户端管理器
	srCliMgr  *client.StarRocksCliMgr  // StarRocks客户端管理器
	storage   storage.Storage          // 存储接口
	logger    *logrus.Logger           // 日志记录器
	pipeline  *Pipeline                // 数据管道
	progress  *TableSyncProgress       // 同步进度
	stats     *TableSyncStats          // 统计信息
	mu        sync.RWMutex             // 读写锁
	ctx       context.Context          // 上下文
	cancel    context.CancelFunc       // 取消函数
}

// NewTableSyncJob 创建新的表同步作业
func NewTableSyncJob(taskID, tableName string, dstTable string,
	taskConfig *config.SyncTaskConfig,
	policy *config.PolicyConfig,
	ckCliMgr *client.ClickHouseCliMgr,
	srCliMgr *client.StarRocksCliMgr,
	store storage.Storage,
	logger *logrus.Logger) TableSyncJob {
	// 为该表创建专用的日志记录器
	tableLogger := logger.WithFields(logrus.Fields{
		"task_id": taskID,
		"table":   tableName,
	}).Logger

	job := &DefaultTableSyncJob{
		taskID:    taskID,
		tableName: tableName,
		dstTable:  dstTable,
		config:    taskConfig,
		policy:    policy,
		ckCliMgr:  ckCliMgr,
		srCliMgr:  srCliMgr,
		storage:   store,
		logger:    tableLogger,
		progress: &TableSyncProgress{
			TaskID:        taskID,
			TableName:     tableName,
			Offset:        0,
			TotalRows:     0,
			ProcessedRows: 0,
			LastSyncTime:  time.Time{},
			Status:        "pending",
		},
		stats: &TableSyncStats{
			TableName:      tableName,
			StartTime:      time.Time{},
			EndTime:        time.Time{},
			ProcessedRows:  0,
			ProcessedBytes: 0,
			ErrorCount:     0,
			Status:         "pending",
		},
	}

	return job
}

// GetTableName 获取表名
func (j *DefaultTableSyncJob) GetTableName() string {
	return j.tableName
}

// Start 启动表同步作业
func (j *DefaultTableSyncJob) Start(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	// 创建带取消的上下文
	j.ctx, j.cancel = context.WithCancel(ctx)

	j.logger.Infof("Starting table sync job for %s", j.tableName)

	// 更新状态为运行中
	j.progress.Status = "running"
	j.stats.Status = "running"
	j.stats.StartTime = time.Now()

	// 从存储中加载上次的同步进度
	if err := j.loadProgress(); err != nil {
		j.logger.Warnf("Failed to load progress for table %s: %v, starting from beginning", j.tableName, err)
		j.progress.Offset = 0
	} else {
		j.logger.Infof("Loaded progress for table %s: offset=%d, processed_rows=%d",
			j.tableName, j.progress.Offset, j.progress.ProcessedRows)
	}

	// 创建数据管道
	j.pipeline = NewPipeline(j.config, j.policy, j.ckCliMgr, j.srCliMgr, j.tableName, j.dstTable, j.logger)
	if err := j.pipeline.Initialize(j.ctx, j.progress.Offset); err != nil {
		return fmt.Errorf("failed to initialize pipeline for table %s: %w", j.tableName, err)
	}

	// 在独立的goroutine中执行同步
	go func() {
		defer func() {
			if r := recover(); r != nil {
				j.logger.Errorf("Table sync job panic for %s: %v", j.tableName, r)
				j.handleError(fmt.Errorf("panic: %v", r))
			}
		}()

		if err := j.syncTable(); err != nil {
			j.logger.Errorf("Table sync failed for %s: %v", j.tableName, err)
			j.handleError(err)
		} else {
			j.handleSuccess()
		}
	}()

	return nil
}

// Stop 停止表同步作业
func (j *DefaultTableSyncJob) Stop() error {
	j.mu.Lock()
	defer j.mu.Unlock()

	if j.cancel != nil {
		j.logger.Infof("Stopping table sync job for %s", j.tableName)
		j.cancel()
	}

	return nil
}

// GetProgress 获取同步进度
func (j *DefaultTableSyncJob) GetProgress() *TableSyncProgress {
	j.mu.RLock()
	defer j.mu.RUnlock()

	// 创建副本以避免并发访问问题
	progress := *j.progress
	return &progress
}

// GetStats 获取统计信息
func (j *DefaultTableSyncJob) GetStats() *TableSyncStats {
	j.mu.RLock()
	defer j.mu.RUnlock()

	// 创建副本并计算当前持续时间
	stats := *j.stats
	if !stats.StartTime.IsZero() && stats.Status == "running" {
		stats.Duration = time.Since(stats.StartTime)
	}
	return &stats
}

// loadProgress 从存储中加载同步进度
func (j *DefaultTableSyncJob) loadProgress() error {
	progressKey := fmt.Sprintf("%s_%s", j.taskID, j.tableName)
	syncProgress, err := j.storage.LoadSyncProgress(context.Background(), j.taskID, progressKey)
	if err != nil {
		return err
	}

	if syncProgress != nil {
		j.progress.Offset = syncProgress.SyncedRows // 使用已同步行数作为偏移量
		j.progress.ProcessedRows = syncProgress.SyncedRows
		j.progress.TotalRows = syncProgress.TotalRows
		j.progress.LastSyncTime = syncProgress.LastSyncTime
	}

	return nil
}

// saveProgress 保存同步进度到存储
func (j *DefaultTableSyncJob) saveProgress() error {
	syncProgress := &storage.SyncProgress{
		TaskID:       j.taskID,
		SourceTable:  j.tableName,
		TargetTable:  j.dstTable,
		TotalRows:    j.progress.TotalRows,
		SyncedRows:   j.progress.ProcessedRows,
		SyncedBytes:  j.stats.ProcessedBytes,
		LastSyncTime: time.Now(),
		Progress:     float64(j.progress.ProcessedRows) / float64(j.progress.TotalRows) * 100,
	}

	return j.storage.SaveSyncProgress(context.Background(), syncProgress)
}

// syncTable 执行表同步逻辑
func (j *DefaultTableSyncJob) syncTable() error {
	j.logger.Infof("Starting table synchronization for %s from offset %d", j.tableName, j.progress.Offset)

	// 使用Pipeline处理表数据（传入当前offset支持后续优化）
	if err := j.pipeline.Process(j.ctx, j.tableName); err != nil {
		return fmt.Errorf("pipeline processing failed: %w", err)
	}

	// 获取Pipeline的统计信息并更新本地统计
	pipelineStats := j.pipeline.GetStats()
	j.mu.Lock()
	j.stats.ProcessedRows = pipelineStats.TotalRows
	j.stats.ProcessedBytes = pipelineStats.TotalBytes
	j.progress.ProcessedRows += pipelineStats.TotalRows
	j.mu.Unlock()

	// 定期保存进度
	if err := j.saveProgress(); err != nil {
		j.logger.Warnf("Failed to save progress for table %s: %v", j.tableName, err)
	}

	j.logger.Infof("Table synchronization completed for %s, processed %d rows",
		j.tableName, j.stats.ProcessedRows)

	return nil
}

// handleError 处理同步错误
func (j *DefaultTableSyncJob) handleError(err error) {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.progress.Status = "failed"
	j.stats.Status = "failed"
	j.stats.EndTime = time.Now()
	j.stats.Duration = j.stats.EndTime.Sub(j.stats.StartTime)
	j.stats.ErrorCount++

	// 尝试保存错误状态
	if saveErr := j.saveProgress(); saveErr != nil {
		j.logger.Errorf("Failed to save error progress for table %s: %v", j.tableName, saveErr)
	}

	j.logger.Errorf("Table sync job failed for %s: %v", j.tableName, err)
}

// handleSuccess 处理同步成功
func (j *DefaultTableSyncJob) handleSuccess() {
	j.mu.Lock()
	defer j.mu.Unlock()

	j.progress.Status = "completed"
	j.progress.LastSyncTime = time.Now()
	j.stats.Status = "completed"
	j.stats.EndTime = time.Now()
	j.stats.Duration = j.stats.EndTime.Sub(j.stats.StartTime)

	// 保存最终进度
	if err := j.saveProgress(); err != nil {
		j.logger.Errorf("Failed to save final progress for table %s: %v", j.tableName, err)
	}

	j.logger.Infof("Table sync job completed successfully for %s in %v, processed %d rows, %d bytes",
		j.tableName, j.stats.Duration, j.stats.ProcessedRows, j.stats.ProcessedBytes)
}
