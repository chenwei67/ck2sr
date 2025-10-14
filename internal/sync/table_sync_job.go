package sync

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/client"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/internal/storage"
	"github.com/sunkaimr/ck2sr/pkg/utils"
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
	TaskID           string    `json:"task_id"`           // 所属任务ID
	TableName        string    `json:"table_name"`        // 表名
	Offset           int64     `json:"offset"`            // 当前偏移量
	TotalRows        int64     `json:"total_rows"`        // 总行数（估算）
	ProcessedBatches int64     `json:"processed_batches"` // 已处理批次数
	ProcessedRows    int64     `json:"processed_rows"`    // 已处理行数
	ProcessedBytes   int64     `json:"processed_bytes"`   // 已处理字节数
	LastSyncTime     time.Time `json:"last_sync_time"`    // 最后同步时间
	Status           string    `json:"status"`            // 同步状态: pending, running, completed, failed
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
	// mu        sync.RWMutex             // 读写锁，是想保护哪个数据？
	ctx    context.Context    // 上下文
	cancel context.CancelFunc // 取消函数
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
	// 创建带取消的上下文
	j.ctx, j.cancel = context.WithCancel(ctx)

	j.logger.Infof("Starting sync job for table: %s", j.tableName)

	// 更新状态为运行中
	j.progress.Status = "running"
	j.stats.Status = "running"
	// 仅在新任务时设置StartTime，断点续传时保留原有时间
	if j.stats.StartTime.IsZero() {
		j.stats.StartTime = time.Now()
	}

	// 从存储中加载上次的同步进度
	progressLoaded := false
	if err := j.loadProgress(); err != nil {
		j.logger.Infof("No previous progress found for table %s, starting from beginning (reason: %v)", j.tableName, err)
		j.progress.Offset = 0
		j.progress.ProcessedRows = 0
	} else {
		// 成功加载了进度数据
		progressLoaded = true
		j.logger.Infof("Loaded progress for table %s: offset=%d, processed_rows=%d, total_rows=%d",
			j.tableName, j.progress.Offset, j.progress.ProcessedRows, j.progress.TotalRows)

		// 幂等性检查：如果表已经完成同步，直接返回成功
		if j.progress.Status == "completed" || (j.progress.TotalRows > 0 && j.progress.ProcessedRows >= j.progress.TotalRows) {
			j.logger.Infof("Table %s already completed (processed_rows=%d, total_rows=%d, status=%s), skipping resync",
				j.tableName, j.progress.ProcessedRows, j.progress.TotalRows, j.progress.Status)
			// 确保状态设置为 completed（同步操作，不使用 goroutine）
			j.progress.Status = "completed"
			j.stats.Status = "completed"
			if j.stats.EndTime.IsZero() {
				j.stats.EndTime = time.Now()
				j.stats.Duration = j.stats.EndTime.Sub(j.stats.StartTime)
			}
			// 保存最终状态（同步操作）
			if err := j.saveProgress(); err != nil {
				j.logger.Warnf("Failed to save completed status for table %s: %v", j.tableName, err)
			}
			j.logger.Infof("Table %s status already completed, no sync needed", j.tableName)
			return nil
		}
	}

	// 查询本次需要同步的总行数（仅在没有加载进度或进度中没有总行数时查询）
	if !progressLoaded || j.progress.TotalRows == 0 {
		totalRows, err := j.queryTotalRows()
		if err != nil {
			j.logger.Warnf("Failed to query total rows for table %s: %v, will continue without total count", j.tableName, err)
			return err
		} else {
			j.progress.TotalRows = totalRows
			j.logger.Infof("Total rows to sync for table %s: %d", j.tableName, totalRows)
		}
	}

	// 创建数据管道
	j.pipeline = NewPipeline(j.config, j.policy, j.ckCliMgr, j.srCliMgr, j.tableName, j.dstTable, j.logger)
	if err := j.pipeline.Initialize(j.ctx, j.progress.Offset, j.progress.TotalRows); err != nil {
		return fmt.Errorf("failed to initialize pipeline for table %s: %w", j.tableName, err)
	}

	// 设置进度回调函数，每次批次写入完成后调用
	j.pipeline.SetProgressCallback(func(progress ProgressInfo) {
		j.onBatchProgress(progress)
	})

	// 保存初始进度状态（仅在新任务时保存，断点续传时不覆盖）
	if !progressLoaded {
		if err := j.saveInitialProgress(); err != nil {
			j.logger.Warnf("Failed to save initial progress for table %s: %v", j.tableName, err)
		}
	}

	// 启动同步过程（同步操作，不使用 goroutine）
	if err := j.syncTable(); err != nil {
		j.logger.Errorf("Failed sync job for table %s: %v", j.tableName, err)
		j.handleError(err)
		return err
	} else {
		j.handleSuccess()
	}

	// 同步成功
	j.logger.Infof("Success sync job for table: %s", j.tableName)
	return nil
}

// Stop 停止表同步作业
func (j *DefaultTableSyncJob) Stop() error {
	if j.cancel != nil {
		j.logger.Infof("Stopping table sync job for %s", j.tableName)
		j.cancel()
	}

	return nil
}

// GetProgress 获取同步进度
func (j *DefaultTableSyncJob) GetProgress() *TableSyncProgress {
	// 创建副本以避免并发访问问题
	progress := *j.progress
	return &progress
}

// GetStats 获取统计信息
func (j *DefaultTableSyncJob) GetStats() *TableSyncStats {
	// 创建副本并计算当前持续时间
	stats := *j.stats
	if !stats.StartTime.IsZero() && stats.Status == "running" {
		stats.Duration = time.Since(stats.StartTime)
	}
	return &stats
}

// queryTotalRows 查询本次需要同步的总行数
// 根据任务配置的数据范围条件构建COUNT查询
func (j *DefaultTableSyncJob) queryTotalRows() (int64, error) {
	// 构建COUNT查询语句,与Pipeline中的查询条件保持一致
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", j.config.Reader.Database, j.tableName)

	// 添加时间范围条件（与Pipeline.Initialize中的逻辑保持一致）
	if j.config.Settings.DataRange.TimeColumn != "" {
		if j.config.Settings.DataRange.StartTime != "" {
			query += fmt.Sprintf(" WHERE %s >= '%s'", j.config.Settings.DataRange.TimeColumn, j.config.Settings.DataRange.StartTime)
		}

		if j.config.Settings.DataRange.EndTime != "" {
			if j.config.Settings.DataRange.StartTime != "" {
				query += fmt.Sprintf(" AND %s < '%s'", j.config.Settings.DataRange.TimeColumn, j.config.Settings.DataRange.EndTime)
			} else {
				query += fmt.Sprintf(" WHERE %s < '%s'", j.config.Settings.DataRange.TimeColumn, j.config.Settings.DataRange.EndTime)
			}
		}
	}

	j.logger.Debugf("Query total rows: %s", query)

	// 根据reader的协议类型选择合适的客户端执行COUNT查询
	switch j.config.Reader.Protocol {
	case "mysql":
		switch j.config.Reader.Vendor {
		case "clickhouse":
			cli, err := j.ckCliMgr.GetMySQLClient(j.config.Reader.Name)
			if err != nil {
				return 0, fmt.Errorf("failed to get MySQL client: %w", err)
			}
			return cli.Count(context.Background(), query)
		case "starrocks":
			cli, err := j.srCliMgr.GetMySQLClient(j.config.Reader.Name)
			if err != nil {
				return 0, fmt.Errorf("failed to get MySQL client: %w", err)
			}
			return cli.Count(context.Background(), query)
		default:
			return 0, fmt.Errorf("unsupported vendor: %s", j.config.Reader.Vendor)
		}
	default:
		return 0, fmt.Errorf("unsupported protocol: %s", j.config.Reader.Protocol)
	}
}

// saveInitialProgress 保存初始进度状态
// 在任务开始时调用，记录开始时间、表信息、需要同步的总行数等
func (j *DefaultTableSyncJob) saveInitialProgress() error {
	syncProgress := &storage.SyncProgress{
		TaskID:        j.taskID,
		Status:        j.stats.Status,
		SourceDB:      j.config.Reader.Database, // 源数据库
		SourceTable:   j.tableName,
		TargetDB:      j.config.Writer.Database, // 目标数据库
		TargetTable:   j.dstTable,
		TotalRows:     j.progress.TotalRows,
		SyncedRows:    0, // 初始时未同步任何数据
		SyncedBytes:   0,
		StartSyncTime: time.Now(),
		LastSyncTime:  time.Now(),
		Progress:      0.0,
	}

	return j.storage.SaveSyncProgress(context.Background(), syncProgress)
}

// loadProgress 从存储中加载同步进度
func (j *DefaultTableSyncJob) loadProgress() error {
	syncProgress, err := j.storage.LoadSyncProgress(context.Background(), j.taskID, j.tableName)
	if err != nil {
		return err
	}

	// 如果没有加载到进度数据，返回特殊错误以区分加载失败和无进度
	if syncProgress == nil {
		return fmt.Errorf("no progress data found")
	}

	j.progress.Offset = syncProgress.SyncedRows // 使用已同步行数作为偏移量
	j.progress.ProcessedRows = syncProgress.SyncedRows
	j.progress.ProcessedBytes = syncProgress.SyncedBytes
	j.progress.TotalRows = syncProgress.TotalRows
	j.progress.LastSyncTime = syncProgress.LastSyncTime
	// 从存储中恢复StartSyncTime
	j.stats.StartTime = syncProgress.StartSyncTime

	// 根据进度数据判断状态
	if syncProgress.Progress >= 100.0 || (syncProgress.TotalRows > 0 && syncProgress.SyncedRows >= syncProgress.TotalRows) {
		j.progress.Status = "completed"
	} else if syncProgress.SyncedRows > 0 {
		j.progress.Status = "running" // 有进度但未完成，视为中断后待恢复
	} else {
		j.progress.Status = "pending"
	}

	return nil
}

// saveProgress 保存同步进度到存储
func (j *DefaultTableSyncJob) saveProgress() error {
	// 计算进度百分比，保留2位小数
	progress := 0.0
	if j.progress.TotalRows > 0 {
		rawProgress := float64(j.progress.ProcessedRows) / float64(j.progress.TotalRows) * 100
		// 四舍五入到小数点后2位
		progress = float64(int(rawProgress*100+0.5)) / 100
	}

	syncProgress := &storage.SyncProgress{
		TaskID:        j.taskID,
		Status:        j.stats.Status,
		SourceDB:      j.config.Reader.Database, // 源数据库
		SourceTable:   j.tableName,
		TargetDB:      j.config.Writer.Database, // 目标数据库
		TargetTable:   j.dstTable,
		TotalRows:     j.progress.TotalRows,
		SyncedRows:    j.progress.ProcessedRows,
		SyncedBytes:   j.stats.ProcessedBytes,
		StartSyncTime: j.stats.StartTime, // 保留原始开始时间
		LastSyncTime:  time.Now(),
		Progress:      progress,
	}

	return j.storage.SaveSyncProgress(context.Background(), syncProgress)
}

// onBatchProgress 批次进度回调函数
// 每次批次写入完成后被Pipeline调用，更新并保存进度
func (j *DefaultTableSyncJob) onBatchProgress(progress ProgressInfo) {
	// 更新进度信息
	j.progress.ProcessedRows += progress.ProcessedRows
	j.progress.ProcessedBatches += progress.BatchCount
	j.progress.LastSyncTime = time.Now()

	// 获取Pipeline最新统计信息以更新字节数
	pipelineStats := j.pipeline.GetStats()
	j.stats.ProcessedBytes = pipelineStats.TotalBytes
	j.stats.ProcessedRows = pipelineStats.TotalRows

	// 输出增强的进度日志（包含总行数、字节数、速率）
	elapsed := time.Since(j.stats.StartTime).Seconds()
	if elapsed > 0 {
		rowsPerSec := float64(j.progress.ProcessedRows) / elapsed
		bytesPerSec := float64(j.stats.ProcessedBytes) / elapsed

		if j.progress.TotalRows > 0 {
			// 有总行数时，显示进度百分比
			progressPct := float64(j.progress.ProcessedRows) * 100.0 / float64(j.progress.TotalRows)
			j.logger.Infof("Progress [%s]: %d batches, %d/%d rows (%.1f%%), %.2f rows/sec, %s, %.2f MB/sec",
				j.tableName, j.progress.ProcessedBatches, j.progress.ProcessedRows, j.progress.TotalRows,
				progressPct, rowsPerSec, utils.FormatBytes(j.stats.ProcessedBytes), bytesPerSec/1024/1024)
		} else {
			// 无总行数时，只显示已处理数量
			j.logger.Infof("Progress [%s]: %d batches, %d rows, %.2f rows/sec, %s, %.2f MB/sec",
				j.tableName, j.progress.ProcessedBatches, j.progress.ProcessedRows,
				rowsPerSec, utils.FormatBytes(j.stats.ProcessedBytes), bytesPerSec/1024/1024)
		}
	}

	// 创建进度数据快照，避免在异步保存时出现竞态条件
	progressSnapshot := &storage.SyncProgress{
		TaskID:        j.taskID,
		SourceDB:      j.config.Reader.Database, // 源数据库
		SourceTable:   j.tableName,
		TargetDB:      j.config.Writer.Database, // 目标数据库
		TargetTable:   j.dstTable,
		TotalRows:     j.progress.TotalRows,
		SyncedRows:    j.progress.ProcessedRows,
		SyncedBytes:   j.stats.ProcessedBytes,
		StartSyncTime: j.stats.StartTime,
		LastSyncTime:  time.Now(),
		Progress:      0.0, // 将在保存时计算
	}

	// 计算进度百分比，保留2位小数
	if progressSnapshot.TotalRows > 0 {
		rawProgress := float64(progressSnapshot.SyncedRows) / float64(progressSnapshot.TotalRows) * 100
		progressSnapshot.Progress = float64(int(rawProgress*100+0.5)) / 100
	}

	if err := j.storage.SaveSyncProgress(context.Background(), progressSnapshot); err != nil {
		j.logger.Warnf("Failed to save batch progress for table %s: %v", j.tableName, err)
	}

}

// syncTable 执行表同步逻辑
func (j *DefaultTableSyncJob) syncTable() error {
	j.logger.Infof("Starting table synchronization for %s from offset %d", j.tableName, j.progress.Offset)

	// 使用Pipeline处理表数据
	if err := j.pipeline.Process(j.ctx, j.tableName); err != nil {
		return fmt.Errorf("pipeline processing failed: %w", err)
	}

	j.logger.Infof("Table synchronization completed for %s, processed %d rows",
		j.tableName, j.stats.ProcessedRows)

	return nil
}

// handleError 处理同步错误
func (j *DefaultTableSyncJob) handleError(err error) {
	j.progress.Status = "failed"
	j.stats.Status = "failed"
	j.stats.EndTime = time.Now()
	j.stats.Duration = j.stats.EndTime.Sub(j.stats.StartTime)
	j.stats.ErrorCount++

	// 尝试保存当前进度状态
	if saveErr := j.saveProgress(); saveErr != nil {
		j.logger.Errorf("Failed to save error progress for table %s: %v", j.tableName, saveErr)
	}

	j.logger.Errorf("Table sync job failed for %s: %v", j.tableName, err)
}

// handleSuccess 处理同步成功
func (j *DefaultTableSyncJob) handleSuccess() {
	j.progress.Status = "completed"
	j.progress.LastSyncTime = time.Now()
	j.stats.Status = "completed"
	j.stats.EndTime = time.Now()
	j.stats.Duration = j.stats.EndTime.Sub(j.stats.StartTime)

	if err := j.saveProgress(); err != nil {
		j.logger.Errorf("Failed to save final progress for table %s: %v", j.tableName, err)
	}

	j.logger.Infof("Table sync job completed successfully for %s in %v, processed %d rows, %d bytes",
		j.tableName, j.stats.Duration, j.stats.ProcessedRows, j.stats.ProcessedBytes)
}
