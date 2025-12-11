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
}

// TableSyncProgress 表同步进度信息
type TableSyncProgress struct {
	TaskID           string        `json:"task_id"`           // 所属任务ID
	TableName        string        `json:"table_name"`        // 表名
	Offset           uint64        `json:"offset"`            // 当前偏移量
	TotalRows        uint64        `json:"total_rows"`        // 要同步的总行数
	ProcessedBatches int64         `json:"processed_batches"` // 已处理批次数
	ProcessedRows    uint64        `json:"processed_rows"`    // 已处理行数
	ProcessedBytes   int64         `json:"processed_bytes"`   // 已处理字节数
	Progress         float64       `json:"progress"`          // 百分比（0.00-100.00）
	LastSyncTime     time.Time     `json:"last_sync_time"`    // 最后更新时间
	StartTime        time.Time     `json:"start_time"`        // 同步开始时间
	EndTime          time.Time     `json:"end_time"`          // 同步结束时间
	Duration         time.Duration `json:"duration"`          // 同步耗时
	Status           string        `json:"status"`            // 同步状态: pending, running, completed, failed
}

// TableSyncStats 表同步统计
type TableSyncStats struct {
	TableName      string        `json:"table_name"`      // Deprecated 表名
	StartTime      time.Time     `json:"start_time"`      // Deprecated同步开始时间
	EndTime        time.Time     `json:"end_time"`        // Deprecated 同步结束时间
	Duration       time.Duration `json:"duration"`        // Deprecated 同步耗时
	ProcessedRows  int64         `json:"processed_rows"`  // Deprecated
	ProcessedBytes int64         `json:"processed_bytes"` // Deprecated
	ErrorCount     int64         `json:"error_count"`     // Deprecated 同步失败次数
	Status         string        `json:"status"`          // Deprecated 同步结果: pending, running, completed, failed
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
			TaskID:    taskID,
			TableName: tableName,
			Status:    "pending",
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
	// 仅在新任务时设置StartTime，断点续传时保留原有时间
	if j.progress.StartTime.IsZero() {
		j.progress.StartTime = time.Now()
	}

	// 从存储中加载上次的同步进度
	progressLoaded := false
	if err := j.loadProgress(); err != nil {
		j.logger.Infof("No previous progress found for table %s, starting from beginning (reason: %v)", j.tableName, err)
	} else {
		// 成功加载了进度数据
		progressLoaded = true
		j.logger.Infof("Loaded progress for table %s: offset=%d, processed_rows=%d, total_rows=%d",
			j.tableName, j.progress.Offset, j.progress.ProcessedRows, j.progress.TotalRows)

		// 幂等性检查：如果表已经完成同步，直接返回成功
		if j.progress.Status == "completed" {
			j.logger.Infof("Table %s status already completed, no sync needed", j.tableName)
			return nil
		} else {
			j.progress.Status = "running"
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

	// 启动同步过程
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

// queryTotalRows 查询本次需要同步的总行数
// 根据任务配置的数据范围条件构建COUNT查询
func (j *DefaultTableSyncJob) queryTotalRows() (uint64, error) {
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
		Status:        j.progress.Status,
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

	// 读取中断重试关心的字段数据
	j.progress.Offset = syncProgress.SyncedRows // 使用已同步行数作为偏移量
	j.progress.ProcessedRows = syncProgress.SyncedRows
	j.progress.ProcessedBytes = syncProgress.SyncedBytes
	j.progress.TotalRows = syncProgress.TotalRows
	j.progress.Status = syncProgress.Status

	// Progress信息是每次写入成功就会回调的，根据进度信息修改进度状态值
	if syncProgress.Progress >= 100.0 || (syncProgress.TotalRows > 0 && syncProgress.SyncedRows >= syncProgress.TotalRows) {
		j.progress.Status = "completed"
	}

	return nil
}

// saveProgress 保存同步进度到存储
func (j *DefaultTableSyncJob) saveProgress() error {
	syncProgress := &storage.SyncProgress{
		TaskID:        j.taskID,
		Status:        j.progress.Status,
		SourceDB:      j.config.Reader.Database, // 源数据库
		SourceTable:   j.tableName,
		TargetDB:      j.config.Writer.Database, // 目标数据库
		TargetTable:   j.dstTable,
		TotalRows:     j.progress.TotalRows,
		SyncedRows:    j.progress.ProcessedRows,
		SyncedBytes:   j.progress.ProcessedBytes,
		StartSyncTime: j.progress.StartTime, // 保留原始开始时间
		LastSyncTime:  time.Now(),
		Progress:      j.progress.Progress,
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
	j.progress.ProcessedBytes = pipelineStats.TotalBytes

	// 输出增强的进度日志（包含总行数、字节数、速率）
	elapsed := time.Since(j.progress.StartTime).Seconds()
	if elapsed > 0 {
		rowsPerSec := float64(j.progress.ProcessedRows) / elapsed
		bytesPerSec := float64(j.progress.ProcessedBytes) / elapsed

		if j.progress.TotalRows > 0 {
			// 有总行数时，显示进度百分比
			progressPct := float64(j.progress.ProcessedRows) * 100.0 / float64(j.progress.TotalRows)
			j.logger.Infof("Progress [%s]: %d batches, %d/%d rows (%.1f%%), %.2f rows/sec, %s, %.2f MB/sec",
				j.tableName, j.progress.ProcessedBatches, j.progress.ProcessedRows, j.progress.TotalRows,
				progressPct, rowsPerSec, utils.FormatBytes(j.progress.ProcessedBytes), bytesPerSec/1024/1024)
		} else {
			// 无总行数时，只显示已处理数量
			j.logger.Infof("Progress [%s]: %d batches, %d rows, %.2f rows/sec, %s, %.2f MB/sec",
				j.tableName, j.progress.ProcessedBatches, j.progress.ProcessedRows,
				rowsPerSec, utils.FormatBytes(j.progress.ProcessedBytes), bytesPerSec/1024/1024)
		}
	}
	// 计算进度百分比
	if j.progress.TotalRows > 0 {
		rawProgress := float64(j.progress.ProcessedRows) / float64(j.progress.TotalRows) * 100
		j.progress.Progress = float64(int(rawProgress*100)) / 100
	}

	// 尝试保存当前进度状态
	if saveErr := j.saveProgress(); saveErr != nil {
		j.logger.Errorf("Failed to save error progress for table %s: %v", j.tableName, saveErr)
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
		j.tableName, j.progress.ProcessedRows)

	return nil
}

// handleError 处理同步错误
func (j *DefaultTableSyncJob) handleError(err error) {
	j.progress.Status = "failed"
	j.progress.EndTime = time.Now()
	j.progress.Duration = j.progress.EndTime.Sub(j.progress.StartTime)

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
	j.progress.EndTime = time.Now()
	j.progress.Duration = j.progress.EndTime.Sub(j.progress.StartTime)

	if err := j.saveProgress(); err != nil {
		j.logger.Errorf("Failed to save final progress for table %s: %v", j.tableName, err)
	}

	j.logger.Infof("Table sync job completed successfully for %s in %v, processed %d rows, %d bytes",
		j.tableName, j.progress.Duration, j.progress.ProcessedRows, j.progress.ProcessedBytes)
}
