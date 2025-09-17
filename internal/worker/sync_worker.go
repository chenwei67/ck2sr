package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/ck2sr/ck2sr/internal/pipeline"
	"github.com/ck2sr/ck2sr/internal/storage"
	"github.com/ck2sr/ck2sr/pkg/clickhouse"
	"github.com/ck2sr/ck2sr/pkg/starrocks"
	"github.com/ck2sr/ck2sr/pkg/utils"
)

// SyncWorker 数据同步工作单元实现
type SyncWorker struct {
	id              string
	taskID          string
	taskConfig      *config.SyncTaskConfig
	chClient        *clickhouse.Client
	srClient        *starrocks.Client
	pipeline        pipeline.Pipeline
	storage         storage.Storage
	logger          *logrus.Logger
	rateLimiter     *utils.RateLimiter
	metrics         *utils.MetricsCollector

	// 状态管理
	status          WorkerStatus
	statusMutex     sync.RWMutex

	// 控制信号
	ctx             context.Context
	cancel          context.CancelFunc
	pauseChan       chan struct{}
	resumeChan      chan struct{}

	// 统计信息
	stats           *WorkerStats
	statsMutex      sync.RWMutex

	// 同步状态
	totalRows       int64
	totalBytes      int64
	processedRows   int64
	processedBytes  int64
	errorCount      int64
	currentOffset   int64
	startTime       time.Time
}

// NewSyncWorker 创建数据同步工作单元
func NewSyncWorker(
	taskConfig *config.SyncTaskConfig,
	chClient *clickhouse.Client,
	srClient *starrocks.Client,
	pipeline pipeline.Pipeline,
	storage storage.Storage,
	logger *logrus.Logger,
) *SyncWorker {
	if logger == nil {
		logger = logrus.New()
	}

	workerID := fmt.Sprintf("worker-%s-%s", taskConfig.TaskID, uuid.New().String()[:8])

	// 创建速率限制器
	rateLimiter := utils.NewRateLimiter(
		taskConfig.RateLimit.MaxBytesPerSecond,
		taskConfig.RateLimit.MaxRowsPerSecond,
		taskConfig.RateLimit.BurstSize,
	)

	worker := &SyncWorker{
		id:          workerID,
		taskID:      taskConfig.TaskID,
		taskConfig:  taskConfig,
		chClient:    chClient,
		srClient:    srClient,
		pipeline:    pipeline,
		storage:     storage,
		logger:      logger,
		rateLimiter: rateLimiter,
		metrics:     utils.NewMetricsCollector(),
		status:      WorkerStatusIdle,
		pauseChan:   make(chan struct{}),
		resumeChan:  make(chan struct{}),
		stats: &WorkerStats{
			WorkerID: workerID,
			TaskID:   taskConfig.TaskID,
			Status:   WorkerStatusIdle,
		},
	}

	return worker
}

// GetID 获取工作单元 ID
func (w *SyncWorker) GetID() string {
	return w.id
}

// GetTaskID 获取任务 ID
func (w *SyncWorker) GetTaskID() string {
	return w.taskID
}

// GetStatus 获取工作单元状态
func (w *SyncWorker) GetStatus() WorkerStatus {
	w.statusMutex.RLock()
	defer w.statusMutex.RUnlock()
	return w.status
}

// setStatus 设置工作单元状态
func (w *SyncWorker) setStatus(status WorkerStatus) {
	w.statusMutex.Lock()
	defer w.statusMutex.Unlock()

	w.status = status

	w.statsMutex.Lock()
	w.stats.Status = status
	w.stats.LastUpdateTime = time.Now()
	w.statsMutex.Unlock()

	w.logger.Debugf("Worker %s status changed to %s", w.id, status)
}

// Start 启动工作单元
func (w *SyncWorker) Start(ctx context.Context) error {
	if w.IsRunning() {
		return fmt.Errorf("worker %s is already running", w.id)
	}

	w.ctx, w.cancel = context.WithCancel(ctx)
	w.setStatus(WorkerStatusRunning)
	w.startTime = time.Now()

	w.statsMutex.Lock()
	w.stats.StartTime = &w.startTime
	w.statsMutex.Unlock()

	// 启动同步处理
	go w.syncLoop()

	w.logger.Infof("Worker %s started for task %s", w.id, w.taskID)
	return nil
}

// Stop 停止工作单元
func (w *SyncWorker) Stop(ctx context.Context) error {
	if !w.IsRunning() {
		return nil
	}

	w.setStatus(WorkerStatusStopped)

	if w.cancel != nil {
		w.cancel()
	}

	// 等待同步循环结束或超时
	done := make(chan struct{})
	go func() {
		defer close(done)
		// 等待状态变为 stopped
		for w.GetStatus() == WorkerStatusRunning {
			time.Sleep(100 * time.Millisecond)
		}
	}()

	select {
	case <-done:
		w.logger.Infof("Worker %s stopped", w.id)
	case <-ctx.Done():
		w.logger.Warnf("Worker %s stop timeout", w.id)
	}

	endTime := time.Now()
	w.statsMutex.Lock()
	w.stats.EndTime = &endTime
	w.statsMutex.Unlock()

	return nil
}

// Pause 暂停工作单元
func (w *SyncWorker) Pause(ctx context.Context) error {
	if !w.IsRunning() {
		return fmt.Errorf("worker %s is not running", w.id)
	}

	w.setStatus(WorkerStatusPaused)

	// 发送暂停信号
	select {
	case w.pauseChan <- struct{}{}:
	default:
	}

	w.logger.Infof("Worker %s paused", w.id)
	return nil
}

// Resume 恢复工作单元
func (w *SyncWorker) Resume(ctx context.Context) error {
	if w.GetStatus() != WorkerStatusPaused {
		return fmt.Errorf("worker %s is not paused", w.id)
	}

	w.setStatus(WorkerStatusRunning)

	// 发送恢复信号
	select {
	case w.resumeChan <- struct{}{}:
	default:
	}

	w.logger.Infof("Worker %s resumed", w.id)
	return nil
}

// IsRunning 检查是否正在运行
func (w *SyncWorker) IsRunning() bool {
	status := w.GetStatus()
	return status == WorkerStatusRunning || status == WorkerStatusPaused
}

// CanResume 检查是否可以恢复
func (w *SyncWorker) CanResume() bool {
	status := w.GetStatus()
	return status == WorkerStatusPaused || status == WorkerStatusStopped
}

// GetStats 获取统计信息
func (w *SyncWorker) GetStats() *WorkerStats {
	w.statsMutex.RLock()
	defer w.statsMutex.RUnlock()

	// 创建副本
	stats := *w.stats
	return &stats
}

// GetProgress 获取进度信息
func (w *SyncWorker) GetProgress() *SyncProgress {
	w.statsMutex.RLock()
	defer w.statsMutex.RUnlock()

	progress := &SyncProgress{
		TaskID:         w.taskID,
		TotalRows:      w.totalRows,
		ProcessedRows:  w.processedRows,
		TotalBytes:     w.totalBytes,
		ProcessedBytes: w.processedBytes,
		ActiveWorkers:  1, // 单个工作单元
		ErrorCount:     w.errorCount,
	}

	// 计算进度百分比
	if w.totalRows > 0 {
		progress.Percentage = float64(w.processedRows) / float64(w.totalRows) * 100
	}

	// 计算速率和时间
	if w.startTime.IsZero() {
		return progress
	}

	elapsed := time.Since(w.startTime)
	progress.ElapsedTime = elapsed

	if elapsed.Seconds() > 0 {
		progress.AverageRate = float64(w.processedBytes) / elapsed.Seconds()
	}

	// 获取当前速率
	bytesPerSecond, _ := w.metrics.GetCurrentRate()
	progress.CurrentRate = bytesPerSecond

	// 估算剩余时间
	if w.processedRows > 0 && w.totalRows > 0 && progress.AverageRate > 0 {
		remainingBytes := w.totalBytes - w.processedBytes
		if remainingBytes > 0 {
			progress.EstimatedTime = time.Duration(float64(remainingBytes)/progress.AverageRate) * time.Second
		}
	}

	return progress
}

// syncLoop 同步循环
func (w *SyncWorker) syncLoop() {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Errorf("Worker %s panic: %v", w.id, r)
			w.setStatus(WorkerStatusError)
		}
	}()

	// 初始化同步状态
	if err := w.initializeSync(); err != nil {
		w.logger.Errorf("Failed to initialize sync: %v", err)
		w.setStatus(WorkerStatusError)
		return
	}

	w.logger.Infof("Worker %s initialized, total rows: %d, total bytes: %d",
		w.id, w.totalRows, w.totalBytes)

	// 开始同步数据
	if err := w.performSync(); err != nil {
		w.logger.Errorf("Sync failed: %v", err)
		w.setStatus(WorkerStatusError)

		w.statsMutex.Lock()
		w.stats.ErrorMessage = err.Error()
		w.statsMutex.Unlock()

		return
	}

	w.setStatus(WorkerStatusStopped)
	w.logger.Infof("Worker %s completed successfully", w.id)
}

// initializeSync 初始化同步
func (w *SyncWorker) initializeSync() error {
	// 获取源表信息
	sourceTableInfo, err := w.chClient.GetTableInfo(w.ctx, w.taskConfig.SourceTable)
	if err != nil {
		return fmt.Errorf("failed to get source table info: %w", err)
	}

	// 计算需要同步的数据量
	whereClause := ""
	if w.taskConfig.DataRange.Where != "" {
		whereClause = w.taskConfig.DataRange.Where
	}

	// 添加时间范围条件
	if w.taskConfig.DataRange.TimeColumn != "" {
		timeConditions := make([]string, 0)

		if w.taskConfig.DataRange.StartTime != nil {
			timeConditions = append(timeConditions,
				fmt.Sprintf("%s >= '%s'", w.taskConfig.DataRange.TimeColumn,
					w.taskConfig.DataRange.StartTime.Format("2006-01-02 15:04:05")))
		}

		if w.taskConfig.DataRange.EndTime != nil {
			timeConditions = append(timeConditions,
				fmt.Sprintf("%s < '%s'", w.taskConfig.DataRange.TimeColumn,
					w.taskConfig.DataRange.EndTime.Format("2006-01-02 15:04:05")))
		}

		if len(timeConditions) > 0 {
			timeClause := fmt.Sprintf("(%s)", fmt.Sprintf("%s", timeConditions[0]))
			if len(timeConditions) > 1 {
				timeClause = fmt.Sprintf("(%s)", fmt.Sprintf("%s AND %s", timeConditions[0], timeConditions[1]))
			}

			if whereClause != "" {
				whereClause = fmt.Sprintf("%s AND %s", whereClause, timeClause)
			} else {
				whereClause = timeClause
			}
		}
	}

	// 统计总行数
	totalRows, err := w.chClient.CountRows(w.ctx, w.taskConfig.SourceTable, whereClause)
	if err != nil {
		return fmt.Errorf("failed to count source rows: %w", err)
	}

	w.totalRows = totalRows
	w.totalBytes = sourceTableInfo.TotalBytes // 估算值

	w.statsMutex.Lock()
	w.stats.TotalBatches = (totalRows + int64(w.taskConfig.Concurrency.BatchSize) - 1) / int64(w.taskConfig.Concurrency.BatchSize)
	w.statsMutex.Unlock()

	// 尝试从检查点恢复
	if checkpoint, err := w.storage.GetCheckpoint(w.ctx, w.taskID); err == nil && checkpoint != nil {
		if offset, ok := checkpoint["offset"].(float64); ok {
			w.currentOffset = int64(offset)
			w.logger.Infof("Resuming from checkpoint: offset %d", w.currentOffset)
		}
	}

	return nil
}

// performSync 执行同步
func (w *SyncWorker) performSync() error {
	batchSize := int64(w.taskConfig.Concurrency.BatchSize)
	offset := w.currentOffset

	for offset < w.totalRows {
		select {
		case <-w.ctx.Done():
			return w.ctx.Err()
		case <-w.pauseChan:
			// 处理暂停信号
			w.logger.Infof("Worker %s paused at offset %d", w.id, offset)
			select {
			case <-w.ctx.Done():
				return w.ctx.Err()
			case <-w.resumeChan:
				w.logger.Infof("Worker %s resumed from offset %d", w.id, offset)
			}
		default:
		}

		// 检查时间窗口
		if w.taskConfig.TimeWindow != nil && !w.taskConfig.TimeWindow.IsInWindow(time.Now()) {
			w.logger.Debugf("Outside time window, pausing worker %s", w.id)
			time.Sleep(time.Minute) // 等待一分钟再检查
			continue
		}

		// 处理一个批次
		batchProcessed, err := w.processBatch(offset, batchSize)
		if err != nil {
			atomic.AddInt64(&w.errorCount, 1)

			// 根据重试配置决定是否重试
			if w.shouldRetry() {
				w.logger.Warnf("Batch processing failed, retrying: %v", err)
				time.Sleep(w.taskConfig.Retry.InitialDelay)
				continue
			} else {
				return fmt.Errorf("batch processing failed: %w", err)
			}
		}

		offset += batchProcessed
		w.currentOffset = offset

		// 更新统计信息
		w.updateStats(batchProcessed)

		// 保存检查点
		w.saveCheckpoint(offset)

		// 应用速率限制
		if err := w.rateLimiter.WaitForRows(w.ctx, int(batchProcessed)); err != nil {
			return err
		}
	}

	return nil
}

// processBatch 处理一个批次
func (w *SyncWorker) processBatch(offset, batchSize int64) (int64, error) {
	// 使用Arrow Flight SQL读取数据
	reader := w.chClient.NewArrowDataReader(w.taskConfig.SourceTable)

	// 设置查询参数
	if len(w.taskConfig.ColumnMapping) > 0 {
		columns := make([]string, 0, len(w.taskConfig.ColumnMapping))
		for srcCol := range w.taskConfig.ColumnMapping {
			columns = append(columns, srcCol)
		}
		reader.WithColumns(columns)
	}

	// 添加 WHERE 条件
	whereClause := w.buildWhereClause()
	if whereClause != "" {
		reader.WithWhere(whereClause)
	}

	// 设置限制和偏移
	reader.WithLimit(batchSize, offset)

	// 如果有时间列，按时间列排序以确保一致性
	if w.taskConfig.DataRange.TimeColumn != "" {
		reader.WithOrderBy(w.taskConfig.DataRange.TimeColumn)
	}

	// 设置批次大小和列映射
	reader.WithBatchSize(w.taskConfig.Concurrency.BatchSize)
	if w.taskConfig.ColumnMapping != nil {
		reader.WithColumnMapping(w.taskConfig.ColumnMapping)
	}

	// 执行Arrow Flight SQL查询并处理每个Record
	var totalRowCount int64
	err := reader.ReadArrowBatch(w.ctx, func(record arrow.Record) error {
		rowCount := record.NumRows()
		totalRowCount += rowCount

		w.logger.Debugf("Processing Arrow record with %d rows", rowCount)

		// 直接使用Arrow Flight SQL写入StarRocks
		if err := w.writeArrowToTarget(record); err != nil {
			return fmt.Errorf("failed to write Arrow record to target: %w", err)
		}

		return nil
	})

	if err != nil {
		return totalRowCount, fmt.Errorf("Arrow batch processing failed: %w", err)
	}

	return totalRowCount, nil
}

// writeArrowToTarget 直接写入Arrow数据到目标数据库
func (w *SyncWorker) writeArrowToTarget(record arrow.Record) error {
	// 使用StarRocks的Arrow Flight SQL写入器
	writer, err := w.srClient.NewArrowDataWriter(w.taskConfig.TargetTable, w.taskConfig.Concurrency.BatchSize)
	if err != nil {
		return fmt.Errorf("failed to create Arrow data writer: %w", err)
	}

	// 设置列映射
	if w.taskConfig.ColumnMapping != nil {
		writer.WithColumnMapping(w.taskConfig.ColumnMapping)
	}

	// 写入Arrow Record
	if err := writer.WriteRecord(record); err != nil {
		return fmt.Errorf("Arrow Flight SQL write failed: %w", err)
	}

	w.logger.Debugf("Arrow Flight SQL write completed: %d rows", record.NumRows())
	return nil
}

// writeToTarget 写入目标数据库（保留向后兼容）
func (w *SyncWorker) writeToTarget(batch pipeline.DataBatch) error {
	// 转换为 CSV 数据
	csvData := make([][]string, 0, batch.Size())

	for _, row := range batch.GetRows() {
		csvRow := row.ToCSV()
		csvData = append(csvData, csvRow)
	}

	// 执行 Stream Load
	result, err := w.srClient.StreamLoadFromCSV(w.ctx, w.taskConfig.TargetTable, csvData, nil)
	if err != nil {
		return fmt.Errorf("stream load failed: %w", err)
	}

	w.logger.Debugf("Stream load completed: %d rows loaded", result.NumberLoadedRows)
	return nil
}

// buildWhereClause 构建 WHERE 条件
func (w *SyncWorker) buildWhereClause() string {
	conditions := make([]string, 0)

	// 添加用户自定义条件
	if w.taskConfig.DataRange.Where != "" {
		conditions = append(conditions, w.taskConfig.DataRange.Where)
	}

	// 添加时间范围条件
	if w.taskConfig.DataRange.TimeColumn != "" {
		if w.taskConfig.DataRange.StartTime != nil {
			conditions = append(conditions,
				fmt.Sprintf("%s >= '%s'", w.taskConfig.DataRange.TimeColumn,
					w.taskConfig.DataRange.StartTime.Format("2006-01-02 15:04:05")))
		}

		if w.taskConfig.DataRange.EndTime != nil {
			conditions = append(conditions,
				fmt.Sprintf("%s < '%s'", w.taskConfig.DataRange.TimeColumn,
					w.taskConfig.DataRange.EndTime.Format("2006-01-02 15:04:05")))
		}
	}

	if len(conditions) == 0 {
		return ""
	}

	if len(conditions) == 1 {
		return conditions[0]
	}

	return fmt.Sprintf("(%s)", fmt.Sprintf("%s", conditions[0]))
}

// shouldRetry 判断是否应该重试
func (w *SyncWorker) shouldRetry() bool {
	w.statsMutex.RLock()
	retryCount := w.stats.ErrorCount
	w.statsMutex.RUnlock()

	return retryCount < int64(w.taskConfig.Retry.MaxRetries)
}

// updateStats 更新统计信息
func (w *SyncWorker) updateStats(processedRows int64) {
	atomic.AddInt64(&w.processedRows, processedRows)

	// 估算处理的字节数
	avgRowSize := w.totalBytes / w.totalRows
	processedBytes := processedRows * avgRowSize
	atomic.AddInt64(&w.processedBytes, processedBytes)

	// 更新指标
	w.metrics.Update(processedBytes, processedRows)

	// 更新统计信息
	w.statsMutex.Lock()
	w.stats.ProcessedRows = w.processedRows
	w.stats.ProcessedBytes = w.processedBytes
	w.stats.CurrentBatch++
	w.stats.LastUpdateTime = time.Now()

	// 计算处理速度
	if elapsed := time.Since(w.startTime).Seconds(); elapsed > 0 {
		w.stats.BytesPerSecond = float64(w.processedBytes) / elapsed
		w.stats.RowsPerSecond = float64(w.processedRows) / elapsed
	}
	w.statsMutex.Unlock()
}

// saveCheckpoint 保存检查点
func (w *SyncWorker) saveCheckpoint(offset int64) {
	checkpoint := map[string]interface{}{
		"worker_id": w.id,
		"offset":    offset,
		"timestamp": time.Now().Unix(),
	}

	if err := w.storage.SaveCheckpoint(w.ctx, w.taskID, checkpoint); err != nil {
		w.logger.Warnf("Failed to save checkpoint: %v", err)
	}
}