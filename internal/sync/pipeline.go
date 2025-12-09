package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/client"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/internal/reader"
	"github.com/sunkaimr/ck2sr/internal/storage"
	"github.com/sunkaimr/ck2sr/internal/writer"
	"github.com/sunkaimr/ck2sr/pkg/protocol"
	"github.com/sunkaimr/ck2sr/pkg/retry"
	"github.com/sunkaimr/ck2sr/pkg/utils"
)

// Pipeline 异步数据处理管道
// 支持Channel-based数据流、Writer并发和Offset持久化
type Pipeline struct {
	config      *config.SyncTaskConfig
	policy      *config.PolicyConfig
	logger      *logrus.Logger
	srcTable    string
	dstTable    string
	reader      reader.ExecutableReader
	writer      writer.ExecutableWriter
	monitor     *Monitor
	storage     storage.Storage
	ckClientMgr *client.ClickHouseCliMgr
	srClientMgr *client.StarRocksCliMgr

	// 异步处理相关
	dataChan     chan DataBatch    // 数据批次通道
	errorChan    chan error        // 错误通道
	progressChan chan ProgressInfo // 进度通道
	cancel       context.CancelFunc
	wg           sync.WaitGroup

	// 进度回调函数
	progressCallback func(progress ProgressInfo)

	// 统计信息
	stats   *PipelineStats
	statsMu sync.RWMutex
}

// DataBatch 数据批次结构
type DataBatch struct {
	Data   []interface{} // 数据记录
	Offset int64         // 当前偏移量
	Table  string        // 表名
}

// ProgressInfo 进度信息
type ProgressInfo struct {
	Table         string
	Offset        int64
	ProcessedRows uint64
	BatchCount    int64
	BatchRecord   []interface{}
}

// NewPipeline 创建新的异步Pipeline
func NewPipeline(cfg *config.SyncTaskConfig,
	policy *config.PolicyConfig,
	ckCliMgr *client.ClickHouseCliMgr,
	srCliMgr *client.StarRocksCliMgr,
	srcTable string,
	dstTable string,
	logger *logrus.Logger) *Pipeline {
	return &Pipeline{
		config:      cfg,
		policy:      policy,
		ckClientMgr: ckCliMgr,
		srClientMgr: srCliMgr,
		srcTable:    srcTable,
		dstTable:    dstTable,
		logger:      logger,
		monitor:     NewMonitor(cfg.TaskID, logger),
		stats: &PipelineStats{
			StartTime: time.Now(),
		},
		// 初始化channel，缓冲区大小为配置的并发数
		dataChan:     make(chan DataBatch, policy.Transfer.WriterConcurrency),
		errorChan:    make(chan error, policy.Transfer.WriterConcurrency),
		progressChan: make(chan ProgressInfo, policy.Transfer.WriterConcurrency),
	}
}

// SetStorage 设置存储接口，用于Offset持久化
func (p *Pipeline) SetStorage(storage storage.Storage) {
	p.storage = storage
}

// SetProgressCallback 设置进度回调函数，每次批次写入完成后调用
func (p *Pipeline) SetProgressCallback(callback func(progress ProgressInfo)) {
	p.progressCallback = callback
}

// Initialize 初始化Pipeline组件
func (p *Pipeline) Initialize(ctx context.Context, offset uint64, limit uint64) error {
	readerFactory := reader.NewReaderFactory()
	writerFactory := writer.NewWriterFactory()

	var err error
	p.reader, err = readerFactory.CreateExecutable(&p.config.Reader, p.ckClientMgr, p.srClientMgr, p.logger)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}

	// 为Reader包装重试功能
	p.reader = reader.WrapReaderWithRetry(p.reader, &p.policy.Retry, p.logger)
	p.logger.Infof("Reader initialized with retry policy: max_attempts=%d, initial_backoff=%v",
		p.policy.Retry.MaxAttempts, p.policy.Retry.InitialBackoff)

	p.writer, err = writerFactory.Create(&p.config.Writer, p.ckClientMgr, p.srClientMgr, p.logger)
	if err != nil {
		return fmt.Errorf("failed to create writer: %w", err)
	}
	p.writer.SetTable(p.dstTable)

	// 为Writer包装重试功能
	p.writer = writer.WrapWriterWithRetry(p.writer, &p.policy.Retry, p.logger)
	p.logger.Infof("Writer initialized with retry policy: max_attempts=%d, initial_backoff=%v",
		p.policy.Retry.MaxAttempts, p.policy.Retry.InitialBackoff)

	// 设置查询条件，并执行查询命令
	var selectClause = "*"
	if strings.EqualFold(p.config.Reader.Vendor, "clickhouse") {
		var cols []string
		err := retry.Retry(ctx, &p.policy.Retry, p.logger, "get clickhouse table columns", func() error {
			var err error
			cols, err = p.getClickHouseSelectableColumns(ctx, &p.config.Reader, p.srcTable, p.config.Settings.Filter.ExcludeColumns)
			if err != nil {
				return fmt.Errorf("Failed to fetch ClickHouse schema for %s.%s, falling back to SELECT *: %v", p.config.Reader.Database, p.srcTable, err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("retry failed: %s", err.Error())
		}

		if len(cols) == 0 {
			return fmt.Errorf("No selectable columns found for %s.%s after applying exclude list", p.config.Reader.Database, p.srcTable)

		}
		selectClause = strings.Join(cols, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM %s.%s", selectClause, p.config.Reader.Database, p.srcTable)
	if p.config.Settings.DataRange.TimeColumn != "" {
		if p.config.Settings.DataRange.StartTime != "" {
			query += fmt.Sprintf(" WHERE %s >= '%s'", p.config.Settings.DataRange.TimeColumn, p.config.Settings.DataRange.StartTime)
		}

		if p.config.Settings.DataRange.EndTime != "" {
			if p.config.Settings.DataRange.StartTime != "" {
				query += fmt.Sprintf(" AND %s < '%s'", p.config.Settings.DataRange.TimeColumn, p.config.Settings.DataRange.EndTime)
			} else {
				query += fmt.Sprintf(" WHERE %s < '%s'", p.config.Settings.DataRange.TimeColumn, p.config.Settings.DataRange.EndTime)
			}
		}
	}
	if offset > 0 && limit > 0 {
		query += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	}

	if err := p.reader.SetColumnFilter(p.config.Settings.Filter.ExcludeColumns, p.config.Settings.Filter.FixedValues).
		SetQuery(query).Execute(ctx); err != nil {
		return fmt.Errorf("failed to execute reader query: %w", err)
	}

	p.logger.Infof("Pipeline initialized with %d writer concurrency", p.policy.Transfer.WriterConcurrency)
	return nil
}

func (p *Pipeline) getClickHouseSelectableColumns(ctx context.Context, readerCfg *config.DataSourceConfig, table string, exclude []string) ([]string, error) {
	q := fmt.Sprintf("SELECT name FROM system.columns WHERE database = '%s' AND table = '%s' AND upper(default_kind) != 'EPHEMERAL' ORDER BY position", readerCfg.Database, table)

	switch strings.ToLower(readerCfg.Protocol) {
	case "mysql":
		cli, err := p.ckClientMgr.GetMySQLClient(readerCfg.Name)
		if err != nil {
			return nil, err
		}
		rows, err := cli.Query(ctx, q)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var cols []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, err
			}
			if !containsString(exclude, name) {
				cols = append(cols, name)
			}
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return cols, nil

	case "http":
		cli, err := p.ckClientMgr.GetHTTPClient(readerCfg.Name)
		if err != nil {
			return nil, err
		}
		data, err := cli.Query(ctx, q, "JSON")
		if err != nil {
			return nil, err
		}

		type chSystemColumnsResp struct {
			Data []struct {
				Name string `json:"name"`
			} `json:"data"`
		}
		var resp chSystemColumnsResp
		if err := json.Unmarshal(data, &resp); err != nil {
			return nil, fmt.Errorf("failed to parse ClickHouse system.columns response: %w", err)
		}
		var cols []string
		for _, row := range resp.Data {
			if !containsString(exclude, row.Name) {
				cols = append(cols, row.Name)
			}
		}
		return cols, nil

	default:
		return nil, fmt.Errorf("unsupported clickhouse protocol: %s", readerCfg.Protocol)
	}
}

func containsString(list []string, target string) bool {
	for _, v := range list {
		if v == target {
			return true
		}
	}
	return false
}

// Process 异步处理表数据，支持Offset和并发Writer
func (p *Pipeline) Process(ctx context.Context, table string) error {
	p.logger.Infof("Starting async pipeline for table: %s with %d writers", table, p.policy.Transfer.WriterConcurrency)
	p.monitor.StartTable(table)

	// 创建带取消的上下文
	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel
	defer cancel()

	defer func() {
		p.monitor.EndTable(table)
		if p.reader != nil {
			p.reader.Close()
		}
		if p.writer != nil {
			p.writer.Close()
		}
		p.updateEndTime()
	}()

	// 启动并发Writer goroutines
	for i := 0; i < p.policy.Transfer.WriterConcurrency; i++ {
		p.wg.Add(1)
		go p.writerWorker(ctx, i)
	}

	// 启动进度监控goroutine
	go p.progressMonitor(ctx, table)

	// 主读取循环
	err := p.readData(ctx, table)
	if err != nil {
		p.logger.Errorf("Reader error: %v", err)
		return err
	}
	// 关闭数据通道，等待writers完成
	close(p.dataChan)
	p.wg.Wait()

	// 检查是否有错误
	select {
	case err := <-p.errorChan:
		return fmt.Errorf("writer error: %w", err)
	default:
	}

	p.logger.Infof("Async pipeline completed for table %s: %d batches, %d rows",
		table, p.getStats().BatchCount, p.getStats().TotalRows)
	return nil
}

// readData 主读取循环，负责从数据源读取数据并发送到channel
// 支持按条数和按字节数两种攒批策略
func (p *Pipeline) readData(ctx context.Context, table string) error {
	batchCount := 0
	currentOffset := int64(0)

	// 如果reader支持OffsetReader，尝试设置offset
	if offsetReader, ok := p.reader.(protocol.OffsetReader); ok {
		currentOffset = offsetReader.GetOffset()
		p.logger.Infof("Starting read from offset: %d", currentOffset)
	}

	// 获取攒批配置：从任务配置中获取
	batchSize := p.config.Settings.BatchSize
	batchBytes := p.config.Settings.BatchBytes

	// 如果两个配置都为0，使用默认条数1000
	if batchSize == 0 && batchBytes == 0 {
		batchSize = 1000
		p.logger.Infof("Using default batch size: %d (both batch_size and batch_bytes are not configured)", batchSize)
	}

	// 输出攒批策略日志
	if batchSize > 0 && batchBytes > 0 {
		p.logger.Infof("Batch strategy: rows=%d OR bytes=%s (whichever comes first)", batchSize, utils.FormatBytes(batchBytes))
	} else if batchSize > 0 {
		p.logger.Infof("Batch strategy: rows=%d", batchSize)
	} else {
		p.logger.Infof("Batch strategy: bytes=%s", utils.FormatBytes(batchBytes))
	}

	p.logger.Debugf("Reader started for table: %s", table)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 检查是否还有数据
		if !p.reader.Next() {
			break
		}

		// 构建数据批次，使用动态攒批策略
		batch := make([]interface{}, 0, batchSize)
		currentBatchBytes := int64(0)

		for {
			// 检查上下文取消，避免CTRL+C时阻塞
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// 时间窗口检查
			if p.policy.Schedule.TimeWindow.Enabled {
				if ok, _ := utils.IsInTimeWindow(p.policy.Schedule.TimeWindow.StartTime, p.policy.Schedule.TimeWindow.EndTime); !ok {
					p.logger.Infof("Current time is outside the allowed time window (%s - %s). Pausing reading.",
						p.policy.Schedule.TimeWindow.StartTime, p.policy.Schedule.TimeWindow.EndTime)
					time.Sleep(p.policy.Schedule.CheckInterval)
					continue
				}
			}

			// 获取当前记录
			record, err := p.reader.GetRecord()
			if err != nil {
				return fmt.Errorf("failed to get record: %w", err)
			}

			// 计算记录大小（仅在启用字节数限制时）
			var recordSize int64
			if batchBytes > 0 {
				recordSize, err = utils.CalculateRecordSize(record)
				if err != nil {
					p.logger.Warnf("Failed to calculate record size, using 0: %v", err)
					recordSize = 0
				}
			}

			// 检查是否达到批次限制（在添加记录之前检查）
			// 规则：如果添加当前记录会超过阈值，则先发送之前的批次
			shouldSendBatch := false
			if batchSize > 0 && batchBytes > 0 {
				// 双重策略：任一条件满足即发送（但至少要有一条记录）
				if len(batch) > 0 && (len(batch) >= batchSize || currentBatchBytes+recordSize > batchBytes) {
					shouldSendBatch = true
				}
			} else if batchSize > 0 {
				// 仅条数策略
				if len(batch) >= batchSize {
					shouldSendBatch = true
				}
			} else if batchBytes > 0 {
				// 仅字节数策略（至少要有一条记录）
				if len(batch) > 0 && currentBatchBytes+recordSize > batchBytes {
					shouldSendBatch = true
				}
			}

			// 如果需要发送批次，先发送已有数据，当前记录放入下一批
			if shouldSendBatch {
				// 发送当前批次
				if err := p.sendBatch(ctx, batch, currentOffset, table); err != nil {
					return err
				}

				batchCount++
				p.updateBatchCount(1)

				// 重置批次
				batch = make([]interface{}, 0, batchSize)
				currentBatchBytes = 0
			}

			// 添加当前记录到批次
			batch = append(batch, record)
			currentBatchBytes += recordSize
			currentOffset++

			// 检查是否还有更多数据
			if !p.reader.Next() {
				break
			}
		}

		// 发送最后一批数据
		if len(batch) > 0 {
			if err := p.sendBatch(ctx, batch, currentOffset, table); err != nil {
				return err
			}

			batchCount++
			p.updateBatchCount(1)
		}

		// 没有更多数据，退出循环
		break
	}

	p.logger.Infof("Read completed: %d batches read", batchCount)
	return nil
}

// sendBatch 发送数据批次到channel
func (p *Pipeline) sendBatch(ctx context.Context, batch []interface{}, offset int64, table string) error {
	if len(batch) == 0 {
		return nil
	}

	dataBatch := DataBatch{
		Data:   batch,
		Offset: offset,
		Table:  table,
	}

	select {
	case p.dataChan <- dataBatch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// writerWorker 并发Writer工作者，处理数据写入
func (p *Pipeline) writerWorker(ctx context.Context, workerID int) {
	defer p.wg.Done()
	p.logger.Debugf("Writer worker %d started", workerID)

	for {
		select {
		case batch, ok := <-p.dataChan:
			if !ok {
				p.logger.Infof("Writer worker %d: data channel closed", workerID)
				return
			}

			p.logger.Debugf("Writer worker %d: processing batch with %d records, offset %d", workerID, len(batch.Data), batch.Offset)
			// 处理数据批次
			if err := p.processBatch(ctx, batch, workerID); err != nil {
				select {
				case p.errorChan <- err:
				default:
					p.logger.Errorf("Error channel full, dropping error: %v", err)
				}
				return
			}

			// 发送进度更新
			progress := ProgressInfo{
				Table:         batch.Table,
				Offset:        batch.Offset,
				ProcessedRows: uint64(len(batch.Data)),
				BatchCount:    1,
				BatchRecord:   batch.Data,
			}
			p.progressChan <- progress

			// 批处理间隔
			if p.config.Settings.BatchInterval > 0 {
				time.Sleep(p.config.Settings.BatchInterval)
			}
		case <-ctx.Done():
			p.logger.Debugf("Writer worker %d: context cancelled", workerID)
			return
		}
	}
}

// processBatch 处理单个数据批次
func (p *Pipeline) processBatch(ctx context.Context, batch DataBatch, workerID int) error {
	// 写入数据
	if err := p.writer.Write(ctx, batch.Data); err != nil {
		return fmt.Errorf("worker %d failed to write batch: %w", workerID, err)
	}

	// 刷新数据
	if err := p.writer.Flush(ctx); err != nil {
		return fmt.Errorf("worker %d failed to flush batch: %w", workerID, err)
	}

	// 计算批次的字节数
	batchBytes, err := utils.CalculateBatchSize(batch.Data)
	if err != nil {
		p.logger.Warnf("Failed to calculate batch size: %v", err)
		batchBytes = 0
	}

	// 更新统计信息：行数和字节数
	p.monitor.AddRows(batch.Table, int64(len(batch.Data)))
	p.monitor.AddBytes(batch.Table, batchBytes)
	p.updateProcessedRows(int64(len(batch.Data)))

	return nil
}

// progressMonitor 监控进度并定期输出
func (p *Pipeline) progressMonitor(ctx context.Context, table string) {
	for {
		select {
		case progress := <-p.progressChan:
			// 调用进度回调函数（如果已设置），实时输出和保存同步进度
			if p.progressCallback != nil {
				p.progressCallback(progress)
			}
		case <-ctx.Done():
			p.logger.Debugf("Progress monitor stopped for table %s", table)
			return
		}
	}
}

// ===== 统计信息更新方法 =====

func (p *Pipeline) updateBatchCount(delta int) {
	p.statsMu.Lock()
	p.stats.BatchCount += delta
	p.statsMu.Unlock()
}

func (p *Pipeline) updateProcessedRows(delta int64) {
	p.statsMu.Lock()
	p.stats.TotalRows += delta
	p.statsMu.Unlock()
}

func (p *Pipeline) updateEndTime() {
	p.statsMu.Lock()
	p.stats.EndTime = time.Now()
	p.statsMu.Unlock()
}

func (p *Pipeline) getStats() PipelineStats {
	p.statsMu.RLock()
	defer p.statsMu.RUnlock()
	return *p.stats
}

// GetStats 获取Pipeline统计信息
func (p *Pipeline) GetStats() *PipelineStats {
	stats := p.getStats() // 使用内部方法获取统计
	// 与监控器数据合并
	return &PipelineStats{
		TablesProcessed:   p.monitor.GetProcessedTables(),
		TotalRows:         stats.TotalRows,
		TotalBytes:        p.monitor.GetTotalBytes(),
		StartTime:         stats.StartTime,
		EndTime:           stats.EndTime,
		BatchCount:        stats.BatchCount,
		WriterConcurrency: p.policy.Transfer.WriterConcurrency,
	}
}

// PipelineStats Pipeline统计信息
type PipelineStats struct {
	TablesProcessed   []string  `json:"tables_processed"`   // 已处理的表列表
	TotalRows         int64     `json:"total_rows"`         // 总处理行数
	TotalBytes        int64     `json:"total_bytes"`        // 总处理字节数
	StartTime         time.Time `json:"start_time"`         // 开始时间
	EndTime           time.Time `json:"end_time"`           // 结束时间
	BatchCount        int       `json:"batch_count"`        // 已处理批次数
	WriterConcurrency int       `json:"writer_concurrency"` // Writer并发数
}
