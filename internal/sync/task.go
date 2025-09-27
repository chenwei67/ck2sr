package sync

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/ck2sr/ck2sr/internal/logging"
)

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// StarRocksStreamLoadResponse StarRocks Stream Load响应结构
type StarRocksStreamLoadResponse struct {
	TxnId                  int64  `json:"TxnId"`
	Label                  string `json:"Label"`
	Status                 string `json:"Status"`
	Message                string `json:"Message"`
	NumberTotalRows        int64  `json:"NumberTotalRows"`
	NumberLoadedRows       int64  `json:"NumberLoadedRows"`
	NumberFilteredRows     int64  `json:"NumberFilteredRows"`
	NumberUnselectedRows   int64  `json:"NumberUnselectedRows"`
	LoadBytes              int64  `json:"LoadBytes"`
	LoadTimeMs             int64  `json:"LoadTimeMs"`
	BeginTxnTimeMs         int64  `json:"BeginTxnTimeMs"`
	StreamLoadPlanTimeMs   int64  `json:"StreamLoadPlanTimeMs"`
	ReadDataTimeMs         int64  `json:"ReadDataTimeMs"`
	WriteDataTimeMs        int64  `json:"WriteDataTimeMs"`
	CommitAndPublishTimeMs int64  `json:"CommitAndPublishTimeMs"`
}

// ColumnInfo 表示数据库列信息
type ColumnInfo struct {
	Name         string
	DataType     string
	IsNullable   bool
	DefaultValue *string
	Comment      string
	CharLength   *int64
	NumPrecision *int64
	NumScale     *int64
}

// DataReader 数据读取器接口
type DataReader interface {
	Next() bool
	GetRecord() (interface{}, error) // 返回一行已经被JSON序列化过数据
	Close() error
}

// DataWriter 数据写入器接口
type DataWriter interface {
	Write(ctx context.Context, data []interface{}) error
	Close() error
}

// MySQLDataReader MySQL协议数据读取器
type MySQLDataReader struct {
	rows   *sql.Rows
	logger *logrus.Logger
}

// FlightSQLDataReader FlightSQL协议数据读取器
type FlightSQLDataReader struct {
	reader          array.RecordReader
	allocator       memory.Allocator
	logger          *logrus.Logger
	currentRecord   arrow.Record // 当前正在处理的记录
	currentRowIndex int          // 当前行索引
}

// HTTPDataWriter HTTP Stream Load数据写入器
type HTTPDataWriter struct {
	client      *http.Client
	endpoint    string
	table       string
	auth        string
	userName    string
	password    string
	columnNames []string // 添加列名信息用于JSON转换
	logger      *logrus.Logger
}

// SyncTask 同步任务
type SyncTask struct {
	id           string
	name         string
	config       *config.SyncTaskConfig
	globalConfig *config.Config
	enabled      bool
	mutex        sync.RWMutex
	logger       *logrus.Logger

	// 执行状态
	status     TaskStatusType
	lastRun    time.Time
	nextRun    time.Time
	errorCount int
	lastError  error

	// 统计信息
	totalRuns   int64
	successRuns int64
	failedRuns  int64
	totalRows   int64
	totalBytes  int64
}

// TaskStatusType 任务状态类型
type TaskStatusType int

const (
	TaskStatusIdle TaskStatusType = iota
	TaskStatusRunning
	TaskStatusPaused
	TaskStatusFailed
)

// NewSyncTask 创建新的同步任务
func NewSyncTask(taskConfig *config.SyncTaskConfig, globalConfig *config.Config) (*SyncTask, error) {
	// 使用配置文件中的日志配置创建logger
	logger, err := logging.CreateLoggerWithFileRotation(globalConfig.Log)
	if err != nil {
		// 如果创建配置化的logger失败，回退到默认logger
		logger = logrus.New()
		logger.Warnf("Failed to create configured logger, using default: %v", err)
	}

	task := &SyncTask{
		id:           taskConfig.TaskID,
		name:         taskConfig.Name,
		config:       taskConfig,
		globalConfig: globalConfig,
		enabled:      taskConfig.Enabled,
		status:       TaskStatusIdle,
		logger:       logger,
	}

	return task, nil
}

// GetID 获取任务ID
func (t *SyncTask) GetID() string {
	return t.id
}

// GetName 获取任务名称
func (t *SyncTask) GetName() string {
	return t.name
}

// IsEnabled 检查任务是否启用
func (t *SyncTask) IsEnabled() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()
	return t.enabled
}

// SetEnabled 设置任务启用状态
func (t *SyncTask) SetEnabled(enabled bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()
	t.enabled = enabled
}

// IsInTimeWindow 检查是否在执行时间窗口内
func (t *SyncTask) IsInTimeWindow() bool {
	now := time.Now()
	startTime := t.config.Settings.TimeWindow.StartTime
	endTime := t.config.Settings.TimeWindow.EndTime

	// 如果没有配置时间窗口，总是允许执行
	if startTime == "" || endTime == "" {
		return true
	}

	currentTime := now.Format("15:04")

	// 简单的时间比较（这里应该有更复杂的逻辑处理跨天的情况）
	return currentTime >= startTime && currentTime <= endTime
}

// Execute 执行同步任务
func (t *SyncTask) Execute(ctx context.Context) error {
	t.mutex.Lock()
	if t.status == TaskStatusRunning {
		t.mutex.Unlock()
		return fmt.Errorf("task %s is already running", t.id)
	}

	t.status = TaskStatusRunning
	t.lastRun = time.Now()
	t.totalRuns++
	t.mutex.Unlock()

	defer func() {
		t.mutex.Lock()
		t.status = TaskStatusIdle
		t.mutex.Unlock()
	}()

	// 输出同步任务开始的详细信息
	t.logger.Infof("========== Starting Sync Task: %s ==========", t.name)
	t.logger.Infof("Task ID: %s", t.id)
	t.logger.Infof("Source: %s (%s) -> Target: %s (%s)",
		t.config.Reader.Vendor, t.config.Reader.Protocol,
		t.config.Writer.Vendor, t.config.Writer.Protocol)

	// 输出数据规模信息
	tableCount := len(t.config.Reader.Tables)
	t.logger.Infof("Data Scale: %d tables to synchronize", tableCount)
	for i, table := range t.config.Reader.Tables {
		targetTable := t.config.Writer.Tables[i]
		t.logger.Infof("  Table %d: %s.%s -> %s.%s", i+1,
			t.config.Reader.Database, table,
			t.config.Writer.Database, targetTable)
	}

	// 输出同步设置和限制
	t.logger.Infof("Sync Settings:")
	t.logger.Infof("  Batch Size: %d rows per batch", t.config.Settings.BatchSize)
	t.logger.Infof("  Parallel Tables: %d", t.config.Settings.ParallelTables)

	if t.config.Settings.RateLimit.MaxBytesPerSecond > 0 {
		t.logger.Infof("  Rate Limit: %.2f MB/s max throughput",
			float64(t.config.Settings.RateLimit.MaxBytesPerSecond)/1024/1024)
	}
	if t.config.Settings.RateLimit.MaxRowsPerSecond > 0 {
		t.logger.Infof("  Rate Limit: %d rows/s max", t.config.Settings.RateLimit.MaxRowsPerSecond)
	}

	// 输出时间范围设置
	if t.config.Settings.DataRange.StartTime != "" || t.config.Settings.DataRange.EndTime != "" {
		t.logger.Infof("  Data Range:")
		if t.config.Settings.DataRange.TimeColumn != "" {
			t.logger.Infof("    Time Column: %s", t.config.Settings.DataRange.TimeColumn)
		}
		if t.config.Settings.DataRange.StartTime != "" {
			t.logger.Infof("    Start Time: %s", t.config.Settings.DataRange.StartTime)
		}
		if t.config.Settings.DataRange.EndTime != "" {
			t.logger.Infof("    End Time: %s", t.config.Settings.DataRange.EndTime)
		} else {
			t.logger.Infof("    End Time: Current (real-time sync)")
		}
	}

	// 输出重试设置
	t.logger.Infof("  Retry Policy: Max %d retries, initial delay %v, max delay %v, backoff %.1fx",
		t.config.Settings.Retry.MaxRetries,
		t.config.Settings.Retry.InitialDelay,
		t.config.Settings.Retry.MaxDelay,
		t.config.Settings.Retry.BackoffFactor)

	t.logger.Infof("===========================================")

	// 检查源表和目标表数量是否匹配
	if len(t.config.Reader.Tables) != len(t.config.Writer.Tables) {
		err := fmt.Errorf("source tables count (%d) does not match target tables count (%d)",
			len(t.config.Reader.Tables), len(t.config.Writer.Tables))
		t.recordError(err)
		return err
	}

	t.logger.Infof("✓ Table count validation passed: %d tables configured", tableCount)

	// 根据并行表数量决定执行策略
	parallelTables := t.config.Settings.ParallelTables
	if parallelTables <= 1 {
		t.logger.Infof("✓ Using sequential execution strategy (1 table at a time)")
		return t.executeSequential(ctx)
	}

	t.logger.Infof("✓ Using parallel execution strategy (%d tables concurrently)", parallelTables)
	return t.executeParallel(ctx, parallelTables)
}

// executeSequential 顺序执行多表同步
func (t *SyncTask) executeSequential(ctx context.Context) error {
	tableCount := len(t.config.Reader.Tables)
	t.logger.Infof("Starting sequential sync execution for %d tables...", tableCount)

	startTime := time.Now()

	for i := 0; i < tableCount; i++ {
		tableStartTime := time.Now()
		t.logger.Infof("Processing table %d/%d: %s", i+1, tableCount, t.config.Reader.Tables[i])

		if err := t.syncTable(ctx, i); err != nil {
			t.recordError(err)
			t.logger.Errorf("✗ Failed to sync table %s: %v", t.config.Reader.Tables[i], err)
			return fmt.Errorf("failed to sync table %s: %w", t.config.Reader.Tables[i], err)
		}

		tableDuration := time.Since(tableStartTime)
		t.logger.Infof("✓ Table %s completed in %v", t.config.Reader.Tables[i], tableDuration)
	}

	totalDuration := time.Since(startTime)
	t.logger.Infof("✓ Sequential execution completed successfully")
	t.logger.Infof("Total sync duration: %v for %d tables", totalDuration, tableCount)

	t.recordSuccess()
	return nil
}

// executeParallel 并行执行多表同步
func (t *SyncTask) executeParallel(ctx context.Context, parallelCount int) error {
	tableCount := len(t.config.Reader.Tables)
	t.logger.Infof("Starting parallel sync execution for %d tables with %d workers...", tableCount, parallelCount)

	startTime := time.Now()
	semaphore := make(chan struct{}, parallelCount)
	errorChan := make(chan error, tableCount)
	var wg sync.WaitGroup

	// 启动工作协程
	for i := 0; i < tableCount; i++ {
		wg.Add(1)
		go func(tableIndex int) {
			defer wg.Done()

			semaphore <- struct{}{}        // 获取信号量
			defer func() { <-semaphore }() // 释放信号量

			tableName := t.config.Reader.Tables[tableIndex]
			tableStartTime := time.Now()
			t.logger.Infof("Worker started for table %d: %s", tableIndex+1, tableName)

			if err := t.syncTable(ctx, tableIndex); err != nil {
				errorChan <- fmt.Errorf("table %s: %w", tableName, err)
				t.logger.Errorf("✗ Worker failed for table %s: %v", tableName, err)
				return
			}

			tableDuration := time.Since(tableStartTime)
			t.logger.Infof("✓ Worker completed for table %s in %v", tableName, tableDuration)
		}(i)
	}

	wg.Wait()
	close(errorChan)

	// 收集错误
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		err := fmt.Errorf("parallel sync failed with %d errors: %v", len(errors), errors[0])
		t.recordError(err)
		t.logger.Errorf("✗ Parallel execution failed: %d out of %d tables failed", len(errors), tableCount)
		return err
	}

	totalDuration := time.Since(startTime)
	t.logger.Infof("✓ Parallel execution completed successfully")
	t.logger.Infof("Total sync duration: %v for %d tables (max %d concurrent)", totalDuration, tableCount, parallelCount)

	t.recordSuccess()
	return nil
}

// syncTable 同步单张表
func (t *SyncTask) syncTable(ctx context.Context, tableIndex int) error {
	sourceTable := t.config.Reader.Tables[tableIndex]
	targetTable := t.config.Writer.Tables[tableIndex]
	syncStartTime := time.Now()

	t.logger.Infof("🔄 Starting table sync: %s.%s -> %s.%s",
		t.config.Reader.Database, sourceTable,
		t.config.Writer.Database, targetTable)

	// 构建数据查询和同步流程
	t.logger.Debugf("Building data converter and select query...")
	converter := NewDataConverter(t.config)
	query := converter.BuildSelectQuery(t.config.Reader.Database, sourceTable)

	t.logger.Infof("📋 Query prepared: %s", query)
	t.logger.Infof("📡 Protocol mapping: %s (%s) -> %s (%s)",
		t.config.Reader.Vendor, t.config.Reader.Protocol,
		t.config.Writer.Vendor, t.config.Writer.Protocol)

	// 1. 根据协议配置验证连接信息
	t.logger.Debugf("Stage 1: Validating connection configurations...")
	if err := t.validateConnectionConfigs(); err != nil {
		t.logger.Errorf("✗ Connection validation failed: %v", err)
		return fmt.Errorf("connection validation failed: %w", err)
	}
	t.logger.Infof("✓ Stage 1 completed: Connection configurations validated")

	// 2. 执行数据同步
	t.logger.Infof("Stage 2: Starting data synchronization process...")
	dataStartTime := time.Now()

	rowCount, processedBytes, err := t.processDataSync(ctx, sourceTable, targetTable, query)
	if err != nil {
		t.logger.Errorf("✗ Data synchronization failed: %v", err)
		return fmt.Errorf("data processing failed: %w", err)
	}

	dataDuration := time.Since(dataStartTime)
	t.logger.Infof("✓ Stage 2 completed: Data synchronization finished in %v", dataDuration)

	// 3. 更新统计信息
	t.logger.Debugf("Stage 3: Updating task statistics...")
	t.mutex.Lock()
	t.totalRows += rowCount
	t.totalBytes += processedBytes
	t.mutex.Unlock()

	syncDuration := time.Since(syncStartTime)

	// 计算性能指标
	rowsPerSec := float64(rowCount) / syncDuration.Seconds()
	mbPerSec := float64(processedBytes) / syncDuration.Seconds() / 1024 / 1024

	t.logger.Infof("✅ Table sync completed successfully:")
	t.logger.Infof("   📊 Data volume: %d rows (%s)",
		rowCount, formatBytes(processedBytes))
	t.logger.Infof("   ⏱️ Duration: %v", syncDuration)
	t.logger.Infof("   🚀 Performance: %.0f rows/sec, %.2f MB/sec", rowsPerSec, mbPerSec)
	t.logger.Infof("   🎯 Source: %s.%s (%s)",
		t.config.Reader.Database, sourceTable, t.config.Reader.Vendor)
	t.logger.Infof("   🎯 Target: %s.%s (%s)",
		t.config.Writer.Database, targetTable, t.config.Writer.Vendor)

	return nil
}

// formatBytes 格式化字节数显示
func formatBytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	} else if bytes < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	} else if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(bytes)/1024/1024)
	} else {
		return fmt.Sprintf("%.2f GB", float64(bytes)/1024/1024/1024)
	}
}

// validateConnectionConfigs 验证连接配置 - 基于vendor简化实现
func (t *SyncTask) validateConnectionConfigs() error {
	t.logger.Debugf("Validating source database configuration...")
	// 验证源数据库配置
	sourceConfig, err := t.getVendorConfig(t.config.Reader.Vendor, t.config.Reader.Name)
	if err != nil {
		t.logger.Errorf("Source config retrieval failed for %s (%s): %v",
			t.config.Reader.Name, t.config.Reader.Vendor, err)
		return fmt.Errorf("reader config validation failed: %w", err)
	}

	if err := t.validateVendorProtocolConfig(sourceConfig, t.config.Reader.Vendor, t.config.Reader.Protocol); err != nil {
		t.logger.Errorf("Source protocol validation failed for %s protocol on %s: %v",
			t.config.Reader.Protocol, t.config.Reader.Vendor, err)
		return fmt.Errorf("reader protocol validation failed: %w", err)
	}

	t.logger.Debugf("✓ Source database configuration validated")

	t.logger.Debugf("Validating target database configuration...")
	// 验证目标数据库配置
	targetConfig, err := t.getVendorConfig(t.config.Writer.Vendor, t.config.Writer.Name)
	if err != nil {
		t.logger.Errorf("Target config retrieval failed for %s (%s): %v",
			t.config.Writer.Name, t.config.Writer.Vendor, err)
		return fmt.Errorf("writer config validation failed: %w", err)
	}

	if err := t.validateVendorProtocolConfig(targetConfig, t.config.Writer.Vendor, t.config.Writer.Protocol); err != nil {
		t.logger.Errorf("Target protocol validation failed for %s protocol on %s: %v",
			t.config.Writer.Protocol, t.config.Writer.Vendor, err)
		return fmt.Errorf("writer protocol validation failed: %w", err)
	}

	t.logger.Debugf("✓ Target database configuration validated")

	t.logger.Infof("Connection configuration validation successful: %s(%s) -> %s(%s)",
		t.config.Reader.Vendor, t.config.Reader.Protocol,
		t.config.Writer.Vendor, t.config.Writer.Protocol)

	return nil
}

// validateVendorProtocolConfig 验证vendor和协议配置的匹配性
func (t *SyncTask) validateVendorProtocolConfig(dbConfig interface{}, vendor, protocol string) error {
	switch vendor {
	case "clickhouse":
		chConfig := dbConfig.(*config.ClickHouseConfig)
		switch protocol {
		case "mysql":
			if chConfig.MySQL == nil {
				return fmt.Errorf("ClickHouse MySQL config is required")
			}
		case "http":
			if chConfig.HTTP == nil {
				return fmt.Errorf("ClickHouse HTTP config is required")
			}
		default:
			return fmt.Errorf("unsupported ClickHouse protocol: %s", protocol)
		}
	case "starrocks":
		srConfig := dbConfig.(*config.StarRocksConfig)
		switch protocol {
		case "mysql":
			if srConfig.MySQL == nil {
				return fmt.Errorf("StarRocks MySQL config is required")
			}
		case "flightsql":
			if srConfig.FlightSQL == nil {
				return fmt.Errorf("StarRocks FlightSQL config is required")
			}
		case "http":
			if srConfig.HTTP == nil {
				return fmt.Errorf("StarRocks HTTP config is required")
			}
		default:
			return fmt.Errorf("unsupported StarRocks protocol: %s", protocol)
		}
	default:
		return fmt.Errorf("unsupported vendor: %s", vendor)
	}
	return nil
}

// processDataSync 处理数据同步 - 基于vendor配置简化实现
func (t *SyncTask) processDataSync(ctx context.Context, sourceTable, targetTable, query string) (int64, int64, error) {
	t.logger.Infof("Processing data sync for table: %s -> %s", sourceTable, targetTable)

	// 1. 根据vendor获取数据库配置
	t.logger.Debugf("Step 2.1: Retrieving database configurations...")
	sourceConfig, err := t.getVendorConfig(t.config.Reader.Vendor, t.config.Reader.Name)
	if err != nil {
		t.logger.Errorf("Failed to get source config for %s: %v", t.config.Reader.Name, err)
		return 0, 0, fmt.Errorf("failed to get source config: %w", err)
	}

	targetConfig, err := t.getVendorConfig(t.config.Writer.Vendor, t.config.Writer.Name)
	if err != nil {
		t.logger.Errorf("Failed to get target config for %s: %v", t.config.Writer.Name, err)
		return 0, 0, fmt.Errorf("failed to get target config: %w", err)
	}

	t.logger.Infof("✓ Step 2.1: Database configurations retrieved")
	t.logger.Debugf("Source: %s (%s protocol) -> Target: %s (%s protocol)",
		t.config.Reader.Vendor, t.config.Reader.Protocol,
		t.config.Writer.Vendor, t.config.Writer.Protocol)

	// 2. 验证数据源连接配置
	t.logger.Debugf("Step 2.2: Validating vendor connections...")
	if err := t.validateVendorConnection(sourceConfig, t.config.Reader.Vendor, t.config.Reader.Protocol); err != nil {
		t.logger.Errorf("Source connection validation failed: %v", err)
		return 0, 0, fmt.Errorf("source connection validation failed: %w", err)
	}

	if err := t.validateVendorConnection(targetConfig, t.config.Writer.Vendor, t.config.Writer.Protocol); err != nil {
		t.logger.Errorf("Target connection validation failed: %v", err)
		return 0, 0, fmt.Errorf("target connection validation failed: %w", err)
	}
	t.logger.Infof("✓ Step 2.2: Vendor connections validated")

	// 2.5. 检查并创建目标数据库和表
	t.logger.Debugf("Step 2.5: Checking and creating target database/table...")
	if err := t.ensureTargetDatabaseAndTable(ctx, sourceConfig, targetConfig, sourceTable, targetTable); err != nil {
		t.logger.Errorf("Failed to ensure target database/table: %v", err)
		return 0, 0, fmt.Errorf("failed to ensure target database/table: %w", err)
	}
	t.logger.Infof("✓ Step 2.5: Target database/table ensured")

	// 3. 创建数据读取器
	t.logger.Debugf("Step 2.3: Creating data reader...")
	readerStartTime := time.Now()

	reader, err := t.createDataReader(ctx, sourceConfig, sourceTable, query)
	if err != nil {
		t.logger.Errorf("Failed to create data reader: %v", err)
		return 0, 0, fmt.Errorf("failed to create data reader: %w", err)
	}
	defer reader.Close()

	readerDuration := time.Since(readerStartTime)
	t.logger.Infof("✓ Step 2.3: Data reader created in %v (%s protocol)",
		readerDuration, t.config.Reader.Protocol)

	// 4. 创建数据写入器
	t.logger.Debugf("Step 2.4: Creating data writer...")
	writerStartTime := time.Now()

	writer, err := t.createDataWriter(ctx, targetConfig, targetTable)
	if err != nil {
		t.logger.Errorf("Failed to create data writer: %v", err)
		return 0, 0, fmt.Errorf("failed to create data writer: %w", err)
	}
	defer writer.Close()

	writerDuration := time.Since(writerStartTime)
	t.logger.Infof("✓ Step 2.4: Data writer created in %v (%s protocol)",
		writerDuration, t.config.Writer.Protocol)

	// 5. 执行数据传输
	t.logger.Infof("Step 2.6: Starting data transfer process...")
	transferStartTime := time.Now()

	rowCount, byteCount, err := t.transferData(ctx, reader, writer)
	if err != nil {
		t.logger.Errorf("Data transfer failed: %v", err)
		return 0, 0, fmt.Errorf("data transfer failed: %w", err)
	}

	transferDuration := time.Since(transferStartTime)
	t.logger.Infof("✓ Step 2.6: Data transfer completed in %v", transferDuration)

	return rowCount, byteCount, nil
}

// getVendorConfig 根据vendor获取对应的数据库配置
func (t *SyncTask) getVendorConfig(vendor, name string) (interface{}, error) {
	switch vendor {
	case "clickhouse":
		return t.globalConfig.GetClickHouseConfig(name)
	case "starrocks":
		return t.globalConfig.GetStarRocksConfig(name)
	default:
		return nil, fmt.Errorf("unsupported vendor: %s", vendor)
	}
}

// validateVendorConnection 验证vendor连接配置
func (t *SyncTask) validateVendorConnection(dbConfig interface{}, vendor, protocol string) error {
	switch vendor {
	case "clickhouse":
		chConfig := dbConfig.(*config.ClickHouseConfig)
		switch protocol {
		case "mysql":
			if chConfig.MySQL == nil {
				return fmt.Errorf("ClickHouse MySQL configuration is missing")
			}
			fmt.Printf("Validating ClickHouse MySQL connection to %s:%d\n",
				chConfig.MySQL.Host, chConfig.MySQL.Port)
		case "http":
			if chConfig.HTTP == nil {
				return fmt.Errorf("ClickHouse HTTP configuration is missing")
			}
			fmt.Printf("Validating ClickHouse HTTP connection to %s:%d\n",
				chConfig.HTTP.Host, chConfig.HTTP.Port)
		}
	case "starrocks":
		srConfig := dbConfig.(*config.StarRocksConfig)
		switch protocol {
		case "mysql":
			if srConfig.MySQL == nil {
				return fmt.Errorf("StarRocks MySQL configuration is missing")
			}
			fmt.Printf("Validating StarRocks MySQL connection to %s:%d\n",
				srConfig.MySQL.Host, srConfig.MySQL.Port)
		case "flightsql":
			if srConfig.FlightSQL == nil {
				return fmt.Errorf("StarRocks FlightSQL configuration is missing")
			}
			fmt.Printf("Validating StarRocks FlightSQL connection to %s:%d\n",
				srConfig.FlightSQL.Host, srConfig.FlightSQL.Port)
		case "http":
			if srConfig.HTTP == nil {
				return fmt.Errorf("StarRocks HTTP configuration is missing")
			}
			fmt.Printf("Validating StarRocks HTTP connection to %s:%d\n",
				srConfig.HTTP.Host, srConfig.HTTP.Port)
		}
	}
	return nil
}

// createDataReader 创建数据读取器
func (t *SyncTask) createDataReader(ctx context.Context, dbConfig interface{}, sourceTable, query string) (DataReader, error) {
	switch t.config.Reader.Vendor {
	case "clickhouse":
		return t.createClickHouseReader(ctx, dbConfig.(*config.ClickHouseConfig), sourceTable, query)
	case "starrocks":
		return t.createStarRocksReader(ctx, dbConfig.(*config.StarRocksConfig), sourceTable, query)
	default:
		return nil, fmt.Errorf("unsupported reader vendor: %s", t.config.Reader.Vendor)
	}
}

// createClickHouseReader 创建ClickHouse数据读取器
func (t *SyncTask) createClickHouseReader(ctx context.Context, chConfig *config.ClickHouseConfig, sourceTable, query string) (DataReader, error) {
	switch t.config.Reader.Protocol {
	case "mysql":
		return t.createClickHouseMySQLReader(ctx, chConfig, sourceTable, query)
	default:
		return nil, fmt.Errorf("unsupported ClickHouse protocol: %s", t.config.Reader.Protocol)
	}
}

// createClickHouseMySQLReader 创建ClickHouse MySQL读取器
func (t *SyncTask) createClickHouseMySQLReader(ctx context.Context, chConfig *config.ClickHouseConfig, sourceTable, query string) (*MySQLDataReader, error) {
	if chConfig.MySQL == nil {
		return nil, fmt.Errorf("ClickHouse MySQL configuration is missing")
	}

	// 构建MySQL DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true",
		chConfig.MySQL.Username, chConfig.MySQL.Password,
		chConfig.MySQL.Host, chConfig.MySQL.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse MySQL connection: %w", err)
	}

	// 设置连接超时
	db.SetConnMaxLifetime(chConfig.MySQL.Timeout)

	// 测试连接
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping ClickHouse MySQL: %w", err)
	}

	// 执行查询
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return &MySQLDataReader{
		rows:   rows,
		logger: logrus.New(),
	}, nil
}

// createStarRocksReader 创建StarRocks数据读取器
func (t *SyncTask) createStarRocksReader(ctx context.Context, srConfig *config.StarRocksConfig, sourceTable, query string) (DataReader, error) {
	switch t.config.Reader.Protocol {
	case "flightsql":
		return t.createStarRocksFlightSQLReader(ctx, srConfig, sourceTable, query)
	case "mysql":
		return t.createStarRocksMySQLReader(ctx, srConfig, sourceTable, query)
	default:
		return nil, fmt.Errorf("unsupported StarRocks protocol: %s", t.config.Reader.Protocol)
	}
}

// createStarRocksFlightSQLReader 创建StarRocks FlightSQL读取器
func (t *SyncTask) createStarRocksFlightSQLReader(ctx context.Context, srConfig *config.StarRocksConfig, sourceTable, query string) (*FlightSQLDataReader, error) {
	if srConfig.FlightSQL == nil {
		return nil, fmt.Errorf("StarRocks FlightSQL configuration is missing")
	}

	t.logger.Infof("Creating Arrow FlightSQL connection to %s:%d", srConfig.FlightSQL.Host, srConfig.FlightSQL.Port)

	// 构建Arrow Flight SQL连接字符串 - 参考Java示例
	uri := fmt.Sprintf("grpc+tcp://%s:%d", srConfig.FlightSQL.Host, srConfig.FlightSQL.Port)

	// 创建FlightSQL驱动选项
	options := map[string]string{
		"uri":      uri,
		"username": srConfig.FlightSQL.Username,
		"password": srConfig.FlightSQL.Password,
	}

	// 添加TLS配置
	if srConfig.FlightSQL.TLS.Enabled {
		options["adbc.flight.sql.client_option.tls_skip_verify"] = strconv.FormatBool(srConfig.FlightSQL.TLS.SkipVerify)
		if srConfig.FlightSQL.TLS.CertFile != "" {
			options["adbc.flight.sql.client_option.tls_cert_chain"] = srConfig.FlightSQL.TLS.CertFile
		}
		if srConfig.FlightSQL.TLS.KeyFile != "" {
			options["adbc.flight.sql.client_option.tls_private_key"] = srConfig.FlightSQL.TLS.KeyFile
		}
		if srConfig.FlightSQL.TLS.CAFile != "" {
			options["adbc.flight.sql.client_option.tls_root_certs"] = srConfig.FlightSQL.TLS.CAFile
		}
	} else {
		options["adbc.flight.sql.client_option.tls_skip_verify"] = "true"
	}

	// 创建FlightSQL驱动 - 使用默认内存分配器
	var driver adbc.Driver = flightsql.NewDriver(memory.DefaultAllocator)

	// 创建ADBC数据库实例
	adbcDB, err := driver.NewDatabase(options)
	if err != nil {
		return nil, fmt.Errorf("failed to create ADBC database: %w", err)
	}

	// 创建ADBC连接
	conn, err := adbcDB.Open(ctx)
	if err != nil {
		adbcDB.Close()
		return nil, fmt.Errorf("failed to open ADBC connection: %w", err)
	}

	t.logger.Infof("Executing FlightSQL query: %s", query)

	// 创建查询Statement
	srcStmt, err := conn.NewStatement()
	if err != nil {
		conn.Close()
		adbcDB.Close()
		return nil, fmt.Errorf("failed to create source statement: %w", err)
	}

	// 设置查询语句
	if err := srcStmt.SetSqlQuery(query); err != nil {
		srcStmt.Close()
		conn.Close()
		adbcDB.Close()
		return nil, fmt.Errorf("failed to set source query: %w", err)
	}

	// 执行查询并获取Arrow Record Reader - 直接零拷贝传输
	reader, _, err := srcStmt.ExecuteQuery(ctx)
	if err != nil {
		srcStmt.Close()
		conn.Close()
		adbcDB.Close()
		return nil, fmt.Errorf("failed to execute source query: %w", err)
	}

	t.logger.Infof("FlightSQL query executed successfully, ready for zero-copy Arrow data transfer")

	return &FlightSQLDataReader{
		reader:    reader,
		allocator: memory.DefaultAllocator,
		logger:    t.logger,
	}, nil
}

// createStarRocksMySQLReader 创建StarRocks MySQL读取器
func (t *SyncTask) createStarRocksMySQLReader(ctx context.Context, srConfig *config.StarRocksConfig, sourceTable, query string) (*MySQLDataReader, error) {
	if srConfig.MySQL == nil {
		return nil, fmt.Errorf("StarRocks MySQL configuration is missing")
	}

	// 构建MySQL DSN
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true",
		srConfig.MySQL.Username, srConfig.MySQL.Password,
		srConfig.MySQL.Host, srConfig.MySQL.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open StarRocks MySQL connection: %w", err)
	}

	// 设置连接超时
	db.SetConnMaxLifetime(srConfig.MySQL.Timeout)

	// 测试连接
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping StarRocks MySQL: %w", err)
	}

	// 执行查询
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return &MySQLDataReader{
		rows:   rows,
		logger: logrus.New(),
	}, nil
}

// createDataWriter 创建数据写入器
func (t *SyncTask) createDataWriter(ctx context.Context, dbConfig interface{}, targetTable string) (DataWriter, error) {
	switch t.config.Writer.Vendor {
	case "clickhouse":
		return t.createClickHouseWriter(ctx, dbConfig.(*config.ClickHouseConfig), targetTable)
	case "starrocks":
		return t.createStarRocksWriter(ctx, dbConfig.(*config.StarRocksConfig), targetTable)
	default:
		return nil, fmt.Errorf("unsupported writer vendor: %s", t.config.Writer.Vendor)
	}
}

// createClickHouseWriter 创建ClickHouse数据写入器
func (t *SyncTask) createClickHouseWriter(ctx context.Context, chConfig *config.ClickHouseConfig, targetTable string) (DataWriter, error) {
	switch t.config.Writer.Protocol {
	case "http":
		return t.createClickHouseHTTPWriter(ctx, chConfig, targetTable)
	default:
		return nil, fmt.Errorf("unsupported ClickHouse writer protocol: %s", t.config.Writer.Protocol)
	}
}

// createClickHouseHTTPWriter 创建ClickHouse HTTP写入器
func (t *SyncTask) createClickHouseHTTPWriter(ctx context.Context, chConfig *config.ClickHouseConfig, targetTable string) (*HTTPDataWriter, error) {
	if chConfig.HTTP == nil {
		return nil, fmt.Errorf("ClickHouse HTTP configuration is missing")
	}

	endpoint := fmt.Sprintf("http://%s:%d/", chConfig.HTTP.Host, chConfig.HTTP.Port)

	// 创建HTTP客户端
	client := &http.Client{
		Timeout: chConfig.HTTP.Timeout,
	}

	// 构建认证字符串
	auth := fmt.Sprintf("%s:%s", chConfig.HTTP.Username, chConfig.HTTP.Password)

	return &HTTPDataWriter{
		client:      client,
		endpoint:    endpoint,
		table:       fmt.Sprintf("%s.%s", t.config.Writer.Database, targetTable),
		auth:        auth,
		columnNames: nil, // 稍后可以通过SetColumnNames方法设置
		logger:      logrus.New(),
	}, nil
}

// createStarRocksWriter 创建StarRocks数据写入器
func (t *SyncTask) createStarRocksWriter(ctx context.Context, srConfig *config.StarRocksConfig, targetTable string) (DataWriter, error) {
	switch t.config.Writer.Protocol {
	case "http":
		return t.createStarRocksHTTPWriter(ctx, srConfig, targetTable)
	default:
		return nil, fmt.Errorf("unsupported StarRocks writer protocol: %s", t.config.Writer.Protocol)
	}
}

// createStarRocksHTTPWriter 创建StarRocks HTTP写入器
func (t *SyncTask) createStarRocksHTTPWriter(ctx context.Context, srConfig *config.StarRocksConfig, targetTable string) (*HTTPDataWriter, error) {
	if srConfig.HTTP == nil {
		return nil, fmt.Errorf("StarRocks HTTP configuration is missing")
	}

	endpoint := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load",
		srConfig.HTTP.Host, srConfig.HTTP.Port,
		t.config.Writer.Database, targetTable)

	// 创建HTTP客户端
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        5,                // 全局最大空闲连接
			MaxIdleConnsPerHost: 10,               // 单主机最大空闲连接（关键！）
			MaxConnsPerHost:     50,               // 单主机最大总连接
			IdleConnTimeout:     60 * time.Second, // 空闲超时
			// 其他优化参数
			TLSHandshakeTimeout: 10 * time.Second,
		},
		Timeout: srConfig.HTTP.Timeout,
		// 解决starRocks的FE重定向到BE时认证丢失问题
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			SetStarRocksStreamLoadHeaders(req, srConfig.HTTP.Username, srConfig.HTTP.Password)
			return nil // 返回 nil 表示信任所有重定向
		},
	}

	// 构建认证字符串
	auth := fmt.Sprintf("%s:%s", srConfig.HTTP.Username, srConfig.HTTP.Password)

	return &HTTPDataWriter{
		client:      client,
		endpoint:    endpoint,
		table:       targetTable,
		auth:        auth,
		userName:    srConfig.HTTP.Username,
		password:    srConfig.HTTP.Password,
		columnNames: nil, // 稍后可以通过SetColumnNames方法设置
		logger:      logrus.New(),
	}, nil
}

// transferData 执行数据传输
func (t *SyncTask) transferData(ctx context.Context, reader DataReader, writer DataWriter) (int64, int64, error) {
	var totalRows int64
	var totalBytes int64
	batchSize := t.config.Settings.BatchSize

	// 如果writer是HTTPDataWriter，尝试设置列名
	if httpWriter, ok := writer.(*HTTPDataWriter); ok && strings.Contains(httpWriter.endpoint, "/api/") {
		// 这是StarRocks Stream Load，尝试获取列名
		columnNames, err := t.getColumnNamesForTable(ctx, t.config.Writer.Tables[0]) // 假设是第一个表
		if err != nil {
			t.logger.Warnf("Failed to get column names for JSON format: %v, using array format", err)
		} else {
			httpWriter.SetColumnNames(columnNames)
			t.logger.Debugf("Set column names for JSON format: %v", columnNames)
		}
	}

	t.logger.Infof("Starting data transfer with batch size: %d rows", batchSize)

	batch := make([]interface{}, 0, batchSize)
	batchCount := 0
	transferStartTime := time.Now()

	for reader.Next() {
		select {
		case <-ctx.Done():
			t.logger.Warnf("Data transfer cancelled by context")
			return totalRows, totalBytes, ctx.Err()
		default:
		}

		// 获取记录（已经是JSON序列化的数据）
		record, err := reader.GetRecord()
		if err != nil {
			t.logger.Errorf("Failed to get record from reader: %v", err)
			return totalRows, totalBytes, fmt.Errorf("failed to get record from reader: %w", err)
		}

		// 添加到批次
		batch = append(batch, record)

		// 检查批次大小
		if len(batch) >= batchSize {
			batchCount++
			batchStartTime := time.Now()

			// 写入批次
			if err := writer.Write(ctx, batch); err != nil {
				t.logger.Errorf("Failed to write batch %d: %v", batchCount, err)
				return totalRows, totalBytes, fmt.Errorf("failed to write batch: %w", err)
			}

			// 更新统计
			rowCount := int64(len(batch))
			byteCount := t.estimateBatchSize(batch)

			totalRows += rowCount
			totalBytes += byteCount

			batchDuration := time.Since(batchStartTime)
			batchRowsPerSec := float64(rowCount) / batchDuration.Seconds()
			batchMBPerSec := float64(byteCount) / batchDuration.Seconds() / 1024 / 1024

			// 每10个批次或每10秒输出一次进度
			if batchCount%10 == 0 || batchDuration > 10*time.Second {
				overallDuration := time.Since(transferStartTime)
				overallRowsPerSec := float64(totalRows) / overallDuration.Seconds()
				overallMBPerSec := float64(totalBytes) / overallDuration.Seconds() / 1024 / 1024

				t.logger.Infof("📊 Batch %d progress: %d rows (%s) in %v (%.0f rows/sec, %.2f MB/sec)",
					batchCount, rowCount, formatBytes(byteCount), batchDuration, batchRowsPerSec, batchMBPerSec)
				t.logger.Infof("📈 Overall progress: %d total rows (%s) in %v (avg %.0f rows/sec, %.2f MB/sec)",
					totalRows, formatBytes(totalBytes), overallDuration, overallRowsPerSec, overallMBPerSec)
			}

			// 清空批次
			batch = batch[:0]

			// 应用速率限制
			if t.config.Settings.RateLimit.MaxBytesPerSecond > 0 || t.config.Settings.RateLimit.MaxRowsPerSecond > 0 {
				time.Sleep(time.Millisecond * 10) // 简单的速率控制
			}
		}
	}

	// 处理剩余的数据
	if len(batch) > 0 {
		batchCount++
		batchStartTime := time.Now()

		t.logger.Debugf("Processing final batch with %d remaining rows", len(batch))

		if err := writer.Write(ctx, batch); err != nil {
			t.logger.Errorf("Failed to write final batch: %v", err)
			return totalRows, totalBytes, fmt.Errorf("failed to write final batch: %w", err)
		}

		rowCount := int64(len(batch))
		byteCount := t.estimateBatchSize(batch)

		totalRows += rowCount
		totalBytes += byteCount

		finalBatchDuration := time.Since(batchStartTime)
		t.logger.Debugf("Final batch processed: %d rows in %v", rowCount, finalBatchDuration)
	}

	totalTransferDuration := time.Since(transferStartTime)
	avgRowsPerSec := float64(totalRows) / totalTransferDuration.Seconds()
	avgMBPerSec := float64(totalBytes) / totalTransferDuration.Seconds() / 1024 / 1024

	t.logger.Infof("✅ Data transfer completed successfully:")
	t.logger.Infof("   📊 Total: %d rows (%s) processed in %d batches",
		totalRows, formatBytes(totalBytes), batchCount)
	t.logger.Infof("   ⏱️ Duration: %v", totalTransferDuration)
	t.logger.Infof("   🚀 Average performance: %.0f rows/sec, %.2f MB/sec", avgRowsPerSec, avgMBPerSec)

	return totalRows, totalBytes, nil
}

// estimateBatchSize 估算批次大小
func (t *SyncTask) estimateBatchSize(batch []interface{}) int64 {
	var size int64
	// TODO: 这里可以改进为更精确的JSON序列化大小估算
	// for _, record := range batch {
	// 	size += int64(len(record)) + 1 // +1 for delimiter/newline
	// }
	return size
}

// MySQLDataReader 方法实现
func (r *MySQLDataReader) Next() bool {
	return r.rows.Next()
}

func (r *MySQLDataReader) GetRecord() (interface{}, error) {
	columns, err := r.rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// 创建扫描目标
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range columns {
		valuePtrs[i] = &values[i]
	}

	// 扫描行
	err = r.rows.Scan(valuePtrs...)
	if err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	// 构建JSON对象
	jsonObj := make(map[string]interface{})
	for i, val := range values {
		columnName := columns[i]
		if val == nil {
			jsonObj[columnName] = nil
		} else {
			// 正确处理不同数据类型，特别是字节数组
			switch v := val.(type) {
			case []byte:
				// 字节数组转换为字符串，并处理特定字段
				strVal := string(v)
				jsonObj[columnName] = r.processFieldValue(columnName, strVal)
			case string:
				// 字符串直接处理特定字段
				jsonObj[columnName] = r.processFieldValue(columnName, v)
			case time.Time:
				// 时间类型特殊处理
				jsonObj[columnName] = r.processTimeField(columnName, v)
			default:
				// 其他类型转换为字符串
				strVal := fmt.Sprintf("%v", v)
				jsonObj[columnName] = r.processFieldValue(columnName, strVal)
			}
		}
	}

	return jsonObj, nil
}

// processFieldValue 处理特定字段的值
func (r *MySQLDataReader) processFieldValue(columnName, value string) string {
	switch columnName {
	case "category":
		// 去除字符串末尾的null字符
		return strings.TrimRight(value, "\x00")
	case "birth_date", "created_at", "updated_timestamp":
		// 解析时间字符串并格式化为日期
		return r.parseAndFormatDate(value)
	default:
		return value
	}
}

// processTimeField 处理时间类型字段
func (r *MySQLDataReader) processTimeField(columnName string, timeVal time.Time) string {
	switch columnName {
	case "birth_date", "created_at", "updated_timestamp":
		// 对于date类型字段，只返回日期部分 YYYY-MM-DD
		return timeVal.Format("2006-01-02")
	default:
		// 其他时间字段保持完整格式
		return timeVal.Format("2006-01-02 15:04:05")
	}
}

// parseAndFormatDate 解析字符串时间并格式化为日期
func (r *MySQLDataReader) parseAndFormatDate(dateStr string) string {
	// 尝试解析不同的时间格式
	timeFormats := []string{
		"2006-01-02 15:04:05 +0000 UTC", // "2004-01-27 00:00:00 +0000 UTC"
		"2006-01-02 15:04:05.000",       // "2025-03-05 16:33:28.000"
		"2006-01-02 15:04:05",           // "2025-04-14 16:33:28"
		"2006-01-02",                    // "2004-01-27"
	}

	for _, format := range timeFormats {
		if t, err := time.Parse(format, dateStr); err == nil {
			// 成功解析，返回日期部分
			return t.Format("2006-01-02")
		}
	}

	// 如果都解析失败，记录警告并返回原值
	r.logger.Warnf("Failed to parse date string: %s, using original value", dateStr)
	return dateStr
}

func (r *MySQLDataReader) Close() error {
	if r.rows != nil {
		return r.rows.Close()
	}
	return nil
}

// FlightSQLDataReader 方法实现
func (r *FlightSQLDataReader) Next() bool {
	// 如果当前记录还有更多行，返回true
	if r.currentRecord != nil && r.currentRowIndex < int(r.currentRecord.NumRows()) {
		return true
	}

	// 否则尝试获取下一个记录
	hasNext := r.reader.Next()
	if !hasNext {
		return false
	}

	// 释放旧记录并获取新记录
	if r.currentRecord != nil {
		r.currentRecord.Release()
	}
	r.currentRecord = r.reader.Record()
	r.currentRecord.Retain() // 保持引用
	r.currentRowIndex = 0

	return r.currentRecord != nil && r.currentRecord.NumRows() > 0
}

func (r *FlightSQLDataReader) GetRecord() (interface{}, error) {
	// 确保有当前记录可用
	if r.currentRecord == nil {
		return nil, fmt.Errorf("no current record available")
	}

	// 处理当前行
	numCols := int(r.currentRecord.NumCols())
	schema := r.currentRecord.Schema()

	// 构建JSON对象
	jsonObj := make(map[string]interface{})

	for j := 0; j < numCols; j++ {
		col := r.currentRecord.Column(j)
		columnName := schema.Field(j).Name

		// 转换Arrow数据为适当的Go类型
		var value interface{}
		switch arr := col.(type) {
		case *array.String:
			if arr.IsValid(r.currentRowIndex) {
				value = arr.Value(r.currentRowIndex)
			} else {
				value = nil
			}
		case *array.Int64:
			if arr.IsValid(r.currentRowIndex) {
				value = arr.Value(r.currentRowIndex)
			} else {
				value = nil
			}
		case *array.Int32:
			if arr.IsValid(r.currentRowIndex) {
				value = int64(arr.Value(r.currentRowIndex))
			} else {
				value = nil
			}
		case *array.Float64:
			if arr.IsValid(r.currentRowIndex) {
				value = arr.Value(r.currentRowIndex)
			} else {
				value = nil
			}
		case *array.Float32:
			if arr.IsValid(r.currentRowIndex) {
				value = float64(arr.Value(r.currentRowIndex))
			} else {
				value = nil
			}
		case *array.Boolean:
			if arr.IsValid(r.currentRowIndex) {
				value = arr.Value(r.currentRowIndex)
			} else {
				value = nil
			}
		case *array.Timestamp:
			if arr.IsValid(r.currentRowIndex) {
				ts := arr.Value(r.currentRowIndex).ToTime(arrow.TimeUnit(arr.DataType().(*arrow.TimestampType).Unit))
				value = ts.Format("2006-01-02 15:04:05")
			} else {
				value = nil
			}
		case *array.Date32:
			if arr.IsValid(r.currentRowIndex) {
				days := arr.Value(r.currentRowIndex)
				date := arrow.Date32(days).ToTime()
				value = date.Format("2006-01-02")
			} else {
				value = nil
			}
		case *array.Date64:
			if arr.IsValid(r.currentRowIndex) {
				ms := arr.Value(r.currentRowIndex)
				date := arrow.Date64(ms).ToTime()
				value = date.Format("2006-01-02")
			} else {
				value = nil
			}
		case *array.Decimal128:
			if arr.IsValid(r.currentRowIndex) {
				val := arr.Value(r.currentRowIndex)
				dec := decimal128.FromU64(val.LowBits())
				value = dec.ToString(int32(arr.DataType().(*arrow.Decimal128Type).Scale))
			} else {
				value = nil
			}
		default:
			// 其他类型使用字符串转换
			if col.IsValid(r.currentRowIndex) {
				value = fmt.Sprintf("%v", col.GetOneForMarshal(r.currentRowIndex))
			} else {
				value = nil
			}
		}

		jsonObj[columnName] = value
	}

	// 移动到下一行
	r.currentRowIndex++

	return jsonObj, nil
}

func (r *FlightSQLDataReader) Close() error {
	if r.reader != nil {
		r.reader.Release()
	}
	return nil
}

// HTTPDataWriter 方法实现
func (w *HTTPDataWriter) Write(ctx context.Context, data []interface{}) error {
	if len(data) == 0 {
		return nil
	}

	// 根据不同的数据库执行HTTP请求
	if strings.Contains(w.endpoint, "/api/") {
		// StarRocks Stream Load - 使用JSON Lines格式
		return w.writeToStarRocks(ctx, data)
	} else {
		return w.writeToClickHouse(ctx, "todo")
	}
}

// 通用的starRocks的Stream Load请求头设置
func SetStarRocksStreamLoadHeaders(req *http.Request, userName, password string) {
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("format", "json")
	req.Header.Set("strip_outer_array", "true") // 剥离JSON数组的外层数组包装
	req.Header.Set("ignore_json_size", "true")  // 忽略JSON大小限制
	req.Header.Set("fuzzy_parse", "true")       // 启用模糊解析以提高容错性
	// 移除可能导致冲突的头部设置
	req.Header.Set("compress_type", "lz4_frame") // 暂时禁用压缩避免问题
	req.Header.Set("num_as_string", "true")      // 移除强制数字为字符串，让StarRocks自动推断类型
	// 设置认证
	req.SetBasicAuth(userName, password)
}

// writeToStarRocks 写入到StarRocks
func (w *HTTPDataWriter) writeToStarRocks(ctx context.Context, data []interface{}) error {
	dataByte, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data to JSON: %w", err)
	}
	jsonBuffer := bytes.NewBuffer(dataByte)

	// 打印HTTP请求body数据用于调试
	w.logger.Infof("Put Data: %s", string(dataByte))

	req, err := http.NewRequestWithContext(ctx, "PUT", w.endpoint, jsonBuffer)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// 设置StarRocks Stream Load特定的请求头
	SetStarRocksStreamLoadHeaders(req, w.userName, w.password)

	w.logger.Infof("stream Load to StarRocks table %s at %s using JSON format", w.table, w.endpoint)

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	// 读取响应体
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// 检查HTTP状态码
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("StarRocks Stream Load failed: HTTP status=%d, body=%s", resp.StatusCode, string(body))
	}

	// 解析Stream Load响应
	var loadResponse StarRocksStreamLoadResponse
	if err := json.Unmarshal(body, &loadResponse); err != nil {
		w.logger.Warnf("Failed to parse StarRocks response JSON: %v, raw response: %s", err, string(body))
		// 如果JSON解析失败但HTTP状态码正常，仅记录警告，不失败
		w.logger.Infof("Successfully wrote %d bytes to StarRocks table %s (response parsing failed)", jsonBuffer.Len(), w.table)
		return nil
	}

	// 记录详细的加载统计信息
	w.logger.Infof("✓ StarRocks Stream Load successful for table %s:", w.table)
	w.logger.Infof("  - Transaction ID: %d", loadResponse.TxnId)
	w.logger.Infof("  - Label: %s", loadResponse.Label)
	w.logger.Infof("  - Status: %s", loadResponse.Status)
	w.logger.Infof("  - Total Rows: %d", loadResponse.NumberTotalRows)
	w.logger.Infof("  - Loaded Rows: %d", loadResponse.NumberLoadedRows)
	w.logger.Infof("  - Filtered Rows: %d", loadResponse.NumberFilteredRows)
	w.logger.Infof("  - Load Bytes: %d", loadResponse.LoadBytes)
	w.logger.Infof("  - Load Time: %dms", loadResponse.LoadTimeMs)

	if loadResponse.NumberFilteredRows > 0 {
		w.logger.Warnf("⚠️ Warning: %d rows were filtered during load", loadResponse.NumberFilteredRows)
	}

	// 验证Stream Load状态
	if err := w.validateStreamLoadResponse(&loadResponse); err != nil {
		return fmt.Errorf("StarRocks Stream Load validation failed: %w", err)
	}

	return nil
}

// validateStreamLoadResponse 验证StarRocks Stream Load响应
func (w *HTTPDataWriter) validateStreamLoadResponse(response *StarRocksStreamLoadResponse) error {
	// 验证Status字段
	if response.Status != "Success" {
		return fmt.Errorf("stream load failed with status '%s': %s, txId: %d", response.Status, response.Message, response.TxnId)
	}

	// 验证加载的行数
	if response.NumberTotalRows > 0 && response.NumberLoadedRows == 0 {
		return fmt.Errorf("no rows were loaded despite having %d total rows", response.NumberTotalRows)
	}

	// 检查是否有过多的过滤行（可选的质量检查）
	if response.NumberTotalRows > 0 {
		filteredRatio := float64(response.NumberFilteredRows) / float64(response.NumberTotalRows)
		if filteredRatio > 0.5 { // 如果超过50%的行被过滤，发出警告
			w.logger.Warnf("High filtered ratio detected: %.2f%% (%d/%d rows filtered)",
				filteredRatio*100, response.NumberFilteredRows, response.NumberTotalRows)
		}
	}

	// 验证事务ID和标签
	if response.TxnId <= 0 {
		return fmt.Errorf("invalid transaction ID: %d", response.TxnId)
	}

	if response.Label == "" {
		return fmt.Errorf("empty label in stream load response")
	}

	w.logger.Debugf("Stream Load validation passed: Status=%s, TxnId=%d, LoadedRows=%d/%d",
		response.Status, response.TxnId, response.NumberLoadedRows, response.NumberTotalRows)

	return nil
}

// getColumnNamesForTable 获取表的列名
func (t *SyncTask) getColumnNamesForTable(ctx context.Context, tableName string) ([]string, error) {
	// 从源表获取列名
	sourceConfig, err := t.getVendorConfig(t.config.Reader.Vendor, t.config.Reader.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get source config: %w", err)
	}

	// 获取源表结构
	schema, err := t.getSourceTableSchema(ctx, sourceConfig, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get source table schema: %w", err)
	}

	// 提取列名
	var columnNames []string
	for _, col := range schema {
		columnNames = append(columnNames, col.Name)
	}

	return columnNames, nil
}

// SetColumnNames 设置HTTPDataWriter的列名
func (w *HTTPDataWriter) SetColumnNames(columnNames []string) {
	w.columnNames = make([]string, len(columnNames))
	copy(w.columnNames, columnNames)
}

// writeToClickHouse 写入到ClickHouse
func (w *HTTPDataWriter) writeToClickHouse(ctx context.Context, csvData string) error {
	query := fmt.Sprintf("INSERT INTO %s FORMAT CSV", w.table)

	req, err := http.NewRequestWithContext(ctx, "POST", w.endpoint, strings.NewReader(csvData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// 设置ClickHouse HTTP头部
	req.Header.Set("Content-Type", "text/plain")
	req.URL.RawQuery = fmt.Sprintf("query=%s&user=%s&password=%s",
		query, w.extractUsername(), w.extractPassword())

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ClickHouse HTTP insert failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	w.logger.Debugf("Successfully wrote %d bytes to ClickHouse table %s", len(csvData), w.table)
	return nil
}

// encodeAuth Base64编码认证信息
func (w *HTTPDataWriter) encodeAuth() string {
	return base64.StdEncoding.EncodeToString([]byte(w.auth))
}

// extractUsername 提取用户名
func (w *HTTPDataWriter) extractUsername() string {
	parts := strings.SplitN(w.auth, ":", 2)
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}

// extractPassword 提取密码
func (w *HTTPDataWriter) extractPassword() string {
	parts := strings.SplitN(w.auth, ":", 2)
	if len(parts) > 1 {
		return parts[1]
	}
	return ""
}

func (w *HTTPDataWriter) Close() error {
	// HTTP客户端不需要显式关闭
	return nil
}

// ensureTargetDatabaseAndTable 确保目标数据库和表存在
func (t *SyncTask) ensureTargetDatabaseAndTable(ctx context.Context, sourceConfig, targetConfig interface{}, sourceTable, targetTable string) error {
	t.logger.Infof("Checking target database and table existence...")

	// 检查目标数据库是否存在
	exists, err := t.checkTargetDatabaseExists(ctx, targetConfig)
	if err != nil {
		return fmt.Errorf("failed to check target database: %w", err)
	}

	if !exists {
		t.logger.Infof("Target database '%s' does not exist, creating...", t.config.Writer.Database)
		if err := t.createTargetDatabase(ctx, targetConfig); err != nil {
			return fmt.Errorf("failed to create target database: %w", err)
		}
		t.logger.Infof("✓ Target database '%s' created successfully", t.config.Writer.Database)
	} else {
		t.logger.Infof("✓ Target database '%s' already exists", t.config.Writer.Database)
	}

	// 检查目标表是否存在
	tableExists, err := t.checkTargetTableExists(ctx, targetConfig, targetTable)
	if err != nil {
		return fmt.Errorf("failed to check target table: %w", err)
	}

	if !tableExists {
		t.logger.Infof("Target table '%s.%s' does not exist, creating...", t.config.Writer.Database, targetTable)

		// 获取源表结构
		sourceSchema, err := t.getSourceTableSchema(ctx, sourceConfig, sourceTable)
		if err != nil {
			return fmt.Errorf("failed to get source table schema: %w", err)
		}

		t.logger.Debugf("Source table schema retrieved: %d columns", len(sourceSchema))

		// 创建目标表
		if err := t.createTargetTable(ctx, targetConfig, targetTable, sourceSchema); err != nil {
			return fmt.Errorf("failed to create target table: %w", err)
		}
		t.logger.Infof("✓ Target table '%s.%s' created successfully with matching schema", t.config.Writer.Database, targetTable)
	} else {
		t.logger.Infof("✓ Target table '%s.%s' already exists", t.config.Writer.Database, targetTable)
	}

	return nil
}

// checkTargetDatabaseExists 检查目标数据库是否存在
func (t *SyncTask) checkTargetDatabaseExists(ctx context.Context, targetConfig interface{}) (bool, error) {
	switch t.config.Writer.Vendor {
	case "clickhouse":
		return t.checkClickHouseDatabaseExists(ctx, targetConfig.(*config.ClickHouseConfig))
	case "starrocks":
		return t.checkStarRocksDatabaseExists(ctx, targetConfig.(*config.StarRocksConfig))
	default:
		return false, fmt.Errorf("unsupported writer vendor: %s", t.config.Writer.Vendor)
	}
}

// checkTargetTableExists 检查目标表是否存在
func (t *SyncTask) checkTargetTableExists(ctx context.Context, targetConfig interface{}, tableName string) (bool, error) {
	switch t.config.Writer.Vendor {
	case "clickhouse":
		return t.checkClickHouseTableExists(ctx, targetConfig.(*config.ClickHouseConfig), tableName)
	case "starrocks":
		return t.checkStarRocksTableExists(ctx, targetConfig.(*config.StarRocksConfig), tableName)
	default:
		return false, fmt.Errorf("unsupported writer vendor: %s", t.config.Writer.Vendor)
	}
}

// createTargetDatabase 创建目标数据库
func (t *SyncTask) createTargetDatabase(ctx context.Context, targetConfig interface{}) error {
	switch t.config.Writer.Vendor {
	case "clickhouse":
		return t.createClickHouseDatabase(ctx, targetConfig.(*config.ClickHouseConfig))
	case "starrocks":
		return t.createStarRocksDatabase(ctx, targetConfig.(*config.StarRocksConfig))
	default:
		return fmt.Errorf("unsupported writer vendor: %s", t.config.Writer.Vendor)
	}
}

// createTargetTable 创建目标表
func (t *SyncTask) createTargetTable(ctx context.Context, targetConfig interface{}, tableName string, schema []ColumnInfo) error {
	switch t.config.Writer.Vendor {
	case "clickhouse":
		return t.createClickHouseTable(ctx, targetConfig.(*config.ClickHouseConfig), tableName, schema)
	case "starrocks":
		return t.createStarRocksTable(ctx, targetConfig.(*config.StarRocksConfig), tableName, schema)
	default:
		return fmt.Errorf("unsupported writer vendor: %s", t.config.Writer.Vendor)
	}
}

// getSourceTableSchema 获取源表结构信息
func (t *SyncTask) getSourceTableSchema(ctx context.Context, sourceConfig interface{}, tableName string) ([]ColumnInfo, error) {
	switch t.config.Reader.Vendor {
	case "clickhouse":
		return t.getClickHouseTableSchema(ctx, sourceConfig.(*config.ClickHouseConfig), tableName)
	case "starrocks":
		return t.getStarRocksTableSchema(ctx, sourceConfig.(*config.StarRocksConfig), tableName)
	default:
		return nil, fmt.Errorf("unsupported reader vendor: %s", t.config.Reader.Vendor)
	}
}

// Stop 停止任务
func (t *SyncTask) Stop() error {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.enabled = false
	if t.status == TaskStatusRunning {
		t.status = TaskStatusPaused
	}

	return nil
}

// GetStatus 获取任务状态
func (t *SyncTask) GetStatus() *TaskStatus {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return &TaskStatus{
		ID:          t.id,
		Name:        t.name,
		Enabled:     t.enabled,
		Status:      t.status,
		LastRun:     t.lastRun,
		NextRun:     t.nextRun,
		ErrorCount:  t.errorCount,
		LastError:   t.lastError,
		TotalRuns:   t.totalRuns,
		SuccessRuns: t.successRuns,
		FailedRuns:  t.failedRuns,
		TotalRows:   t.totalRows,
		TotalBytes:  t.totalBytes,
	}
}

// recordError 记录错误
func (t *SyncTask) recordError(err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.errorCount++
	t.failedRuns++
	t.lastError = err
	t.status = TaskStatusFailed
}

// recordSuccess 记录成功
func (t *SyncTask) recordSuccess() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.successRuns++
	t.lastError = nil

	// 记录成功的详细日志
	syncDuration := time.Since(t.lastRun)

	t.logger.Infof("🎉 ===== SYNC TASK COMPLETED SUCCESSFULLY =====")
	t.logger.Infof("Task: %s (ID: %s)", t.name, t.id)
	t.logger.Infof("Total Duration: %v", syncDuration)
	t.logger.Infof("Statistics Summary:")
	t.logger.Infof("  📊 Total Rows Processed: %d", t.totalRows)
	t.logger.Infof("  📦 Total Data Volume: %s", formatBytes(t.totalBytes))
	t.logger.Infof("  ✅ Success Rate: %d/%d (%.1f%%)",
		t.successRuns, t.totalRuns, float64(t.successRuns)/float64(t.totalRuns)*100)
	t.logger.Infof("  📈 Historical Stats: %d successes, %d failures, %d total runs",
		t.successRuns, t.failedRuns, t.totalRuns)

	if syncDuration.Seconds() > 0 {
		overallRowsPerSec := float64(t.totalRows) / syncDuration.Seconds()
		overallMBPerSec := float64(t.totalBytes) / syncDuration.Seconds() / 1024 / 1024
		t.logger.Infof("  🚀 Overall Performance: %.0f rows/sec, %.2f MB/sec", overallRowsPerSec, overallMBPerSec)
	}

	t.logger.Infof("============================================")

	// errorCount 不重置，保持历史错误计数
}

// TaskStatus 任务状态
type TaskStatus struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Enabled     bool           `json:"enabled"`
	Status      TaskStatusType `json:"status"`
	LastRun     time.Time      `json:"last_run"`
	NextRun     time.Time      `json:"next_run"`
	ErrorCount  int            `json:"error_count"`
	LastError   error          `json:"last_error,omitempty"`
	TotalRuns   int64          `json:"total_runs"`
	SuccessRuns int64          `json:"success_runs"`
	FailedRuns  int64          `json:"failed_runs"`
	TotalRows   int64          `json:"total_rows"`
	TotalBytes  int64          `json:"total_bytes"`
}

// ClickHouse数据库和表操作方法

// checkClickHouseDatabaseExists 检查ClickHouse数据库是否存在
func (t *SyncTask) checkClickHouseDatabaseExists(ctx context.Context, config *config.ClickHouseConfig) (bool, error) {
	if config.MySQL == nil {
		return false, fmt.Errorf("ClickHouse MySQL configuration is missing")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		config.MySQL.Username, config.MySQL.Password,
		config.MySQL.Host, config.MySQL.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return false, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return false, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	query := fmt.Sprintf("SELECT count() FROM system.databases WHERE name = '%s'", t.config.Writer.Database)
	var count int
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to check database existence: %w", err)
	}

	return count > 0, nil
}

// checkClickHouseTableExists 检查ClickHouse表是否存在
func (t *SyncTask) checkClickHouseTableExists(ctx context.Context, config *config.ClickHouseConfig, tableName string) (bool, error) {
	if config.MySQL == nil {
		return false, fmt.Errorf("ClickHouse MySQL configuration is missing")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.MySQL.Username, config.MySQL.Password,
		config.MySQL.Host, config.MySQL.Port, t.config.Writer.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return false, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT count() FROM system.tables WHERE database = '%s' AND name = '%s'", t.config.Writer.Database, tableName)
	var count int
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	return count > 0, nil
}

// createClickHouseDatabase 创建ClickHouse数据库
func (t *SyncTask) createClickHouseDatabase(ctx context.Context, config *config.ClickHouseConfig) error {
	if config.MySQL == nil {
		return fmt.Errorf("ClickHouse MySQL configuration is missing")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		config.MySQL.Username, config.MySQL.Password,
		config.MySQL.Host, config.MySQL.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", t.config.Writer.Database)
	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create ClickHouse database: %w", err)
	}

	return nil
}

// getClickHouseTableSchema 获取ClickHouse表结构
func (t *SyncTask) getClickHouseTableSchema(ctx context.Context, config *config.ClickHouseConfig, tableName string) ([]ColumnInfo, error) {
	if config.MySQL == nil {
		return nil, fmt.Errorf("ClickHouse MySQL configuration is missing")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.MySQL.Username, config.MySQL.Password,
		config.MySQL.Host, config.MySQL.Port, t.config.Reader.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}
	defer db.Close()

	// 使用字符串拼接而不是预处理语句，避免ClickHouse的NOT_IMPLEMENTED错误
	query := fmt.Sprintf(`SELECT name, type, default_expression, comment, is_in_primary_key
			  FROM system.columns
			  WHERE database = '%s' AND table = '%s'
			  ORDER BY position`, t.config.Reader.Database, tableName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get ClickHouse table schema: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var defaultExpr sql.NullString
		var isPrimaryKey bool

		if err := rows.Scan(&col.Name, &col.DataType, &defaultExpr, &col.Comment, &isPrimaryKey); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		// ClickHouse默认值处理
		if defaultExpr.Valid && defaultExpr.String != "" {
			col.DefaultValue = &defaultExpr.String
		}

		// ClickHouse类型都可以为Nullable，这里简化处理
		col.IsNullable = strings.Contains(strings.ToUpper(col.DataType), "NULLABLE")

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading table schema: %w", err)
	}

	return columns, nil
}

// createClickHouseTable 创建ClickHouse表
func (t *SyncTask) createClickHouseTable(ctx context.Context, config *config.ClickHouseConfig, tableName string, schema []ColumnInfo) error {
	if config.MySQL == nil {
		return fmt.Errorf("ClickHouse MySQL configuration is missing")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.MySQL.Username, config.MySQL.Password,
		config.MySQL.Host, config.MySQL.Port, t.config.Writer.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}
	defer db.Close()

	// 构建CREATE TABLE语句
	var columns []string
	for _, col := range schema {
		colDef := fmt.Sprintf("`%s` %s", col.Name, t.convertToClickHouseType(col.DataType))
		if col.DefaultValue != nil && *col.DefaultValue != "" {
			colDef += fmt.Sprintf(" DEFAULT %s", *col.DefaultValue)
		}
		if col.Comment != "" {
			colDef += fmt.Sprintf(" COMMENT '%s'", strings.ReplaceAll(col.Comment, "'", "\\'"))
		}
		columns = append(columns, colDef)
	}

	// 假设第一列为排序键
	orderBy := schema[0].Name
	if len(schema) > 0 {
		orderBy = schema[0].Name
	}

	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		%s
	) ENGINE = MergeTree()
	ORDER BY %s`, tableName, strings.Join(columns, ",\n\t\t"), orderBy)

	t.logger.Debugf("Creating ClickHouse table with SQL: %s", createSQL)

	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("failed to create ClickHouse table: %w", err)
	}

	return nil
}

// convertToClickHouseType 转换数据类型到ClickHouse
func (t *SyncTask) convertToClickHouseType(sourceType string) string {
	sourceType = strings.ToUpper(sourceType)

	// StarRocks到ClickHouse类型映射
	switch {
	case strings.Contains(sourceType, "BIGINT"):
		return "Int64"
	case strings.Contains(sourceType, "INT"):
		return "Int32"
	case strings.Contains(sourceType, "SMALLINT"):
		return "Int16"
	case strings.Contains(sourceType, "TINYINT"):
		return "Int8"
	case strings.Contains(sourceType, "VARCHAR"):
		return "String"
	case strings.Contains(sourceType, "TEXT"):
		return "String"
	case strings.Contains(sourceType, "CHAR"):
		return "String"
	case strings.Contains(sourceType, "DECIMAL"):
		return sourceType // DECIMAL保持不变
	case strings.Contains(sourceType, "FLOAT"):
		return "Float32"
	case strings.Contains(sourceType, "DOUBLE"):
		return "Float64"
	case strings.Contains(sourceType, "BOOLEAN"):
		return "UInt8"
	case strings.Contains(sourceType, "DATE"):
		return "Date"
	case strings.Contains(sourceType, "DATETIME"):
		return "DateTime"
	case strings.Contains(sourceType, "TIMESTAMP"):
		return "DateTime64"
	case strings.Contains(sourceType, "JSON"):
		return "String"
	default:
		return "String" // 默认转换为String
	}
}

// StarRocks数据库和表操作方法

// checkStarRocksDatabaseExists 检查StarRocks数据库是否存在
func (t *SyncTask) checkStarRocksDatabaseExists(ctx context.Context, config *config.StarRocksConfig) (bool, error) {
	if config.MySQL == nil {
		return false, fmt.Errorf("StarRocks MySQL configuration is missing")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		config.MySQL.Username, config.MySQL.Password,
		config.MySQL.Host, config.MySQL.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return false, fmt.Errorf("failed to open StarRocks connection: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return false, fmt.Errorf("failed to ping StarRocks: %w", err)
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '%s'", t.config.Writer.Database)
	var count int
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to check database existence: %w", err)
	}

	return count > 0, nil
}

// checkStarRocksTableExists 检查StarRocks表是否存在
func (t *SyncTask) checkStarRocksTableExists(ctx context.Context, config *config.StarRocksConfig, tableName string) (bool, error) {
	if config.MySQL == nil {
		return false, fmt.Errorf("StarRocks MySQL configuration is missing")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.MySQL.Username, config.MySQL.Password,
		config.MySQL.Host, config.MySQL.Port, t.config.Writer.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return false, fmt.Errorf("failed to open StarRocks connection: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", t.config.Writer.Database, tableName)
	var count int
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return false, fmt.Errorf("failed to check table existence: %w", err)
	}

	return count > 0, nil
}

// createStarRocksDatabase 创建StarRocks数据库
func (t *SyncTask) createStarRocksDatabase(ctx context.Context, config *config.StarRocksConfig) error {
	if config.MySQL == nil {
		return fmt.Errorf("StarRocks MySQL configuration is missing")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		config.MySQL.Username, config.MySQL.Password,
		config.MySQL.Host, config.MySQL.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open StarRocks connection: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", t.config.Writer.Database)
	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create StarRocks database: %w", err)
	}

	return nil
}

// getStarRocksTableSchema 获取StarRocks表结构
func (t *SyncTask) getStarRocksTableSchema(ctx context.Context, config *config.StarRocksConfig, tableName string) ([]ColumnInfo, error) {
	if config.MySQL == nil {
		return nil, fmt.Errorf("StarRocks MySQL configuration is missing")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.MySQL.Username, config.MySQL.Password,
		config.MySQL.Host, config.MySQL.Port, t.config.Reader.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open StarRocks connection: %w", err)
	}
	defer db.Close()

	// 使用字符串拼接而不是预处理语句，避免StarRocks的预处理语句限制
	query := fmt.Sprintf(`SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_COMMENT,
					 CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
			  FROM information_schema.COLUMNS
			  WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'
			  ORDER BY ORDINAL_POSITION`, t.config.Reader.Database, tableName)

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get StarRocks table schema: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var isNullable string
		var defaultValue, charLength, numPrecision, numScale sql.NullString

		if err := rows.Scan(&col.Name, &col.DataType, &isNullable, &defaultValue, &col.Comment,
			&charLength, &numPrecision, &numScale); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		col.IsNullable = strings.ToUpper(isNullable) == "YES"

		if defaultValue.Valid {
			col.DefaultValue = &defaultValue.String
		}

		if charLength.Valid {
			if length, err := strconv.ParseInt(charLength.String, 10, 64); err == nil {
				col.CharLength = &length
			}
		}

		if numPrecision.Valid {
			if precision, err := strconv.ParseInt(numPrecision.String, 10, 64); err == nil {
				col.NumPrecision = &precision
			}
		}

		if numScale.Valid {
			if scale, err := strconv.ParseInt(numScale.String, 10, 64); err == nil {
				col.NumScale = &scale
			}
		}

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading table schema: %w", err)
	}

	return columns, nil
}

// createStarRocksTable 创建StarRocks表
func (t *SyncTask) createStarRocksTable(ctx context.Context, config *config.StarRocksConfig, tableName string, schema []ColumnInfo) error {
	if config.MySQL == nil {
		return fmt.Errorf("StarRocks MySQL configuration is missing")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.MySQL.Username, config.MySQL.Password,
		config.MySQL.Host, config.MySQL.Port, t.config.Writer.Database)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open StarRocks connection: %w", err)
	}
	defer db.Close()

	// 构建CREATE TABLE语句
	var columns []string
	var keyColumns []string

	t.logger.Debugf("Processing %d columns for table schema conversion", len(schema))

	for i, col := range schema {
		t.logger.Debugf("Converting column %d: name=%s, dataType=%s", i+1, col.Name, col.DataType)

		colType := t.convertToStarRocksType(col)
		t.logger.Debugf("Column %s converted from %s to %s", col.Name, col.DataType, colType)

		colDef := fmt.Sprintf("`%s` %s", col.Name, colType)

		if !col.IsNullable {
			colDef += " NOT NULL"
		}

		if col.DefaultValue != nil && *col.DefaultValue != "" && *col.DefaultValue != "NULL" {
			// 确保默认值格式正确
			defaultVal := *col.DefaultValue
			// 如果默认值包含特殊字符，需要适当处理
			if strings.Contains(defaultVal, "[") || strings.Contains(defaultVal, "]") {
				t.logger.Warnf("Default value contains brackets, skipping: %s", defaultVal)
			} else {
				colDef += fmt.Sprintf(" DEFAULT %s", defaultVal)
			}
		}

		if col.Comment != "" {
			// 清理注释中的特殊字符
			cleanComment := strings.ReplaceAll(col.Comment, "'", "\\'")
			cleanComment = strings.ReplaceAll(cleanComment, "[", "\\[")
			cleanComment = strings.ReplaceAll(cleanComment, "]", "\\]")
			colDef += fmt.Sprintf(" COMMENT '%s'", cleanComment)
		}

		columns = append(columns, colDef)

		// 前几列作为DUPLICATE KEY
		if i < 3 {
			keyColumns = append(keyColumns, col.Name)
		}
	}

	// 如果没有key列，使用第一列
	if len(keyColumns) == 0 && len(schema) > 0 {
		keyColumns = append(keyColumns, schema[0].Name)
	}

	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		%s
	) ENGINE=OLAP
	DUPLICATE KEY(%s)
	DISTRIBUTED BY HASH(%s) BUCKETS 3
	PROPERTIES (
		"replication_num" = "1"
	)`, tableName, strings.Join(columns, ",\n\t\t"),
		strings.Join(keyColumns, ", "), keyColumns[0])

	t.logger.Debugf("Generated StarRocks CREATE TABLE SQL:")
	t.logger.Debugf("%s", createSQL)

	if _, err := db.ExecContext(ctx, createSQL); err != nil {
		t.logger.Errorf("Failed to execute CREATE TABLE SQL: %v", err)
		t.logger.Errorf("Problematic SQL: %s", createSQL)
		return fmt.Errorf("failed to create StarRocks table: %w", err)
	}

	return nil
}

// convertToStarRocksType 转换数据类型到StarRocks
func (t *SyncTask) convertToStarRocksType(col ColumnInfo) string {
	dataType := strings.ToUpper(col.DataType)

	// 处理数组类型 - ClickHouse: Array(String) -> StarRocks: ARRAY<VARCHAR(65533)>
	if strings.Contains(dataType, "ARRAY(") || strings.Contains(dataType, "ARRAY[") {
		// 提取数组元素类型
		elementType := ""
		if strings.Contains(dataType, "ARRAY(") {
			start := strings.Index(dataType, "ARRAY(") + 6
			end := strings.LastIndex(dataType, ")")
			if end > start {
				elementType = strings.TrimSpace(dataType[start:end])
			}
		} else if strings.Contains(dataType, "ARRAY[") {
			start := strings.Index(dataType, "ARRAY[") + 6
			end := strings.LastIndex(dataType, "]")
			if end > start {
				elementType = strings.TrimSpace(dataType[start:end])
			}
		}

		// 递归转换元素类型
		if elementType != "" {
			elementCol := ColumnInfo{DataType: elementType, CharLength: col.CharLength}
			starRocksElementType := t.convertToStarRocksType(elementCol)
			return fmt.Sprintf("ARRAY<%s>", starRocksElementType)
		}
		// 默认为字符串数组
		return "ARRAY<VARCHAR(65533)>"
	}

	// 处理嵌套类型 - 如 Tuple, Nested 等
	if strings.Contains(dataType, "TUPLE(") || strings.Contains(dataType, "NESTED(") {
		// StarRocks 不直接支持复杂嵌套类型，转换为JSON字符串
		return "JSON"
	}

	// 处理Map类型
	if strings.Contains(dataType, "MAP(") {
		// StarRocks支持MAP类型
		start := strings.Index(dataType, "MAP(") + 4
		end := strings.LastIndex(dataType, ")")
		if end > start {
			mapContent := strings.TrimSpace(dataType[start:end])
			// 简单处理，假设是MAP(String, String)
			parts := strings.Split(mapContent, ",")
			if len(parts) >= 2 {
				keyType := strings.TrimSpace(parts[0])
				valueType := strings.TrimSpace(strings.Join(parts[1:], ","))

				keyCol := ColumnInfo{DataType: keyType}
				valueCol := ColumnInfo{DataType: valueType}

				starRocksKeyType := t.convertToStarRocksType(keyCol)
				starRocksValueType := t.convertToStarRocksType(valueCol)

				return fmt.Sprintf("MAP<%s,%s>", starRocksKeyType, starRocksValueType)
			}
		}
		return "MAP<VARCHAR(65533),VARCHAR(65533)>"
	}

	// 处理Nullable类型 - ClickHouse: Nullable(String) -> StarRocks: VARCHAR(65533)
	if strings.Contains(dataType, "NULLABLE(") {
		start := strings.Index(dataType, "NULLABLE(") + 9
		end := strings.LastIndex(dataType, ")")
		if end > start {
			innerType := strings.TrimSpace(dataType[start:end])
			innerCol := ColumnInfo{
				DataType:     innerType,
				CharLength:   col.CharLength,
				NumPrecision: col.NumPrecision,
				NumScale:     col.NumScale,
			}
			return t.convertToStarRocksType(innerCol)
		}
	}

	// 处理LowCardinality类型 - ClickHouse: LowCardinality(String) -> StarRocks: VARCHAR
	if strings.Contains(dataType, "LOWCARDINALITY(") {
		start := strings.Index(dataType, "LOWCARDINALITY(") + 15
		end := strings.LastIndex(dataType, ")")
		if end > start {
			innerType := strings.TrimSpace(dataType[start:end])
			innerCol := ColumnInfo{
				DataType:     innerType,
				CharLength:   col.CharLength,
				NumPrecision: col.NumPrecision,
				NumScale:     col.NumScale,
			}
			return t.convertToStarRocksType(innerCol)
		}
	}

	// 基础类型转换 - 注意：更具体的类型(UINT)要在通用类型(INT)之前匹配
	switch {
	case strings.Contains(dataType, "INT64"):
		return "BIGINT"
	case strings.Contains(dataType, "UINT64"):
		// UINT64最大值超出BIGINT范围，使用DECIMAL避免溢出
		return "DECIMAL(20,0)"
	case strings.Contains(dataType, "INT32"):
		return "INT"
	case strings.Contains(dataType, "UINT32"):
		// UINT32最大值4,294,967,295超出INT范围，使用BIGINT避免溢出
		return "BIGINT"
	case strings.Contains(dataType, "UINT16"):
		// UINT16最大值65,535超出SMALLINT范围，使用INT避免溢出
		return "INT"
	case strings.Contains(dataType, "INT16"):
		return "SMALLINT"
	case strings.Contains(dataType, "UINT8"):
		// UINT8最大值255超出TINYINT范围(-128到127)，使用SMALLINT避免溢出
		return "SMALLINT"
	case strings.Contains(dataType, "INT8"):
		return "TINYINT"
	case strings.Contains(dataType, "STRING") || strings.Contains(dataType, "FIXEDSTRING"):
		if col.CharLength != nil && *col.CharLength > 0 {
			return fmt.Sprintf("VARCHAR(%d)", *col.CharLength)
		}
		return "VARCHAR(65533)"
	case strings.Contains(dataType, "DECIMAL"):
		precision := int64(10)
		scale := int64(2)
		if col.NumPrecision != nil {
			precision = *col.NumPrecision
		}
		if col.NumScale != nil {
			scale = *col.NumScale
		}
		return fmt.Sprintf("DECIMAL(%d, %d)", precision, scale)
	case strings.Contains(dataType, "FLOAT32"):
		return "FLOAT"
	case strings.Contains(dataType, "FLOAT64"):
		return "DOUBLE"
	case strings.Contains(dataType, "DATE"):
		return "DATE"
	case strings.Contains(dataType, "DATETIME") || strings.Contains(dataType, "DATETIME64"):
		return "DATETIME"
	case strings.Contains(dataType, "TIMESTAMP"):
		return "DATETIME"
	case strings.Contains(dataType, "BOOLEAN"):
		return "BOOLEAN"
	case strings.Contains(dataType, "UUID"):
		return "VARCHAR(36)"
	case strings.Contains(dataType, "IPV4"):
		return "VARCHAR(15)"
	case strings.Contains(dataType, "IPV6"):
		return "VARCHAR(39)"
	case strings.Contains(dataType, "JSON"):
		return "JSON"
	default:
		// 对于任何包含方括号的未知类型，记录日志并转换为字符串
		if strings.Contains(dataType, "[") || strings.Contains(dataType, "]") {
			t.logger.Warnf("Unknown complex type with brackets: %s, converting to VARCHAR", dataType)
			return "VARCHAR(65533)"
		}
		// 默认使用VARCHAR
		t.logger.Warnf("Unknown data type: %s, converting to VARCHAR", dataType)
		return "VARCHAR(65533)"
	}
}
