package pipeline

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SimpleDataRow 简单数据行实现
type SimpleDataRow struct {
	columns []string
	values  map[string]interface{}
	mutex   sync.RWMutex
}

// NewSimpleDataRow 创建简单数据行
func NewSimpleDataRow(columns []string, values []interface{}) *SimpleDataRow {
	valueMap := make(map[string]interface{})
	for i, col := range columns {
		if i < len(values) {
			valueMap[col] = values[i]
		}
	}

	return &SimpleDataRow{
		columns: columns,
		values:  valueMap,
	}
}

// NewSimpleDataRowFromMap 从 map 创建数据行
func NewSimpleDataRowFromMap(data map[string]interface{}) *SimpleDataRow {
	columns := make([]string, 0, len(data))
	for col := range data {
		columns = append(columns, col)
	}

	return &SimpleDataRow{
		columns: columns,
		values:  data,
	}
}

// GetColumns 获取列名列表
func (r *SimpleDataRow) GetColumns() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	result := make([]string, len(r.columns))
	copy(result, r.columns)
	return result
}

// GetValue 根据列名获取值
func (r *SimpleDataRow) GetValue(column string) interface{} {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return r.values[column]
}

// GetValues 获取所有值
func (r *SimpleDataRow) GetValues() []interface{} {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	result := make([]interface{}, len(r.columns))
	for i, col := range r.columns {
		result[i] = r.values[col]
	}
	return result
}

// SetValue 设置列值
func (r *SimpleDataRow) SetValue(column string, value interface{}) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.values[column] = value

	// 如果是新列，添加到列列表
	for _, col := range r.columns {
		if col == column {
			return
		}
	}
	r.columns = append(r.columns, column)
}

// ToMap 转换为 map
func (r *SimpleDataRow) ToMap() map[string]interface{} {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	result := make(map[string]interface{})
	for k, v := range r.values {
		result[k] = v
	}
	return result
}

// ToCSV 转换为 CSV 行
func (r *SimpleDataRow) ToCSV() []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	result := make([]string, len(r.columns))
	for i, col := range r.columns {
		value := r.values[col]
		result[i] = r.valueToString(value)
	}
	return result
}

// Clone 克隆数据行
func (r *SimpleDataRow) Clone() DataRow {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	newValues := make(map[string]interface{})
	for k, v := range r.values {
		newValues[k] = v
	}

	newColumns := make([]string, len(r.columns))
	copy(newColumns, r.columns)

	return &SimpleDataRow{
		columns: newColumns,
		values:  newValues,
	}
}

// Size 估算数据大小（字节）
func (r *SimpleDataRow) Size() int64 {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	var size int64
	for _, col := range r.columns {
		size += int64(len(col)) // 列名长度
		value := r.values[col]
		size += r.estimateValueSize(value)
	}
	return size
}

// valueToString 将值转换为字符串
func (r *SimpleDataRow) valueToString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case bool:
		return strconv.FormatBool(v)
	case time.Time:
		return v.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", v)
	}
}

// estimateValueSize 估算值的大小
func (r *SimpleDataRow) estimateValueSize(value interface{}) int64 {
	if value == nil {
		return 0
	}

	switch v := value.(type) {
	case string:
		return int64(len(v))
	case []byte:
		return int64(len(v))
	case int, int32, uint, uint32, float32:
		return 4
	case int64, uint64, float64:
		return 8
	case bool:
		return 1
	case time.Time:
		return 8
	default:
		return int64(len(fmt.Sprintf("%v", v)))
	}
}

// SimpleDataBatch 简单数据批次实现
type SimpleDataBatch struct {
	rows  []DataRow
	mutex sync.RWMutex
}

// NewSimpleDataBatch 创建简单数据批次
func NewSimpleDataBatch() *SimpleDataBatch {
	return &SimpleDataBatch{
		rows: make([]DataRow, 0),
	}
}

// GetRows 获取所有数据行
func (b *SimpleDataBatch) GetRows() []DataRow {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	result := make([]DataRow, len(b.rows))
	copy(result, b.rows)
	return result
}

// AddRow 添加数据行
func (b *SimpleDataBatch) AddRow(row DataRow) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.rows = append(b.rows, row)
}

// Size 获取批次大小（行数）
func (b *SimpleDataBatch) Size() int {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return len(b.rows)
}

// Bytes 估算批次大小（字节）
func (b *SimpleDataBatch) Bytes() int64 {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	var total int64
	for _, row := range b.rows {
		total += row.Size()
	}
	return total
}

// Clear 清空批次
func (b *SimpleDataBatch) Clear() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.rows = b.rows[:0]
}

// Clone 克隆批次
func (b *SimpleDataBatch) Clone() DataBatch {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	newBatch := NewSimpleDataBatch()
	for _, row := range b.rows {
		newBatch.AddRow(row.Clone())
	}
	return newBatch
}

// IsEmpty 检查是否为空
func (b *SimpleDataBatch) IsEmpty() bool {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	return len(b.rows) == 0
}

// SimpleMetricsCollector 简单指标收集器实现
type SimpleMetricsCollector struct {
	processedRows   int64
	processedBytes  int64
	filteredRows    int64
	errorRows       int64
	processingTime  int64
	startTime       time.Time
	mutex           sync.RWMutex
}

// NewSimpleMetricsCollector 创建简单指标收集器
func NewSimpleMetricsCollector() *SimpleMetricsCollector {
	return &SimpleMetricsCollector{
		startTime: time.Now(),
	}
}

// RecordProcessed 记录处理的数据
func (c *SimpleMetricsCollector) RecordProcessed(rows int64, bytes int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.processedRows += rows
	c.processedBytes += bytes
}

// RecordFiltered 记录过滤的行数
func (c *SimpleMetricsCollector) RecordFiltered(rows int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.filteredRows += rows
}

// RecordError 记录错误行数
func (c *SimpleMetricsCollector) RecordError(rows int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.errorRows += rows
}

// RecordProcessingTime 记录处理时间
func (c *SimpleMetricsCollector) RecordProcessingTime(duration int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.processingTime += duration
}

// GetMetrics 获取指标
func (c *SimpleMetricsCollector) GetMetrics() *Metrics {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	elapsed := time.Since(c.startTime).Seconds()

	var throughputMBPS, throughputRPS float64
	if elapsed > 0 {
		throughputMBPS = float64(c.processedBytes) / elapsed / 1024 / 1024
		throughputRPS = float64(c.processedRows) / elapsed
	}

	return &Metrics{
		ProcessedRows:  c.processedRows,
		ProcessedBytes: c.processedBytes,
		FilteredRows:   c.filteredRows,
		ErrorRows:      c.errorRows,
		ProcessingTime: c.processingTime,
		ThroughputMBPS: throughputMBPS,
		ThroughputRPS:  throughputRPS,
	}
}

// Reset 重置指标
func (c *SimpleMetricsCollector) Reset() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.processedRows = 0
	c.processedBytes = 0
	c.filteredRows = 0
	c.errorRows = 0
	c.processingTime = 0
	c.startTime = time.Now()
}

// SimpleContext 简单处理上下文实现
type SimpleContext struct {
	config    map[string]interface{}
	userData  map[string]interface{}
	metrics   MetricsCollector
	logger    strings.Builder
	mutex     sync.RWMutex
}

// NewSimpleContext 创建简单处理上下文
func NewSimpleContext() *SimpleContext {
	return &SimpleContext{
		config:   make(map[string]interface{}),
		userData: make(map[string]interface{}),
		metrics:  NewSimpleMetricsCollector(),
	}
}

// GetConfig 获取配置
func (c *SimpleContext) GetConfig(key string) interface{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.config[key]
}

// SetConfig 设置配置
func (c *SimpleContext) SetConfig(key string, value interface{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.config[key] = value
}

// GetMetrics 获取指标收集器
func (c *SimpleContext) GetMetrics() MetricsCollector {
	return c.metrics
}

// GetLogger 获取日志记录器
func (c *SimpleContext) GetLogger() *strings.Builder {
	return &c.logger
}

// GetUserData 获取用户数据
func (c *SimpleContext) GetUserData(key string) interface{} {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.userData[key]
}

// SetUserData 设置用户数据
func (c *SimpleContext) SetUserData(key string, value interface{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.userData[key] = value
}