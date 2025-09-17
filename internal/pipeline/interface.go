package pipeline

import (
	"context"
	"io"
)

// DataRow 数据行接口
type DataRow interface {
	// GetColumns 获取列名列表
	GetColumns() []string

	// GetValue 根据列名获取值
	GetValue(column string) interface{}

	// GetValues 获取所有值
	GetValues() []interface{}

	// SetValue 设置列值
	SetValue(column string, value interface{})

	// ToMap 转换为 map
	ToMap() map[string]interface{}

	// ToCSV 转换为 CSV 行
	ToCSV() []string

	// Clone 克隆数据行
	Clone() DataRow

	// Size 估算数据大小（字节）
	Size() int64
}

// DataBatch 数据批次接口
type DataBatch interface {
	// GetRows 获取所有数据行
	GetRows() []DataRow

	// AddRow 添加数据行
	AddRow(row DataRow)

	// Size 获取批次大小（行数）
	Size() int

	// Bytes 估算批次大小（字节）
	Bytes() int64

	// Clear 清空批次
	Clear()

	// Clone 克隆批次
	Clone() DataBatch

	// IsEmpty 检查是否为空
	IsEmpty() bool
}

// ProcessorConfig 处理器配置
type ProcessorConfig struct {
	Name       string                 `yaml:"name" json:"name"`
	Type       string                 `yaml:"type" json:"type"`
	Enabled    bool                   `yaml:"enabled" json:"enabled"`
	Parameters map[string]interface{} `yaml:"parameters" json:"parameters"`
	Order      int                    `yaml:"order" json:"order"`
}

// Processor 数据处理器接口
type Processor interface {
	// GetName 获取处理器名称
	GetName() string

	// GetType 获取处理器类型
	GetType() string

	// Process 处理数据批次
	Process(ctx context.Context, batch DataBatch) (DataBatch, error)

	// Configure 配置处理器
	Configure(config *ProcessorConfig) error

	// Validate 验证配置
	Validate() error

	// Close 关闭处理器
	Close() error
}

// Filter 数据过滤器接口
type Filter interface {
	Processor

	// ShouldInclude 判断是否应该包含此行
	ShouldInclude(ctx context.Context, row DataRow) (bool, error)
}

// Transformer 数据转换器接口
type Transformer interface {
	Processor

	// Transform 转换数据行
	Transform(ctx context.Context, row DataRow) (DataRow, error)
}

// Validator 数据验证器接口
type Validator interface {
	Processor

	// Validate 验证数据行
	Validate(ctx context.Context, row DataRow) error
}

// Pipeline 数据处理管道接口
type Pipeline interface {
	// AddProcessor 添加处理器
	AddProcessor(processor Processor) error

	// RemoveProcessor 移除处理器
	RemoveProcessor(name string) error

	// GetProcessors 获取所有处理器
	GetProcessors() []Processor

	// Process 处理数据批次
	Process(ctx context.Context, batch DataBatch) (DataBatch, error)

	// ProcessStream 流式处理数据
	ProcessStream(ctx context.Context, input <-chan DataBatch, output chan<- DataBatch) error

	// Configure 配置管道
	Configure(configs []*ProcessorConfig) error

	// Validate 验证管道配置
	Validate() error

	// Close 关闭管道
	Close() error
}

// Reader 数据读取器接口
type Reader interface {
	// Read 读取数据批次
	Read(ctx context.Context, batchSize int) (DataBatch, error)

	// ReadAll 读取所有数据
	ReadAll(ctx context.Context, batchSize int) (<-chan DataBatch, <-chan error)

	// Seek 定位到指定位置
	Seek(offset int64) error

	// Close 关闭读取器
	Close() error
}

// Writer 数据写入器接口
type Writer interface {
	// Write 写入数据批次
	Write(ctx context.Context, batch DataBatch) error

	// WriteStream 流式写入数据
	WriteStream(ctx context.Context, input <-chan DataBatch) error

	// Flush 刷新缓冲区
	Flush(ctx context.Context) error

	// Close 关闭写入器
	Close() error
}

// Metrics 处理指标
type Metrics struct {
	ProcessedRows   int64   `json:"processed_rows"`
	ProcessedBytes  int64   `json:"processed_bytes"`
	FilteredRows    int64   `json:"filtered_rows"`
	ErrorRows       int64   `json:"error_rows"`
	ProcessingTime  int64   `json:"processing_time_ms"`
	ThroughputMBPS  float64 `json:"throughput_mbps"`
	ThroughputRPS   float64 `json:"throughput_rps"`
}

// MetricsCollector 指标收集器接口
type MetricsCollector interface {
	// RecordProcessed 记录处理的数据
	RecordProcessed(rows int64, bytes int64)

	// RecordFiltered 记录过滤的行数
	RecordFiltered(rows int64)

	// RecordError 记录错误行数
	RecordError(rows int64)

	// RecordProcessingTime 记录处理时间
	RecordProcessingTime(duration int64)

	// GetMetrics 获取指标
	GetMetrics() *Metrics

	// Reset 重置指标
	Reset()
}

// Context 处理上下文
type Context interface {
	// GetConfig 获取配置
	GetConfig(key string) interface{}

	// SetConfig 设置配置
	SetConfig(key string, value interface{})

	// GetMetrics 获取指标收集器
	GetMetrics() MetricsCollector

	// GetLogger 获取日志记录器
	GetLogger() io.Writer

	// GetUserData 获取用户数据
	GetUserData(key string) interface{}

	// SetUserData 设置用户数据
	SetUserData(key string, value interface{})
}