package pipeline

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// SimplePipeline 简单数据管道实现
type SimplePipeline struct {
	processors []Processor
	logger     *logrus.Logger
	metrics    MetricsCollector
	mutex      sync.RWMutex
}

// NewSimplePipeline 创建简单数据管道
func NewSimplePipeline(logger *logrus.Logger) *SimplePipeline {
	if logger == nil {
		logger = logrus.New()
	}

	return &SimplePipeline{
		processors: make([]Processor, 0),
		logger:     logger,
		metrics:    NewSimpleMetricsCollector(),
	}
}

// AddProcessor 添加处理器
func (p *SimplePipeline) AddProcessor(processor Processor) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	// 检查处理器名称是否重复
	for _, existing := range p.processors {
		if existing.GetName() == processor.GetName() {
			return fmt.Errorf("processor with name '%s' already exists", processor.GetName())
		}
	}

	p.processors = append(p.processors, processor)
	p.logger.Infof("Added processor: %s (type: %s)", processor.GetName(), processor.GetType())
	return nil
}

// RemoveProcessor 移除处理器
func (p *SimplePipeline) RemoveProcessor(name string) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	for i, processor := range p.processors {
		if processor.GetName() == name {
			// 关闭处理器
			if err := processor.Close(); err != nil {
				p.logger.Warnf("Error closing processor %s: %v", name, err)
			}

			// 从切片中移除
			p.processors = append(p.processors[:i], p.processors[i+1:]...)
			p.logger.Infof("Removed processor: %s", name)
			return nil
		}
	}

	return fmt.Errorf("processor not found: %s", name)
}

// GetProcessors 获取所有处理器
func (p *SimplePipeline) GetProcessors() []Processor {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	result := make([]Processor, len(p.processors))
	copy(result, p.processors)
	return result
}

// Process 处理数据批次
func (p *SimplePipeline) Process(ctx context.Context, batch DataBatch) (DataBatch, error) {
	if batch.IsEmpty() {
		return batch, nil
	}

	startTime := time.Now()
	currentBatch := batch
	originalSize := batch.Size()
	originalBytes := batch.Bytes()

	p.mutex.RLock()
	processors := make([]Processor, len(p.processors))
	copy(processors, p.processors)
	p.mutex.RUnlock()

	// 依次通过每个处理器
	for _, processor := range processors {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		p.logger.Debugf("Processing batch through %s (%s)", processor.GetName(), processor.GetType())

		processedBatch, err := processor.Process(ctx, currentBatch)
		if err != nil {
			p.logger.Errorf("Processor %s failed: %v", processor.GetName(), err)
			return nil, fmt.Errorf("processor %s failed: %w", processor.GetName(), err)
		}

		currentBatch = processedBatch

		// 如果批次被过滤为空，记录过滤的行数
		if currentBatch.IsEmpty() && !batch.IsEmpty() {
			p.metrics.RecordFiltered(int64(originalSize))
			break
		}
	}

	// 记录处理指标
	processingTime := time.Since(startTime).Milliseconds()
	p.metrics.RecordProcessingTime(processingTime)

	if !currentBatch.IsEmpty() {
		p.metrics.RecordProcessed(int64(currentBatch.Size()), currentBatch.Bytes())
	} else {
		p.metrics.RecordFiltered(int64(originalSize))
	}

	p.logger.Debugf("Pipeline processed %d rows (%d bytes) in %dms, result: %d rows",
		originalSize, originalBytes, processingTime, currentBatch.Size())

	return currentBatch, nil
}

// ProcessStream 流式处理数据
func (p *SimplePipeline) ProcessStream(ctx context.Context, input <-chan DataBatch, output chan<- DataBatch) error {
	defer close(output)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-input:
			if !ok {
				return nil // 输入通道已关闭
			}

			processedBatch, err := p.Process(ctx, batch)
			if err != nil {
				p.logger.Errorf("Stream processing failed: %v", err)
				return err
			}

			// 只发送非空批次
			if !processedBatch.IsEmpty() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case output <- processedBatch:
				}
			}
		}
	}
}

// Configure 配置管道
func (p *SimplePipeline) Configure(configs []*ProcessorConfig) error {
	// 按 Order 字段排序配置
	sort.Slice(configs, func(i, j int) bool {
		return configs[i].Order < configs[j].Order
	})

	// 清理现有处理器
	p.mutex.Lock()
	for _, processor := range p.processors {
		processor.Close()
	}
	p.processors = p.processors[:0]
	p.mutex.Unlock()

	// 创建新的处理器
	for _, config := range configs {
		if !config.Enabled {
			continue
		}

		processor, err := p.createProcessor(config)
		if err != nil {
			return fmt.Errorf("failed to create processor %s: %w", config.Name, err)
		}

		if err := processor.Configure(config); err != nil {
			processor.Close()
			return fmt.Errorf("failed to configure processor %s: %w", config.Name, err)
		}

		if err := processor.Validate(); err != nil {
			processor.Close()
			return fmt.Errorf("processor %s validation failed: %w", config.Name, err)
		}

		if err := p.AddProcessor(processor); err != nil {
			processor.Close()
			return fmt.Errorf("failed to add processor %s: %w", config.Name, err)
		}
	}

	return nil
}

// createProcessor 根据配置创建处理器
func (p *SimplePipeline) createProcessor(config *ProcessorConfig) (Processor, error) {
	switch config.Type {
	case "column_mapping":
		return NewColumnMappingProcessor(p.logger), nil
	case "row_filter":
		return NewRowFilterProcessor(p.logger), nil
	case "data_validator":
		return NewDataValidatorProcessor(p.logger), nil
	default:
		return nil, fmt.Errorf("unsupported processor type: %s", config.Type)
	}
}

// Validate 验证管道配置
func (p *SimplePipeline) Validate() error {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	for _, processor := range p.processors {
		if err := processor.Validate(); err != nil {
			return fmt.Errorf("processor %s validation failed: %w", processor.GetName(), err)
		}
	}

	return nil
}

// Close 关闭管道
func (p *SimplePipeline) Close() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	var lastErr error
	for _, processor := range p.processors {
		if err := processor.Close(); err != nil {
			p.logger.Warnf("Error closing processor %s: %v", processor.GetName(), err)
			lastErr = err
		}
	}

	p.processors = p.processors[:0]
	p.logger.Info("Pipeline closed")
	return lastErr
}

// GetMetrics 获取管道指标
func (p *SimplePipeline) GetMetrics() *Metrics {
	return p.metrics.GetMetrics()
}

// ResetMetrics 重置管道指标
func (p *SimplePipeline) ResetMetrics() {
	p.metrics.Reset()
}

// ProcessorFactory 处理器工厂
type ProcessorFactory struct {
	logger *logrus.Logger
}

// NewProcessorFactory 创建处理器工厂
func NewProcessorFactory(logger *logrus.Logger) *ProcessorFactory {
	if logger == nil {
		logger = logrus.New()
	}

	return &ProcessorFactory{
		logger: logger,
	}
}

// CreateProcessor 创建处理器
func (f *ProcessorFactory) CreateProcessor(processorType string) (Processor, error) {
	switch processorType {
	case "column_mapping":
		return NewColumnMappingProcessor(f.logger), nil
	case "row_filter":
		return NewRowFilterProcessor(f.logger), nil
	case "data_validator":
		return NewDataValidatorProcessor(f.logger), nil
	default:
		return nil, fmt.Errorf("unsupported processor type: %s", processorType)
	}
}

// GetSupportedTypes 获取支持的处理器类型
func (f *ProcessorFactory) GetSupportedTypes() []string {
	return []string{
		"column_mapping",
		"row_filter",
		"data_validator",
	}
}

// ValidateConfig 验证处理器配置
func (f *ProcessorFactory) ValidateConfig(config *ProcessorConfig) error {
	if config.Name == "" {
		return fmt.Errorf("processor name is required")
	}

	if config.Type == "" {
		return fmt.Errorf("processor type is required")
	}

	// 检查类型是否支持
	supportedTypes := f.GetSupportedTypes()
	typeSupported := false
	for _, supportedType := range supportedTypes {
		if config.Type == supportedType {
			typeSupported = true
			break
		}
	}

	if !typeSupported {
		return fmt.Errorf("unsupported processor type: %s", config.Type)
	}

	return nil
}

// PipelineBuilder 管道构建器
type PipelineBuilder struct {
	pipeline *SimplePipeline
	factory  *ProcessorFactory
}

// NewPipelineBuilder 创建管道构建器
func NewPipelineBuilder(logger *logrus.Logger) *PipelineBuilder {
	return &PipelineBuilder{
		pipeline: NewSimplePipeline(logger),
		factory:  NewProcessorFactory(logger),
	}
}

// AddColumnMapping 添加列映射处理器
func (b *PipelineBuilder) AddColumnMapping(name string, mapping map[string]string) *PipelineBuilder {
	config := &ProcessorConfig{
		Name:    name,
		Type:    "column_mapping",
		Enabled: true,
		Parameters: map[string]interface{}{
			"mapping": mapping,
		},
		Order: len(b.pipeline.processors),
	}

	processor := NewColumnMappingProcessor(b.pipeline.logger)
	processor.Configure(config)
	b.pipeline.AddProcessor(processor)

	return b
}

// AddRowFilter 添加行过滤处理器
func (b *PipelineBuilder) AddRowFilter(name string, conditions []FilterCondition) *PipelineBuilder {
	conditionsConfig := make([]interface{}, len(conditions))
	for i, cond := range conditions {
		conditionsConfig[i] = map[string]interface{}{
			"column":   cond.Column,
			"operator": cond.Operator,
			"value":    cond.Value,
		}
	}

	config := &ProcessorConfig{
		Name:    name,
		Type:    "row_filter",
		Enabled: true,
		Parameters: map[string]interface{}{
			"conditions": conditionsConfig,
		},
		Order: len(b.pipeline.processors),
	}

	processor := NewRowFilterProcessor(b.pipeline.logger)
	processor.Configure(config)
	b.pipeline.AddProcessor(processor)

	return b
}

// AddDataValidator 添加数据验证处理器
func (b *PipelineBuilder) AddDataValidator(name string, rules []ValidationRule) *PipelineBuilder {
	rulesConfig := make([]interface{}, len(rules))
	for i, rule := range rules {
		rulesConfig[i] = map[string]interface{}{
			"column":     rule.Column,
			"required":   rule.Required,
			"type":       rule.Type,
			"min_length": rule.MinLength,
			"max_length": rule.MaxLength,
			"min_value":  rule.MinValue,
			"max_value":  rule.MaxValue,
			"pattern":    rule.Pattern,
		}
	}

	config := &ProcessorConfig{
		Name:    name,
		Type:    "data_validator",
		Enabled: true,
		Parameters: map[string]interface{}{
			"rules": rulesConfig,
		},
		Order: len(b.pipeline.processors),
	}

	processor := NewDataValidatorProcessor(b.pipeline.logger)
	processor.Configure(config)
	b.pipeline.AddProcessor(processor)

	return b
}

// Build 构建管道
func (b *PipelineBuilder) Build() Pipeline {
	return b.pipeline
}