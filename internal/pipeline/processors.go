package pipeline

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// BaseProcessor 基础处理器
type BaseProcessor struct {
	name   string
	pType  string
	config *ProcessorConfig
	logger *logrus.Logger
}

// NewBaseProcessor 创建基础处理器
func NewBaseProcessor(name, pType string, logger *logrus.Logger) *BaseProcessor {
	if logger == nil {
		logger = logrus.New()
	}

	return &BaseProcessor{
		name:   name,
		pType:  pType,
		logger: logger,
	}
}

// GetName 获取处理器名称
func (p *BaseProcessor) GetName() string {
	return p.name
}

// GetType 获取处理器类型
func (p *BaseProcessor) GetType() string {
	return p.pType
}

// Configure 配置处理器
func (p *BaseProcessor) Configure(config *ProcessorConfig) error {
	p.config = config
	return nil
}

// Validate 验证配置
func (p *BaseProcessor) Validate() error {
	if p.config == nil {
		return fmt.Errorf("processor %s not configured", p.name)
	}
	return nil
}

// Close 关闭处理器
func (p *BaseProcessor) Close() error {
	return nil
}

// ColumnMappingProcessor 列映射处理器
type ColumnMappingProcessor struct {
	*BaseProcessor
	mapping map[string]string // 源列名 -> 目标列名
}

// NewColumnMappingProcessor 创建列映射处理器
func NewColumnMappingProcessor(logger *logrus.Logger) *ColumnMappingProcessor {
	return &ColumnMappingProcessor{
		BaseProcessor: NewBaseProcessor("column_mapping", "transformer", logger),
		mapping:       make(map[string]string),
	}
}

// Configure 配置处理器
func (p *ColumnMappingProcessor) Configure(config *ProcessorConfig) error {
	if err := p.BaseProcessor.Configure(config); err != nil {
		return err
	}

	// 解析列映射配置
	if mappingConfig, ok := config.Parameters["mapping"].(map[string]interface{}); ok {
		for src, dst := range mappingConfig {
			if dstStr, ok := dst.(string); ok {
				p.mapping[src] = dstStr
			}
		}
	}

	return nil
}

// Process 处理数据批次
func (p *ColumnMappingProcessor) Process(ctx context.Context, batch DataBatch) (DataBatch, error) {
	if len(p.mapping) == 0 {
		return batch, nil // 没有映射规则，直接返回
	}

	newBatch := NewSimpleDataBatch()
	for _, row := range batch.GetRows() {
		newRow := p.Transform(ctx, row)
		newBatch.AddRow(newRow)
	}

	return newBatch, nil
}

// Transform 转换数据行
func (p *ColumnMappingProcessor) Transform(ctx context.Context, row DataRow) DataRow {
	if len(p.mapping) == 0 {
		return row
	}

	newRow := NewSimpleDataRow([]string{}, []interface{}{})

	// 应用列映射
	for _, column := range row.GetColumns() {
		value := row.GetValue(column)

		// 检查是否有映射规则
		if newColumn, exists := p.mapping[column]; exists {
			newRow.SetValue(newColumn, value)
		} else {
			newRow.SetValue(column, value) // 保持原列名
		}
	}

	return newRow
}

// RowFilterProcessor 行过滤处理器
type RowFilterProcessor struct {
	*BaseProcessor
	conditions []FilterCondition
}

// FilterCondition 过滤条件
type FilterCondition struct {
	Column   string      `json:"column"`
	Operator string      `json:"operator"` // eq, ne, gt, lt, ge, le, in, not_in, like, regex
	Value    interface{} `json:"value"`
	Regex    *regexp.Regexp `json:"-"`
}

// NewRowFilterProcessor 创建行过滤处理器
func NewRowFilterProcessor(logger *logrus.Logger) *RowFilterProcessor {
	return &RowFilterProcessor{
		BaseProcessor: NewBaseProcessor("row_filter", "filter", logger),
		conditions:    make([]FilterCondition, 0),
	}
}

// Configure 配置处理器
func (p *RowFilterProcessor) Configure(config *ProcessorConfig) error {
	if err := p.BaseProcessor.Configure(config); err != nil {
		return err
	}

	// 解析过滤条件
	if conditionsConfig, ok := config.Parameters["conditions"].([]interface{}); ok {
		for _, condConfig := range conditionsConfig {
			if condMap, ok := condConfig.(map[string]interface{}); ok {
				condition := FilterCondition{
					Column:   getString(condMap, "column"),
					Operator: getString(condMap, "operator"),
					Value:    condMap["value"],
				}

				// 如果是正则表达式，预编译
				if condition.Operator == "regex" {
					if pattern, ok := condition.Value.(string); ok {
						if regex, err := regexp.Compile(pattern); err == nil {
							condition.Regex = regex
						} else {
							return fmt.Errorf("invalid regex pattern: %s", pattern)
						}
					}
				}

				p.conditions = append(p.conditions, condition)
			}
		}
	}

	return nil
}

// Process 处理数据批次
func (p *RowFilterProcessor) Process(ctx context.Context, batch DataBatch) (DataBatch, error) {
	if len(p.conditions) == 0 {
		return batch, nil // 没有过滤条件，直接返回
	}

	newBatch := NewSimpleDataBatch()
	for _, row := range batch.GetRows() {
		if shouldInclude, _ := p.ShouldInclude(ctx, row); shouldInclude {
			newBatch.AddRow(row)
		}
	}

	return newBatch, nil
}

// ShouldInclude 判断是否应该包含此行
func (p *RowFilterProcessor) ShouldInclude(ctx context.Context, row DataRow) (bool, error) {
	for _, condition := range p.conditions {
		if !p.evaluateCondition(row, condition) {
			return false, nil
		}
	}
	return true, nil
}

// evaluateCondition 评估过滤条件
func (p *RowFilterProcessor) evaluateCondition(row DataRow, condition FilterCondition) bool {
	value := row.GetValue(condition.Column)

	switch condition.Operator {
	case "eq":
		return p.compareValues(value, condition.Value) == 0
	case "ne":
		return p.compareValues(value, condition.Value) != 0
	case "gt":
		return p.compareValues(value, condition.Value) > 0
	case "lt":
		return p.compareValues(value, condition.Value) < 0
	case "ge":
		return p.compareValues(value, condition.Value) >= 0
	case "le":
		return p.compareValues(value, condition.Value) <= 0
	case "in":
		if list, ok := condition.Value.([]interface{}); ok {
			for _, item := range list {
				if p.compareValues(value, item) == 0 {
					return true
				}
			}
		}
		return false
	case "not_in":
		if list, ok := condition.Value.([]interface{}); ok {
			for _, item := range list {
				if p.compareValues(value, item) == 0 {
					return false
				}
			}
		}
		return true
	case "like":
		if pattern, ok := condition.Value.(string); ok {
			if str := p.valueToString(value); str != "" {
				pattern = strings.ReplaceAll(pattern, "%", ".*")
				pattern = strings.ReplaceAll(pattern, "_", ".")
				if matched, _ := regexp.MatchString("^"+pattern+"$", str); matched {
					return true
				}
			}
		}
		return false
	case "regex":
		if condition.Regex != nil {
			if str := p.valueToString(value); str != "" {
				return condition.Regex.MatchString(str)
			}
		}
		return false
	default:
		return true
	}
}

// compareValues 比较两个值
func (p *RowFilterProcessor) compareValues(a, b interface{}) int {
	// 尝试转换为数字比较
	if aNum, aOk := p.toFloat64(a); aOk {
		if bNum, bOk := p.toFloat64(b); bOk {
			if aNum < bNum {
				return -1
			} else if aNum > bNum {
				return 1
			}
			return 0
		}
	}

	// 字符串比较
	aStr := p.valueToString(a)
	bStr := p.valueToString(b)
	return strings.Compare(aStr, bStr)
}

// toFloat64 尝试将值转换为 float64
func (p *RowFilterProcessor) toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// valueToString 将值转换为字符串
func (p *RowFilterProcessor) valueToString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// DataValidatorProcessor 数据验证处理器
type DataValidatorProcessor struct {
	*BaseProcessor
	rules []ValidationRule
}

// ValidationRule 验证规则
type ValidationRule struct {
	Column    string      `json:"column"`
	Required  bool        `json:"required"`
	Type      string      `json:"type"`      // string, int, float, bool, date
	MinLength int         `json:"min_length"`
	MaxLength int         `json:"max_length"`
	MinValue  interface{} `json:"min_value"`
	MaxValue  interface{} `json:"max_value"`
	Pattern   string      `json:"pattern"`
	Regex     *regexp.Regexp `json:"-"`
}

// NewDataValidatorProcessor 创建数据验证处理器
func NewDataValidatorProcessor(logger *logrus.Logger) *DataValidatorProcessor {
	return &DataValidatorProcessor{
		BaseProcessor: NewBaseProcessor("data_validator", "validator", logger),
		rules:         make([]ValidationRule, 0),
	}
}

// Configure 配置处理器
func (p *DataValidatorProcessor) Configure(config *ProcessorConfig) error {
	if err := p.BaseProcessor.Configure(config); err != nil {
		return err
	}

	// 解析验证规则
	if rulesConfig, ok := config.Parameters["rules"].([]interface{}); ok {
		for _, ruleConfig := range rulesConfig {
			if ruleMap, ok := ruleConfig.(map[string]interface{}); ok {
				rule := ValidationRule{
					Column:    getString(ruleMap, "column"),
					Required:  getBool(ruleMap, "required"),
					Type:      getString(ruleMap, "type"),
					MinLength: getInt(ruleMap, "min_length"),
					MaxLength: getInt(ruleMap, "max_length"),
					MinValue:  ruleMap["min_value"],
					MaxValue:  ruleMap["max_value"],
					Pattern:   getString(ruleMap, "pattern"),
				}

				// 如果有正则表达式，预编译
				if rule.Pattern != "" {
					if regex, err := regexp.Compile(rule.Pattern); err == nil {
						rule.Regex = regex
					} else {
						return fmt.Errorf("invalid regex pattern for column %s: %s", rule.Column, rule.Pattern)
					}
				}

				p.rules = append(p.rules, rule)
			}
		}
	}

	return nil
}

// Process 处理数据批次
func (p *DataValidatorProcessor) Process(ctx context.Context, batch DataBatch) (DataBatch, error) {
	if len(p.rules) == 0 {
		return batch, nil // 没有验证规则，直接返回
	}

	for _, row := range batch.GetRows() {
		if err := p.Validate(ctx, row); err != nil {
			return nil, fmt.Errorf("data validation failed: %w", err)
		}
	}

	return batch, nil
}

// Validate 验证数据行
func (p *DataValidatorProcessor) Validate(ctx context.Context, row DataRow) error {
	for _, rule := range p.rules {
		if err := p.validateField(row, rule); err != nil {
			return fmt.Errorf("column %s: %w", rule.Column, err)
		}
	}
	return nil
}

// validateField 验证字段
func (p *DataValidatorProcessor) validateField(row DataRow, rule ValidationRule) error {
	value := row.GetValue(rule.Column)

	// 检查必填字段
	if rule.Required && (value == nil || value == "") {
		return fmt.Errorf("required field is empty")
	}

	if value == nil {
		return nil // 非必填字段为空，跳过其他验证
	}

	// 类型验证
	if rule.Type != "" {
		if err := p.validateType(value, rule.Type); err != nil {
			return err
		}
	}

	// 长度验证
	if rule.MinLength > 0 || rule.MaxLength > 0 {
		if err := p.validateLength(value, rule.MinLength, rule.MaxLength); err != nil {
			return err
		}
	}

	// 数值范围验证
	if rule.MinValue != nil || rule.MaxValue != nil {
		if err := p.validateRange(value, rule.MinValue, rule.MaxValue); err != nil {
			return err
		}
	}

	// 正则表达式验证
	if rule.Regex != nil {
		if err := p.validatePattern(value, rule.Regex); err != nil {
			return err
		}
	}

	return nil
}

// validateType 验证类型
func (p *DataValidatorProcessor) validateType(value interface{}, expectedType string) error {
	switch expectedType {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case "int":
		if _, ok := p.toFloat64(value); !ok {
			return fmt.Errorf("expected integer, got %T", value)
		}
	case "float":
		if _, ok := p.toFloat64(value); !ok {
			return fmt.Errorf("expected float, got %T", value)
		}
	case "bool":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("expected boolean, got %T", value)
		}
	case "date":
		if _, ok := value.(time.Time); !ok {
			// 尝试解析字符串日期
			if str, ok := value.(string); ok {
				if _, err := time.Parse("2006-01-02", str); err != nil {
					if _, err := time.Parse("2006-01-02 15:04:05", str); err != nil {
						return fmt.Errorf("expected date, got invalid date string: %s", str)
					}
				}
			} else {
				return fmt.Errorf("expected date, got %T", value)
			}
		}
	}
	return nil
}

// validateLength 验证长度
func (p *DataValidatorProcessor) validateLength(value interface{}, minLen, maxLen int) error {
	str := p.valueToString(value)
	length := len(str)

	if minLen > 0 && length < minLen {
		return fmt.Errorf("length %d is less than minimum %d", length, minLen)
	}

	if maxLen > 0 && length > maxLen {
		return fmt.Errorf("length %d is greater than maximum %d", length, maxLen)
	}

	return nil
}

// validateRange 验证数值范围
func (p *DataValidatorProcessor) validateRange(value, minValue, maxValue interface{}) error {
	num, ok := p.toFloat64(value)
	if !ok {
		return fmt.Errorf("value is not numeric")
	}

	if minValue != nil {
		if minNum, ok := p.toFloat64(minValue); ok && num < minNum {
			return fmt.Errorf("value %f is less than minimum %f", num, minNum)
		}
	}

	if maxValue != nil {
		if maxNum, ok := p.toFloat64(maxValue); ok && num > maxNum {
			return fmt.Errorf("value %f is greater than maximum %f", num, maxNum)
		}
	}

	return nil
}

// validatePattern 验证正则表达式
func (p *DataValidatorProcessor) validatePattern(value interface{}, regex *regexp.Regexp) error {
	str := p.valueToString(value)
	if !regex.MatchString(str) {
		return fmt.Errorf("value does not match pattern")
	}
	return nil
}

// 辅助函数
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getBool(m map[string]interface{}, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func getInt(m map[string]interface{}, key string) int {
	if v, ok := m[key].(float64); ok {
		return int(v)
	}
	if v, ok := m[key].(int); ok {
		return v
	}
	return 0
}