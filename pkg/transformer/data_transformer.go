package transformer

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/sirupsen/logrus"

	"github.com/ck2sr/ck2sr/internal/config"
)

// DataTransformer 数据转换器
type DataTransformer struct {
	config    config.DataTransformConfig
	logger    *logrus.Logger
	allocator memory.Allocator
}

// NewDataTransformer 创建数据转换器
func NewDataTransformer(cfg config.DataTransformConfig, logger *logrus.Logger) *DataTransformer {
	if logger == nil {
		logger = logrus.New()
		logger.SetReportCaller(true)
	}

	return &DataTransformer{
		config:    cfg,
		logger:    logger,
		allocator: memory.NewGoAllocator(),
	}
}

// TransformRecord 转换 Arrow Record
func (dt *DataTransformer) TransformRecord(record arrow.Record) (arrow.Record, error) {
	if !dt.config.Enabled {
		return record, nil
	}

	schema := record.Schema()
	newSchema, err := dt.transformSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to transform schema: %w", err)
	}

	// 创建新的 Record Builder
	builder := array.NewRecordBuilder(dt.allocator, newSchema)
	defer builder.Release()

	// 处理每一列
	for colIdx := 0; colIdx < int(record.NumCols()); colIdx++ {
		column := record.Column(colIdx)
		field := schema.Field(colIdx)

		transformedColumn, err := dt.transformColumn(field, column)
		if err != nil {
			return nil, fmt.Errorf("failed to transform column %s: %w", field.Name, err)
		}

		// 将转换后的数据添加到 builder
		err = dt.copyColumnToBuilder(builder.Field(colIdx), transformedColumn)
		if err != nil {
			return nil, fmt.Errorf("failed to copy column %s to builder: %w", field.Name, err)
		}
	}

	// 构建新的 Record
	newRecord := builder.NewRecord()
	return newRecord, nil
}

// transformSchema 转换 Arrow Schema
func (dt *DataTransformer) transformSchema(schema *arrow.Schema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, schema.NumFields())

	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		newField, err := dt.transformField(field)
		if err != nil {
			return nil, fmt.Errorf("failed to transform field %s: %w", field.Name, err)
		}
		fields[i] = newField
	}

	metadata := schema.Metadata()
	return arrow.NewSchema(fields, &metadata), nil
}

// transformField 转换字段
func (dt *DataTransformer) transformField(field arrow.Field) (arrow.Field, error) {
	// 查找字段特定的转换配置
	for _, transform := range dt.config.FieldTransforms {
		if transform.Column == field.Name {
			if transform.TypeConversion != nil {
				newType, err := dt.convertArrowType(transform.TypeConversion.TargetType)
				if err != nil {
					return field, fmt.Errorf("invalid target type %s: %w", transform.TypeConversion.TargetType, err)
				}
				return arrow.Field{
					Name:     field.Name,
					Type:     newType,
					Nullable: !transform.Required,
					Metadata: field.Metadata,
				}, nil
			}
		}
	}

	// 应用全局转换规则
	sourceTypeName := dt.getArrowTypeName(field.Type)
	for _, rule := range dt.config.GlobalRules {
		if rule.SourceType == sourceTypeName {
			newType, err := dt.convertArrowType(rule.TargetType)
			if err != nil {
				dt.logger.Warnf("Invalid global rule target type %s for field %s: %v", rule.TargetType, field.Name, err)
				continue
			}
			return arrow.Field{
				Name:     field.Name,
				Type:     newType,
				Nullable: field.Nullable,
				Metadata: field.Metadata,
			}, nil
		}
	}

	// 没有转换规则，保持原样
	return field, nil
}

// transformColumn 转换列数据
func (dt *DataTransformer) transformColumn(field arrow.Field, column arrow.Array) (arrow.Array, error) {
	// 查找字段特定的转换配置
	for _, transform := range dt.config.FieldTransforms {
		if transform.Column == field.Name {
			return dt.applyFieldTransform(field, column, transform)
		}
	}

	// 应用全局转换规则
	sourceTypeName := dt.getArrowTypeName(field.Type)
	for _, rule := range dt.config.GlobalRules {
		if rule.SourceType == sourceTypeName {
			return dt.applyGlobalRule(field, column, rule)
		}
	}

	// 没有转换规则，保持原样
	column.Retain()
	return column, nil
}

// applyFieldTransform 应用字段转换
func (dt *DataTransformer) applyFieldTransform(field arrow.Field, column arrow.Array, transform config.FieldTransformConfig) (arrow.Array, error) {
	if transform.TypeConversion != nil {
		return dt.convertColumnType(field, column, *transform.TypeConversion)
	}

	// 如果没有类型转换，检查验证和默认值
	if transform.Validation != nil || transform.DefaultValue != nil {
		return dt.validateAndSetDefaults(field, column, transform)
	}

	// 没有特殊处理，保持原样
	column.Retain()
	return column, nil
}

// applyGlobalRule 应用全局规则
func (dt *DataTransformer) applyGlobalRule(field arrow.Field, column arrow.Array, rule config.TypeConversionRule) (arrow.Array, error) {
	return dt.convertColumnType(field, column, rule)
}

// convertColumnType 转换列类型
func (dt *DataTransformer) convertColumnType(field arrow.Field, column arrow.Array, rule config.TypeConversionRule) (arrow.Array, error) {
	targetType, err := dt.convertArrowType(rule.TargetType)
	if err != nil {
		return nil, fmt.Errorf("invalid target type %s: %w", rule.TargetType, err)
	}

	// 创建目标类型的 builder
	builder := array.NewBuilder(dt.allocator, targetType)
	defer builder.Release()

	// 转换每个值
	for i := 0; i < column.Len(); i++ {
		if column.IsNull(i) {
			builder.AppendNull()
			continue
		}

		value := dt.getValueFromArray(column, i)
		convertedValue, err := dt.convertValue(value, rule)
		if err != nil {
			dt.logger.Warnf("Failed to convert value %v in column %s: %v", value, field.Name, err)
			builder.AppendNull()
			continue
		}

		err = dt.appendConvertedValue(builder, convertedValue, targetType)
		if err != nil {
			return nil, fmt.Errorf("failed to append converted value: %w", err)
		}
	}

	return builder.NewArray(), nil
}

// validateAndSetDefaults 验证数据并设置默认值
func (dt *DataTransformer) validateAndSetDefaults(field arrow.Field, column arrow.Array, transform config.FieldTransformConfig) (arrow.Array, error) {
	builder := array.NewBuilder(dt.allocator, field.Type)
	defer builder.Release()

	for i := 0; i < column.Len(); i++ {
		if column.IsNull(i) {
			if transform.Required && transform.DefaultValue != nil {
				// 设置默认值
				err := dt.appendConvertedValue(builder, transform.DefaultValue, field.Type)
				if err != nil {
					return nil, fmt.Errorf("failed to append default value: %w", err)
				}
			} else if transform.Required {
				return nil, fmt.Errorf("required field %s has null value at row %d", field.Name, i)
			} else {
				builder.AppendNull()
			}
			continue
		}

		value := dt.getValueFromArray(column, i)

		// 验证值
		if transform.Validation != nil {
			if !dt.validateValue(value, *transform.Validation) {
				if transform.DefaultValue != nil {
					// 使用默认值
					err := dt.appendConvertedValue(builder, transform.DefaultValue, field.Type)
					if err != nil {
						return nil, fmt.Errorf("failed to append default value: %w", err)
					}
					continue
				} else {
					return nil, fmt.Errorf("validation failed for field %s at row %d", field.Name, i)
				}
			}
		}

		// 追加原值
		err := dt.appendConvertedValue(builder, value, field.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to append value: %w", err)
		}
	}

	return builder.NewArray(), nil
}

// convertValue 转换值
func (dt *DataTransformer) convertValue(value interface{}, rule config.TypeConversionRule) (interface{}, error) {
	if rule.Expression != "" {
		// 如果有转换表达式，应用表达式转换
		return dt.applyExpression(value, rule.Expression)
	}

	// 直接类型转换
	switch strings.ToLower(rule.TargetType) {
	case "string", "varchar", "text":
		return fmt.Sprintf("%v", value), nil
	case "int", "int32", "integer":
		return dt.convertToInt32(value)
	case "bigint", "int64", "long":
		return dt.convertToInt64(value)
	case "float", "float32":
		return dt.convertToFloat32(value)
	case "double", "float64":
		return dt.convertToFloat64(value)
	case "boolean", "bool":
		return dt.convertToBool(value)
	case "date":
		return dt.convertToDate(value)
	case "datetime", "timestamp":
		return dt.convertToTimestamp(value)
	default:
		return value, nil // 不支持的类型，保持原样
	}
}

// applyExpression 应用转换表达式（简单实现）
func (dt *DataTransformer) applyExpression(value interface{}, expression string) (interface{}, error) {
	// 这里可以实现更复杂的表达式解析
	// 目前支持一些简单的转换表达式
	expr := strings.ToLower(strings.TrimSpace(expression))

	switch expr {
	case "upper", "uppercase":
		return strings.ToUpper(fmt.Sprintf("%v", value)), nil
	case "lower", "lowercase":
		return strings.ToLower(fmt.Sprintf("%v", value)), nil
	case "trim":
		return strings.TrimSpace(fmt.Sprintf("%v", value)), nil
	default:
		// 尝试作为格式化字符串处理
		if strings.Contains(expr, "%s") {
			return fmt.Sprintf(expr, value), nil
		}
		return value, nil
	}
}

// validateValue 验证值
func (dt *DataTransformer) validateValue(value interface{}, rule config.ValidationRule) bool {
	// 检查空值
	if value == nil && !rule.AllowNull {
		return false
	}
	if value == nil && rule.AllowNull {
		return true
	}

	// 检查模式匹配
	if rule.Pattern != "" {
		matched, err := regexp.MatchString(rule.Pattern, fmt.Sprintf("%v", value))
		if err != nil || !matched {
			return false
		}
	}

	// 检查数值范围
	if rule.MinValue != nil || rule.MaxValue != nil {
		return dt.validateNumericRange(value, rule)
	}

	return true
}

// validateNumericRange 验证数值范围
func (dt *DataTransformer) validateNumericRange(value interface{}, rule config.ValidationRule) bool {
	var numVal float64
	var err error

	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		numVal = float64(v.(int64))
	case uint, uint8, uint16, uint32, uint64:
		numVal = float64(v.(uint64))
	case float32, float64:
		numVal = v.(float64)
	case string:
		numVal, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return false
		}
	default:
		return false
	}

	if rule.MinValue != nil {
		minVal := dt.convertToFloat64Interface(rule.MinValue)
		if numVal < minVal {
			return false
		}
	}

	if rule.MaxValue != nil {
		maxVal := dt.convertToFloat64Interface(rule.MaxValue)
		if numVal > maxVal {
			return false
		}
	}

	return true
}

// 辅助方法实现...
func (dt *DataTransformer) convertArrowType(typeName string) (arrow.DataType, error) {
	switch strings.ToLower(typeName) {
	case "string", "varchar", "text":
		return arrow.BinaryTypes.String, nil
	case "int", "int32", "integer":
		return arrow.PrimitiveTypes.Int32, nil
	case "bigint", "int64", "long":
		return arrow.PrimitiveTypes.Int64, nil
	case "float", "float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "double", "float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "boolean", "bool":
		return arrow.FixedWidthTypes.Boolean, nil
	case "date":
		return arrow.FixedWidthTypes.Date32, nil
	case "datetime", "timestamp":
		return arrow.FixedWidthTypes.Timestamp_us, nil
	default:
		return nil, fmt.Errorf("unsupported type: %s", typeName)
	}
}

func (dt *DataTransformer) getArrowTypeName(dataType arrow.DataType) string {
	switch dataType.ID() {
	case arrow.STRING, arrow.BINARY:
		return "string"
	case arrow.INT32:
		return "int32"
	case arrow.INT64:
		return "int64"
	case arrow.FLOAT32:
		return "float32"
	case arrow.FLOAT64:
		return "float64"
	case arrow.BOOL:
		return "boolean"
	case arrow.DATE32:
		return "date"
	case arrow.TIMESTAMP:
		return "timestamp"
	default:
		return "unknown"
	}
}

func (dt *DataTransformer) getValueFromArray(arr arrow.Array, index int) interface{} {
	switch a := arr.(type) {
	case *array.String:
		return a.Value(index)
	case *array.Int32:
		return a.Value(index)
	case *array.Int64:
		return a.Value(index)
	case *array.Float32:
		return a.Value(index)
	case *array.Float64:
		return a.Value(index)
	case *array.Boolean:
		return a.Value(index)
	case *array.Date32:
		return a.Value(index)
	case *array.Timestamp:
		return a.Value(index)
	default:
		return nil
	}
}

func (dt *DataTransformer) copyColumnToBuilder(builder array.Builder, column arrow.Array) error {
	for i := 0; i < column.Len(); i++ {
		if column.IsNull(i) {
			builder.AppendNull()
		} else {
			value := dt.getValueFromArray(column, i)
			err := dt.appendConvertedValue(builder, value, builder.Type())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (dt *DataTransformer) appendConvertedValue(builder array.Builder, value interface{}, targetType arrow.DataType) error {
	switch b := builder.(type) {
	case *array.StringBuilder:
		b.Append(fmt.Sprintf("%v", value))
	case *array.Int32Builder:
		if v, err := dt.convertToInt32(value); err == nil {
			b.Append(v)
		} else {
			return err
		}
	case *array.Int64Builder:
		if v, err := dt.convertToInt64(value); err == nil {
			b.Append(v)
		} else {
			return err
		}
	case *array.Float32Builder:
		if v, err := dt.convertToFloat32(value); err == nil {
			b.Append(v)
		} else {
			return err
		}
	case *array.Float64Builder:
		if v, err := dt.convertToFloat64(value); err == nil {
			b.Append(v)
		} else {
			return err
		}
	case *array.BooleanBuilder:
		if v, err := dt.convertToBool(value); err == nil {
			b.Append(v)
		} else {
			return err
		}
	default:
		return fmt.Errorf("unsupported builder type: %T", builder)
	}
	return nil
}

// 类型转换方法
func (dt *DataTransformer) convertToInt32(value interface{}) (int32, error) {
	switch v := value.(type) {
	case int32:
		return v, nil
	case int:
		return int32(v), nil
	case int64:
		return int32(v), nil
	case float32:
		return int32(v), nil
	case float64:
		return int32(v), nil
	case string:
		if i, err := strconv.ParseInt(v, 10, 32); err == nil {
			return int32(i), nil
		}
	}
	return 0, fmt.Errorf("cannot convert %T to int32", value)
}

func (dt *DataTransformer) convertToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %T to int64", value)
}

func (dt *DataTransformer) convertToFloat32(value interface{}) (float32, error) {
	switch v := value.(type) {
	case float32:
		return v, nil
	case float64:
		return float32(v), nil
	case int:
		return float32(v), nil
	case int32:
		return float32(v), nil
	case int64:
		return float32(v), nil
	case string:
		if f, err := strconv.ParseFloat(v, 32); err == nil {
			return float32(f), nil
		}
	}
	return 0, fmt.Errorf("cannot convert %T to float32", value)
}

func (dt *DataTransformer) convertToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, nil
		}
	}
	return 0, fmt.Errorf("cannot convert %T to float64", value)
}

func (dt *DataTransformer) convertToBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int:
		return v != 0, nil
	case int32:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case string:
		if b, err := strconv.ParseBool(v); err == nil {
			return b, nil
		}
		return strings.ToLower(v) == "true" || v == "1", nil
	}
	return false, fmt.Errorf("cannot convert %T to bool", value)
}

func (dt *DataTransformer) convertToDate(value interface{}) (arrow.Date32, error) {
	switch v := value.(type) {
	case time.Time:
		return arrow.Date32FromTime(v), nil
	case string:
		if t, err := time.Parse("2006-01-02", v); err == nil {
			return arrow.Date32FromTime(t), nil
		}
		if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
			return arrow.Date32FromTime(t), nil
		}
	}
	return 0, fmt.Errorf("cannot convert %T to date", value)
}

func (dt *DataTransformer) convertToTimestamp(value interface{}) (arrow.Timestamp, error) {
	switch v := value.(type) {
	case time.Time:
		return arrow.Timestamp(v.UnixMicro()), nil
	case string:
		if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
			return arrow.Timestamp(t.UnixMicro()), nil
		}
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			return arrow.Timestamp(t.UnixMicro()), nil
		}
	case int64:
		return arrow.Timestamp(v), nil
	}
	return 0, fmt.Errorf("cannot convert %T to timestamp", value)
}

func (dt *DataTransformer) convertToFloat64Interface(value interface{}) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int32:
		return float64(v)
	case int64:
		return float64(v)
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return 0
}