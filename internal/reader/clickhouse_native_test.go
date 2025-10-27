package reader

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sunkaimr/ck2sr/internal/config"
)

// TestClickHouseReaderTypeConversions 测试ClickHouse类型转换
// 这是单元测试，不需要真实的数据库连接
func TestClickHouseReaderTypeConversions(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config:         &config.DataSourceConfig{},
		logger:         logger,
		excludeColumns: []string{},
		fixedValues:    make(map[string]interface{}),
	}

	tests := []struct {
		name     string
		value    interface{}
		dataType string
		want     interface{}
		wantErr  bool
	}{
		// 基础数值类型
		{
			name:     "Int8",
			value:    int8(127),
			dataType: "Int8",
			want:     int8(127),
			wantErr:  false,
		},
		{
			name:     "Int16",
			value:    int16(32767),
			dataType: "Int16",
			want:     int16(32767),
			wantErr:  false,
		},
		{
			name:     "Int32",
			value:    int32(2147483647),
			dataType: "Int32",
			want:     int32(2147483647),
			wantErr:  false,
		},
		{
			name:     "Int64",
			value:    int64(9223372036854775807),
			dataType: "Int64",
			want:     int64(9223372036854775807),
			wantErr:  false,
		},
		{
			name:     "UInt8",
			value:    uint8(255),
			dataType: "UInt8",
			want:     uint8(255),
			wantErr:  false,
		},
		{
			name:     "UInt16",
			value:    uint16(65535),
			dataType: "UInt16",
			want:     uint16(65535),
			wantErr:  false,
		},
		{
			name:     "UInt32",
			value:    uint32(4294967295),
			dataType: "UInt32",
			want:     uint32(4294967295),
			wantErr:  false,
		},
		{
			name:     "UInt64",
			value:    uint64(18446744073709551615),
			dataType: "UInt64",
			want:     uint64(18446744073709551615),
			wantErr:  false,
		},
		{
			name:     "Float32",
			value:    float32(3.14),
			dataType: "Float32",
			want:     float32(3.14),
			wantErr:  false,
		},
		{
			name:     "Float64",
			value:    float64(3.141592653589793),
			dataType: "Float64",
			want:     float64(3.141592653589793),
			wantErr:  false,
		},
		// 字符串类型
		{
			name:     "String",
			value:    "Hello, ClickHouse!",
			dataType: "String",
			want:     "Hello, ClickHouse!",
			wantErr:  false,
		},
		{
			name:     "FixedString",
			value:    "FixedLen",
			dataType: "FixedString(8)",
			want:     "FixedLen",
			wantErr:  false,
		},
		// 布尔类型
		{
			name:     "Bool_true",
			value:    true,
			dataType: "Bool",
			want:     true,
			wantErr:  false,
		},
		{
			name:     "Bool_false",
			value:    false,
			dataType: "Bool",
			want:     false,
			wantErr:  false,
		},
		// UUID类型
		{
			name:     "UUID",
			value:    "550e8400-e29b-41d4-a716-446655440000",
			dataType: "UUID",
			want:     "550e8400-e29b-41d4-a716-446655440000",
			wantErr:  false,
		},
		// IPv4/IPv6类型
		{
			name:     "IPv4",
			value:    "192.168.1.1",
			dataType: "IPv4",
			want:     "192.168.1.1",
			wantErr:  false,
		},
		{
			name:     "IPv6",
			value:    "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
			dataType: "IPv6",
			want:     "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
			wantErr:  false,
		},
		// Enum类型
		{
			name:     "Enum8",
			value:    "active",
			dataType: "Enum8('active' = 1, 'inactive' = 2)",
			want:     "active",
			wantErr:  false,
		},
		{
			name:     "Enum16",
			value:    "success",
			dataType: "Enum16('success' = 1, 'failure' = 2)",
			want:     "success",
			wantErr:  false,
		},
		// Decimal类型
		{
			name:     "Decimal_float64",
			value:    float64(123.456),
			dataType: "Decimal(10, 3)",
			want:     float64(123.456),
			wantErr:  false,
		},
		{
			name:     "Decimal_string",
			value:    "123.456",
			dataType: "Decimal(10, 3)",
			want:     float64(123.456),
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := reader.convertClickHouseType(tt.value, tt.dataType)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

// TestClickHouseReaderDateTimeConversions 测试日期时间类型转换
func TestClickHouseReaderDateTimeConversions(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config:         &config.DataSourceConfig{},
		logger:         logger,
		excludeColumns: []string{},
		fixedValues:    make(map[string]interface{}),
	}

	// 测试DateTime转换
	now := time.Date(2024, 10, 24, 12, 30, 45, 0, time.UTC)
	got, err := reader.convertDateTime(now)
	assert.NoError(t, err)
	assert.Equal(t, now.Format(time.RFC3339), got)

	// 测试Date转换
	dateOnly := time.Date(2024, 10, 24, 0, 0, 0, 0, time.UTC)
	got, err = reader.convertDate(dateOnly)
	assert.NoError(t, err)
	assert.Equal(t, "2024-10-24", got)

	// 测试DateTime64转换
	got, err = reader.convertClickHouseType(now, "DateTime64(3)")
	assert.NoError(t, err)
	assert.Equal(t, now.Format(time.RFC3339), got)
}

// TestClickHouseReaderArrayConversions 测试数组类型转换
func TestClickHouseReaderArrayConversions(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config:         &config.DataSourceConfig{},
		logger:         logger,
		excludeColumns: []string{},
		fixedValues:    make(map[string]interface{}),
	}

	tests := []struct {
		name     string
		value    interface{}
		dataType string
		wantType string // "array" or "rawvalue"
	}{
		{
			name:     "Array_of_Int",
			value:    []interface{}{int64(1), int64(2), int64(3)},
			dataType: "Array(Int32)",
			wantType: "array",
		},
		{
			name:     "Array_of_String",
			value:    []interface{}{"a", "b", "c"},
			dataType: "Array(String)",
			wantType: "array",
		},
		{
			name:     "Array_as_String_JSON",
			value:    "[1, 2, 3]",
			dataType: "Array(Int32)",
			wantType: "rawvalue",
		},
		{
			name:     "Nested_Array",
			value:    []interface{}{[]interface{}{1, 2}, []interface{}{3, 4}},
			dataType: "Array(Array(Int32))",
			wantType: "array",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := reader.convertClickHouseType(tt.value, tt.dataType)
			assert.NoError(t, err)

			switch tt.wantType {
			case "array":
				_, ok := got.([]interface{})
				assert.True(t, ok, "Expected array type")
			case "rawvalue":
				_, ok := got.(RawValue)
				assert.True(t, ok, "Expected RawValue type")
			}
		})
	}
}

// TestClickHouseReaderMapConversions 测试Map类型转换
func TestClickHouseReaderMapConversions(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config:         &config.DataSourceConfig{},
		logger:         logger,
		excludeColumns: []string{},
		fixedValues:    make(map[string]interface{}),
	}

	tests := []struct {
		name     string
		value    interface{}
		dataType string
		wantKeys []string
	}{
		{
			name: "Map_string_to_int",
			value: map[interface{}]interface{}{
				"key1": int64(100),
				"key2": int64(200),
			},
			dataType: "Map(String, Int32)",
			wantKeys: []string{"key1", "key2"},
		},
		{
			name: "Map_already_string_keys",
			value: map[string]interface{}{
				"name":  "Alice",
				"age":   int64(30),
				"city":  "Beijing",
			},
			dataType: "Map(String, String)",
			wantKeys: []string{"name", "age", "city"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := reader.convertClickHouseType(tt.value, tt.dataType)
			assert.NoError(t, err)

			resultMap, ok := got.(map[string]interface{})
			assert.True(t, ok, "Expected map[string]interface{}")

			for _, key := range tt.wantKeys {
				_, exists := resultMap[key]
				assert.True(t, exists, "Expected key %s to exist", key)
			}
		})
	}
}

// TestClickHouseReaderTupleConversions 测试Tuple类型转换
func TestClickHouseReaderTupleConversions(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config:         &config.DataSourceConfig{},
		logger:         logger,
		excludeColumns: []string{},
		fixedValues:    make(map[string]interface{}),
	}

	// Tuple通常表示为数组
	tupleValue := []interface{}{"Alice", int64(30), true}
	got, err := reader.convertClickHouseType(tupleValue, "Tuple(String, Int32, Bool)")
	assert.NoError(t, err)

	result, ok := got.([]interface{})
	assert.True(t, ok, "Expected array type for Tuple")
	assert.Equal(t, 3, len(result))
	assert.Equal(t, "Alice", result[0])
	assert.Equal(t, int64(30), result[1])
	assert.Equal(t, true, result[2])
}

// TestClickHouseReaderNullableTypes 测试Nullable类型
func TestClickHouseReaderNullableTypes(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config:         &config.DataSourceConfig{},
		logger:         logger,
		excludeColumns: []string{},
		fixedValues:    make(map[string]interface{}),
	}

	// NULL值测试
	got, err := reader.convertClickHouseType(nil, "Nullable(String)")
	assert.NoError(t, err)
	assert.Nil(t, got)

	// 非NULL值测试
	got, err = reader.convertClickHouseType("value", "Nullable(String)")
	assert.NoError(t, err)
	assert.Equal(t, "value", got)
}

// TestClickHouseReaderColumnFilter 测试列过滤功能
func TestClickHouseReaderColumnFilter(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config: &config.DataSourceConfig{},
		logger: logger,
		excludeColumns: []string{"password", "internal_id"},
		fixedValues: map[string]interface{}{
			"tenant_id": "tenant_123",
		},
	}

	// 测试排除列检查
	assert.True(t, reader.isColumnExcluded("password"))
	assert.True(t, reader.isColumnExcluded("internal_id"))
	assert.False(t, reader.isColumnExcluded("username"))
	assert.False(t, reader.isColumnExcluded("email"))
}

// TestClickHouseReaderRecordPool 测试对象池功能
func TestClickHouseReaderRecordPool(t *testing.T) {
	columns := []string{"id", "name", "age"}
	dataTypes := []string{"Int32", "String", "Int32"}
	metadata := NewColumnMetadata(columns, dataTypes)

	pool := NewRecordPool(metadata)

	// 从池中获取记录
	record1 := pool.Get()
	assert.NotNil(t, record1)
	assert.Equal(t, 3, len(record1.Values))

	// 设置值
	record1.Values[0] = int32(1)
	record1.Values[1] = "Alice"
	record1.Values[2] = int32(30)

	// 归还到池
	pool.Put(record1)

	// 再次获取（应该是同一个对象，但已重置）
	record2 := pool.Get()
	assert.NotNil(t, record2)

	// 验证已重置
	for _, val := range record2.Values {
		assert.Nil(t, val, "Record should be reset after Put")
	}
}

// TestClickHouseReaderJSONSerialization 测试JSON序列化性能
func TestClickHouseReaderJSONSerialization(t *testing.T) {
	columns := []string{"id", "name", "tags", "metadata"}
	dataTypes := []string{"Int32", "String", "Array(String)", "Map(String, String)"}
	metadata := NewColumnMetadata(columns, dataTypes)

	record := NewRecord(metadata)
	record.Values[0] = int32(123)
	record.Values[1] = "test_user"
	record.Values[2] = []interface{}{"tag1", "tag2", "tag3"}
	record.Values[3] = map[string]interface{}{"key1": "value1", "key2": "value2"}

	// 序列化为JSON
	jsonBytes, err := json.Marshal(record)
	assert.NoError(t, err)
	assert.NotEmpty(t, jsonBytes)

	// 验证JSON格式
	var result map[string]interface{}
	err = json.Unmarshal(jsonBytes, &result)
	assert.NoError(t, err)

	assert.Equal(t, float64(123), result["id"]) // JSON数字默认为float64
	assert.Equal(t, "test_user", result["name"])

	// 验证数组
	tags, ok := result["tags"].([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 3, len(tags))

	// 验证Map
	meta, ok := result["metadata"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "value1", meta["key1"])
}

// TestClickHouseReaderRawValueHandling 测试RawValue延迟解析
func TestClickHouseReaderRawValueHandling(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config:         &config.DataSourceConfig{},
		logger:         logger,
		excludeColumns: []string{},
		fixedValues:    make(map[string]interface{}),
	}

	// 测试JSON类型保存为RawValue
	jsonStr := `{"key": "value", "number": 123}`
	got, err := reader.convertClickHouseType(jsonStr, "JSON")
	assert.NoError(t, err)

	rawVal, ok := got.(RawValue)
	assert.True(t, ok, "Expected RawValue for JSON type")
	assert.True(t, rawVal.IsJSON)
	assert.Equal(t, []byte(jsonStr), rawVal.Data)
}

// TestClickHouseReaderComplexNestedTypes 测试复杂嵌套类型
func TestClickHouseReaderComplexNestedTypes(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config:         &config.DataSourceConfig{},
		logger:         logger,
		excludeColumns: []string{},
		fixedValues:    make(map[string]interface{}),
	}

	tests := []struct {
		name     string
		value    interface{}
		dataType string
		validate func(t *testing.T, result interface{})
	}{
		{
			name: "Array_of_Map",
			value: []interface{}{
				map[string]interface{}{"name": "Alice", "age": 30},
				map[string]interface{}{"name": "Bob", "age": 25},
			},
			dataType: "Array(Map(String, String))",
			validate: func(t *testing.T, result interface{}) {
				arr, ok := result.([]interface{})
				require.True(t, ok)
				assert.Equal(t, 2, len(arr))
			},
		},
		{
			name: "Map_of_Array",
			value: map[string]interface{}{
				"tags":  []interface{}{"tag1", "tag2"},
				"codes": []interface{}{int64(1), int64(2), int64(3)},
			},
			dataType: "Map(String, Array(String))",
			validate: func(t *testing.T, result interface{}) {
				m, ok := result.(map[string]interface{})
				require.True(t, ok)
				assert.Contains(t, m, "tags")
				assert.Contains(t, m, "codes")
			},
		},
		{
			name: "Nested_Tuple",
			value: []interface{}{
				"user1",
				[]interface{}{"permission1", "permission2"},
				map[string]interface{}{"role": "admin"},
			},
			dataType: "Tuple(String, Array(String), Map(String, String))",
			validate: func(t *testing.T, result interface{}) {
				arr, ok := result.([]interface{})
				require.True(t, ok)
				assert.Equal(t, 3, len(arr))
				assert.Equal(t, "user1", arr[0])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := reader.convertClickHouseType(tt.value, tt.dataType)
			assert.NoError(t, err)
			tt.validate(t, got)
		})
	}
}

// TestClickHouseReaderLowCardinalityTypes 测试LowCardinality类型
func TestClickHouseReaderLowCardinalityTypes(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config:         &config.DataSourceConfig{},
		logger:         logger,
		excludeColumns: []string{},
		fixedValues:    make(map[string]interface{}),
	}

	// LowCardinality类型通常直接返回底层值
	value := "category_name"
	got, err := reader.convertClickHouseType(value, "LowCardinality(String)")
	assert.NoError(t, err)
	assert.Equal(t, value, got)
}

// TestClickHouseReaderGeoTypes 测试地理位置类型
func TestClickHouseReaderGeoTypes(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config:         &config.DataSourceConfig{},
		logger:         logger,
		excludeColumns: []string{},
		fixedValues:    make(map[string]interface{}),
	}

	// Point类型测试
	pointValue := "(10.5, 20.3)"
	got, err := reader.convertClickHouseType(pointValue, "Point")
	assert.NoError(t, err)
	assert.Equal(t, pointValue, got)

	// Polygon类型测试
	polygonValue := "[(0,0), (10,0), (10,10), (0,10)]"
	got, err = reader.convertClickHouseType(polygonValue, "Polygon")
	assert.NoError(t, err)
	assert.Equal(t, polygonValue, got)
}

// createTestLogger 创建测试用的logger
func createTestLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetLevel(logrus.WarnLevel) // 测试时减少日志输出
	return logger
}
