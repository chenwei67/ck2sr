package reader

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sunkaimr/ck2sr/internal/config"
)

// TestClickHouseReaderScanDestinationCreation 测试扫描目标变量创建
// 验证所有 ClickHouse 类型都能正确创建对应的 Go 类型指针
func TestClickHouseReaderScanDestinationCreation(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config: &config.DataSourceConfig{},
		logger: logger,
	}

	tests := []struct {
		name         string
		typeName     string
		expectedType string // 期望的 Go 类型名称
	}{
		// 整数类型
		{"Int8", "Int8", "*int8"},
		{"Int16", "Int16", "*int16"},
		{"Int32", "Int32", "*int32"},
		{"Int64", "Int64", "*int64"},
		{"UInt8", "UInt8", "*uint8"},
		{"UInt16", "UInt16", "*uint16"},
		{"UInt32", "UInt32", "*uint32"},
		{"UInt64", "UInt64", "*uint64"},

		// 浮点类型
		{"Float32", "Float32", "*float32"},
		{"Float64", "Float64", "*float64"},

		// 布尔类型
		{"Bool", "Bool", "*bool"},

		// 字符串类型
		{"String", "String", "*string"},
		{"FixedString", "FixedString(10)", "*string"},

		// 日期时间类型
		{"Date", "Date", "*time.Time"},
		{"Date32", "Date32", "*time.Time"},
		{"DateTime", "DateTime", "*time.Time"},
		{"DateTime64", "DateTime64(3)", "*time.Time"},

		// UUID
		{"UUID", "UUID", "*string"},

		// IPv4/IPv6
		{"IPv4", "IPv4", "*string"},
		{"IPv6", "IPv6", "*string"},

		// Enum
		{"Enum8", "Enum8('a' = 1, 'b' = 2)", "*string"},
		{"Enum16", "Enum16('x' = 1, 'y' = 2)", "*string"},

		// Decimal
		{"Decimal", "Decimal(10, 2)", "*float64"},
		{"Decimal32", "Decimal32(2)", "*float64"},
		{"Decimal64", "Decimal64(4)", "*float64"},
		{"Decimal128", "Decimal128(8)", "*float64"},

		// Array
		{"Array", "Array(Int32)", "*[]interface {}"},
		{"Array_String", "Array(String)", "*[]interface {}"},

		// Map
		{"Map", "Map(String, Int32)", "*map[string]interface {}"},

		// Tuple
		{"Tuple", "Tuple(Int32, String)", "*[]interface {}"},

		// JSON
		{"JSON", "JSON", "*string"},

		// Nullable
		{"Nullable_Int32", "Nullable(Int32)", "*int32"},
		{"Nullable_String", "Nullable(String)", "*string"},
		{"Nullable_Float64", "Nullable(Float64)", "*float64"},

		// LowCardinality
		{"LowCardinality_String", "LowCardinality(String)", "*string"},
		{"LowCardinality_Int32", "LowCardinality(Int32)", "*int32"},

		// 嵌套 Nullable + LowCardinality
		{"Nullable_LowCardinality", "Nullable(LowCardinality(String))", "*string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockType := &mockColumnType{typeName: tt.typeName}
			dest := reader.createScanDestination(mockType)

			assert.NotNil(t, dest, "Scan destination should not be nil")

			// 验证类型
			destType := reflect.TypeOf(dest).String()
			assert.Equal(t, tt.expectedType, destType, "Type mismatch for %s", tt.typeName)
		})
	}
}

// TestDerefPointer 测试指针解引用函数
func TestDerefPointer(t *testing.T) {
	// 测试 nil
	assert.Nil(t, derefPointer(nil))

	// 测试基础类型
	val := 42
	assert.Equal(t, 42, derefPointer(val))

	// 测试单层指针
	ptr1 := &val
	assert.Equal(t, 42, derefPointer(ptr1))

	// 测试多层指针
	ptr2 := &ptr1
	assert.Equal(t, 42, derefPointer(ptr2))

	ptr3 := &ptr2
	assert.Equal(t, 42, derefPointer(ptr3))

	// 测试 nil 指针
	var nilPtr *int
	assert.Nil(t, derefPointer(nilPtr))

	// 测试字符串指针
	str := "hello"
	strPtr := &str
	assert.Equal(t, "hello", derefPointer(strPtr))

	// 测试结构体指针
	type TestStruct struct {
		Value int
	}
	testStruct := TestStruct{Value: 100}
	testStructPtr := &testStruct
	result := derefPointer(testStructPtr)
	resultStruct, ok := result.(TestStruct)
	assert.True(t, ok)
	assert.Equal(t, 100, resultStruct.Value)
}

// TestExtractInnerType 测试内部类型提取函数
func TestExtractInnerType(t *testing.T) {
	tests := []struct {
		name     string
		typeName string
		wrapper  string
		expected string
	}{
		{
			name:     "Nullable_Int32",
			typeName: "Nullable(Int32)",
			wrapper:  "Nullable",
			expected: "Int32",
		},
		{
			name:     "Nullable_String",
			typeName: "Nullable(String)",
			wrapper:  "Nullable",
			expected: "String",
		},
		{
			name:     "LowCardinality_String",
			typeName: "LowCardinality(String)",
			wrapper:  "LowCardinality",
			expected: "String",
		},
		{
			name:     "Nullable_Array",
			typeName: "Nullable(Array(Int32))",
			wrapper:  "Nullable",
			expected: "Array(Int32)",
		},
		{
			name:     "No_Wrapper",
			typeName: "Int32",
			wrapper:  "Nullable",
			expected: "Int32",
		},
		{
			name:     "Nested_Nullable_LowCardinality",
			typeName: "Nullable(LowCardinality(String))",
			wrapper:  "Nullable",
			expected: "LowCardinality(String)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractInnerType(tt.typeName, tt.wrapper)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestClickHouseReaderTypeCompatibility 测试类型兼容性
// 验证从具体类型指针扫描后可以正确转换
func TestClickHouseReaderTypeCompatibility(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config: &config.DataSourceConfig{},
		logger: logger,
	}

	tests := []struct {
		name          string
		scannedValue  interface{} // 模拟从 scanDest 获取的值（指针）
		dataType      string
		expectedValue interface{}
	}{
		{
			name:          "Int32_pointer",
			scannedValue:  func() *int32 { v := int32(123); return &v }(),
			dataType:      "Int32",
			expectedValue: int32(123),
		},
		{
			name:          "String_pointer",
			scannedValue:  func() *string { v := "hello"; return &v }(),
			dataType:      "String",
			expectedValue: "hello",
		},
		{
			name:          "Float64_pointer",
			scannedValue:  func() *float64 { v := 3.14; return &v }(),
			dataType:      "Float64",
			expectedValue: 3.14,
		},
		{
			name:          "Bool_pointer",
			scannedValue:  func() *bool { v := true; return &v }(),
			dataType:      "Bool",
			expectedValue: true,
		},
		{
			name:          "Nil_pointer",
			scannedValue:  (*int32)(nil),
			dataType:      "Int32",
			expectedValue: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := reader.convertClickHouseType(tt.scannedValue, tt.dataType)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedValue, result)
		})
	}
}

// BenchmarkDerefPointer 基准测试指针解引用性能
func BenchmarkDerefPointer(b *testing.B) {
	val := int32(123)
	ptr := &val

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = derefPointer(ptr)
	}
}

// BenchmarkCreateScanDestination 基准测试扫描目标创建性能
func BenchmarkCreateScanDestination(b *testing.B) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config: &config.DataSourceConfig{},
		logger: logger,
	}

	mockType := &mockColumnType{typeName: "Int32"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = reader.createScanDestination(mockType)
	}
}
