package reader

import (
	"testing"

	"github.com/sunkaimr/ck2sr/internal/config"
)

// TestParseArrayOrJSONUnit 测试数组和JSON解析（单元测试，不需要数据库连接）
func TestParseArrayOrJSONUnit(t *testing.T) {
	// 创建一个基本的读取器，但不初始化客户端
	reader := &ClickHouseMySQLReader{
		config:         &config.DataSourceConfig{},
		excludeColumns: []string{},
		fixedValues:    make(map[string]interface{}),
	}

	// 测试标准JSON解析
	jsonStr := `{"key": "value", "number": 123}`
	result, err := reader.parseArrayOrJSON(jsonStr)
	if err != nil {
		t.Errorf("Failed to parse JSON: %v", err)
	}
	if result == nil {
		t.Error("JSON result should not be nil")
	}

	// 测试ClickHouse数组格式
	arrayStr := `['item1', 'item2', 'item3']`
	result, err = reader.parseArrayOrJSON(arrayStr)
	if err != nil {
		t.Errorf("Failed to parse ClickHouse array: %v", err)
	}

	array, ok := result.([]interface{})
	if !ok {
		t.Error("Result should be an array")
	}
	if len(array) != 3 {
		t.Errorf("Expected array length 3, got %d", len(array))
	}
	if array[0] != "item1" {
		t.Errorf("Expected first item 'item1', got '%v'", array[0])
	}

	// 测试数字数组
	numberArrayStr := `[1, 2, 3, 4.5]`
	result, err = reader.parseArrayOrJSON(numberArrayStr)
	if err != nil {
		t.Errorf("Failed to parse number array: %v", err)
	}

	array, ok = result.([]interface{})
	if !ok {
		t.Error("Result should be an array")
	}
	if len(array) != 4 {
		t.Errorf("Expected array length 4, got %d", len(array))
	}

	// 验证数字类型 - JSON格式数组所有数字都解析为float64
	if array[0] != float64(1) {
		t.Errorf("Expected first item to be float64(1), got %v (%T)", array[0], array[0])
	}
	if array[3] != 4.5 {
		t.Errorf("Expected last item to be 4.5, got %v (%T)", array[3], array[3])
	}

	// 测试空数组
	emptyArrayStr := `[]`
	result, err = reader.parseArrayOrJSON(emptyArrayStr)
	if err != nil {
		t.Errorf("Failed to parse empty array: %v", err)
	}

	array, ok = result.([]interface{})
	if !ok {
		t.Error("Result should be an array")
	}
	if len(array) != 0 {
		t.Errorf("Expected empty array, got length %d", len(array))
	}

	// 测试混合类型数组
	mixedArrayStr := `['text', 42, true, 3.14]`
	result, err = reader.parseArrayOrJSON(mixedArrayStr)
	if err != nil {
		t.Errorf("Failed to parse mixed array: %v", err)
	}

	array, ok = result.([]interface{})
	if !ok {
		t.Error("Result should be an array")
	}
	if len(array) != 4 {
		t.Errorf("Expected array length 4, got %d", len(array))
	}

	// 验证混合类型 - ClickHouse数组格式，整数解析为int64
	if array[0] != "text" {
		t.Errorf("Expected string 'text', got %v (%T)", array[0], array[0])
	}
	if array[1] != int64(42) {
		t.Errorf("Expected int64(42), got %v (%T)", array[1], array[1])
	}
	if array[2] != true {
		t.Errorf("Expected bool true, got %v (%T)", array[2], array[2])
	}
	if array[3] != 3.14 {
		t.Errorf("Expected float64(3.14), got %v (%T)", array[3], array[3])
	}

	// 测试无效格式
	invalidStr := `not_json_or_array`
	_, err = reader.parseArrayOrJSON(invalidStr)
	if err == nil {
		t.Error("Should return error for invalid format")
	}

	// 测试部分无效的数组格式
	invalidArrayStr := `['item1', 'item2'`  // 缺少结束括号
	_, err = reader.parseArrayOrJSON(invalidArrayStr)
	if err == nil {
		t.Error("Should return error for invalid array format")
	}
}

// TestSplitArrayItems 测试数组项分割功能
func TestSplitArrayItems(t *testing.T) {
	reader := &ClickHouseMySQLReader{}

	// 测试简单分割
	input := "item1,item2,item3"
	result := reader.splitArrayItems(input)
	expected := []string{"item1", "item2", "item3"}

	if len(result) != len(expected) {
		t.Errorf("Expected length %d, got %d", len(expected), len(result))
	}

	for i, item := range result {
		if item != expected[i] {
			t.Errorf("Expected item %d to be '%s', got '%s'", i, expected[i], item)
		}
	}

	// 测试带引号的分割
	input = "'item1','item2,with,comma','item3'"
	result = reader.splitArrayItems(input)
	expected = []string{"'item1'", "'item2,with,comma'", "'item3'"}

	if len(result) != len(expected) {
		t.Errorf("Expected length %d, got %d", len(expected), len(result))
	}

	for i, item := range result {
		if item != expected[i] {
			t.Errorf("Expected item %d to be '%s', got '%s'", i, expected[i], item)
		}
	}

	// 测试双引号
	input = `"item1","item2,with,comma","item3"`
	result = reader.splitArrayItems(input)
	expected = []string{`"item1"`, `"item2,with,comma"`, `"item3"`}

	if len(result) != len(expected) {
		t.Errorf("Expected length %d, got %d", len(expected), len(result))
	}

	for i, item := range result {
		if item != expected[i] {
			t.Errorf("Expected item %d to be '%s', got '%s'", i, expected[i], item)
		}
	}
}

// TestColumnFiltering 测试列过滤功能
func TestColumnFiltering(t *testing.T) {
	reader := &ClickHouseMySQLReader{
		excludeColumns: []string{"secret", "internal_id"},
		fixedValues:    make(map[string]interface{}),
	}

	// 测试排除列检查
	if !reader.isColumnExcluded("secret") {
		t.Error("'secret' should be excluded")
	}

	if !reader.isColumnExcluded("internal_id") {
		t.Error("'internal_id' should be excluded")
	}

	if reader.isColumnExcluded("public_column") {
		t.Error("'public_column' should not be excluded")
	}

	if reader.isColumnExcluded("") {
		t.Error("Empty column name should not be excluded")
	}
}

// TestOffsetOperations 测试偏移量操作
func TestOffsetOperations(t *testing.T) {
	// 偏移量功能已被移除，跳过此测试
	t.Skip("Offset functionality has been removed from the reader implementation")
}