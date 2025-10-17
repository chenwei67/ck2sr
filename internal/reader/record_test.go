package reader

import (
	"encoding/json"
	"testing"
)

// TestMarshalJSON_NilValues 测试nil值处理
func TestMarshalJSON_NilValues(t *testing.T) {
	columns := NewColumnMetadata(
		[]string{"col1", "col2", "col3"},
		[]string{"String", "Int", "String"},
	)

	record := NewRecord(columns)
	record.Values[0] = "value1"
	record.Values[1] = nil // NULL值
	record.Values[2] = "value3"

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	expected := `{"col1":"value1","col3":"value3"}`
	if string(data) != expected {
		t.Errorf("Expected %s, got %s", expected, string(data))
	}
}

// TestMarshalJSON_RawValueArray 测试RawValue ClickHouse数组转JSON
// 用户策略：所有RawValue都视为JSON，单引号转双引号
func TestMarshalJSON_RawValueArray(t *testing.T) {
	columns := NewColumnMetadata(
		[]string{"col1", "col2"},
		[]string{"String", "Array"},
	)

	record := NewRecord(columns)
	record.Values[0] = "normal"
	// ClickHouse数组格式：['item1','item2']
	// 策略：IsJSON=true，序列化时转换单引号为双引号
	record.Values[1] = RawValue{
		IsJSON: true,
		Data:   []byte(`['item1','item2']`),
	}

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	// 验证JSON可以正确解析
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Generated invalid JSON: %v\nJSON: %s", err, string(data))
	}

	// 验证数组被正确转换
	arr, ok := result["col2"].([]interface{})
	if !ok {
		t.Fatalf("col2 should be array, got: %T %v", result["col2"], result["col2"])
	}
	if len(arr) != 2 || arr[0] != "item1" || arr[1] != "item2" {
		t.Errorf("Array values incorrect: %v", arr)
	}
}

// TestMarshalJSON_RawValueJSON 测试RawValue JSON对象直传
func TestMarshalJSON_RawValueJSON(t *testing.T) {
	columns := NewColumnMetadata(
		[]string{"col1", "col2"},
		[]string{"String", "JSON"},
	)

	record := NewRecord(columns)
	record.Values[0] = "test"
	record.Values[1] = RawValue{
		IsJSON: true,
		Data:   []byte(`{"nested":"value","count":123}`),
	}

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	// 验证JSON结构
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Generated invalid JSON: %v\nJSON: %s", err, string(data))
	}

	// 验证嵌套JSON被正确处理
	nested, ok := result["col2"].(map[string]interface{})
	if !ok {
		t.Fatalf("RawValue JSON not properly embedded: %v", result["col2"])
	}
	if nested["nested"] != "value" || nested["count"].(float64) != 123 {
		t.Errorf("Nested JSON values incorrect: %v", nested)
	}
}

// TestMarshalJSON_ClickHouseArrayWithSpecialChars 测试包含特殊字符的ClickHouse数组
func TestMarshalJSON_ClickHouseArrayWithSpecialChars(t *testing.T) {
	columns := NewColumnMetadata(
		[]string{"col1"},
		[]string{"Array"},
	)

	record := NewRecord(columns)
	// ClickHouse数组包含特殊字符（反斜杠、双引号等）
	record.Values[0] = RawValue{
		IsJSON: true,
		Data:   []byte(`['item\\1','item\"2']`), // 已经是转义后的格式
	}

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	// 验证JSON有效
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Generated invalid JSON: %v\nJSON: %s", err, string(data))
	}

	// 验证数组内容
	arr, ok := result["col1"].([]interface{})
	if !ok {
		t.Fatalf("col1 should be array, got: %T", result["col1"])
	}
	if len(arr) != 2 {
		t.Errorf("Expected 2 items, got %d", len(arr))
	}
}

// TestMarshalJSON_MixedTypes 测试混合类型
func TestMarshalJSON_MixedTypes(t *testing.T) {
	columns := NewColumnMetadata(
		[]string{"str", "num", "bool", "nil_col", "json", "array"},
		[]string{"String", "Int", "Bool", "String", "JSON", "Array"},
	)

	record := NewRecord(columns)
	record.Values[0] = "text"
	record.Values[1] = int64(42)
	record.Values[2] = true
	record.Values[3] = nil // NULL
	record.Values[4] = RawValue{IsJSON: true, Data: []byte(`{"key":"val"}`)}
	record.Values[5] = RawValue{IsJSON: true, Data: []byte(`[1,2,3]`)}

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	// 验证JSON有效
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Generated invalid JSON: %v\nJSON: %s", err, string(data))
	}

	// 验证各类型值
	if result["str"] != "text" {
		t.Errorf("String value incorrect: %v", result["str"])
	}
	if result["num"].(float64) != 42 {
		t.Errorf("Number value incorrect: %v", result["num"])
	}
	if result["bool"] != true {
		t.Errorf("Bool value incorrect: %v", result["bool"])
	}
	if _, exists := result["nil_col"]; exists {
		t.Errorf("Nil column should not be present in JSON")
	}

	// 验证JSON数组
	arr, ok := result["array"].([]interface{})
	if !ok {
		t.Fatalf("array should be []interface{}, got: %T", result["array"])
	}
	if len(arr) != 3 {
		t.Errorf("Array length incorrect: %d", len(arr))
	}
}

// TestMarshalJSON_EmptyArray 测试空数组
func TestMarshalJSON_EmptyArray(t *testing.T) {
	columns := NewColumnMetadata(
		[]string{"col1", "col2"},
		[]string{"String", "Array"},
	)

	record := NewRecord(columns)
	record.Values[0] = "test"
	record.Values[1] = RawValue{
		IsJSON: true,
		Data:   []byte(`[]`),
	}

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	// 验证JSON有效
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Generated invalid JSON: %v\nJSON: %s", err, string(data))
	}

	// 验证空数组
	arr, ok := result["col2"].([]interface{})
	if !ok {
		t.Fatalf("col2 should be array, got: %T", result["col2"])
	}
	if len(arr) != 0 {
		t.Errorf("Expected empty array, got length %d", len(arr))
	}
}

// TestMarshalJSON_NestedJSON 测试嵌套JSON结构
func TestMarshalJSON_NestedJSON(t *testing.T) {
	columns := NewColumnMetadata(
		[]string{"col1"},
		[]string{"JSON"},
	)

	record := NewRecord(columns)
	record.Values[0] = RawValue{
		IsJSON: true,
		Data:   []byte(`{"users":[{"name":"Alice","age":30},{"name":"Bob","age":25}]}`),
	}

	data, err := json.Marshal(record)
	if err != nil {
		t.Fatalf("MarshalJSON failed: %v", err)
	}

	// 验证JSON有效
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Generated invalid JSON: %v\nJSON: %s", err, string(data))
	}

	// 验证嵌套结构
	obj, ok := result["col1"].(map[string]interface{})
	if !ok {
		t.Fatalf("col1 should be object, got: %T", result["col1"])
	}

	users, ok := obj["users"].([]interface{})
	if !ok {
		t.Fatalf("users should be array, got: %T", obj["users"])
	}
	if len(users) != 2 {
		t.Errorf("Expected 2 users, got %d", len(users))
	}
}

