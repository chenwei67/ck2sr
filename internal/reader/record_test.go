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

// TestMarshalJSON_RawValueWithSpecialChars 测试RawValue特殊字符转义
func TestMarshalJSON_RawValueWithSpecialChars(t *testing.T) {
	columns := NewColumnMetadata(
		[]string{"col1", "col2"},
		[]string{"String", "String"},
	)

	record := NewRecord(columns)
	record.Values[0] = "normal"
	record.Values[1] = RawValue{
		IsJSON: false,
		Data:   []byte(`value|with"special\chars`), // 包含 |, ", \
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

	// 验证值正确转义
	if result["col2"] != `value|with"special\chars` {
		t.Errorf("Special characters not properly escaped: %v", result["col2"])
	}
}

// TestMarshalJSON_RawValueJSON 测试RawValue JSON直传
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
		t.Fatalf("Generated invalid JSON: %v", err)
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

// TestMarshalJSON_ControlCharacters 测试控制字符转义
func TestMarshalJSON_ControlCharacters(t *testing.T) {
	columns := NewColumnMetadata(
		[]string{"col1"},
		[]string{"String"},
	)

	record := NewRecord(columns)
	record.Values[0] = RawValue{
		IsJSON: false,
		Data:   []byte("line1\nline2\ttab\rcarriage"),
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

	// 验证控制字符被正确转义
	expected := "line1\nline2\ttab\rcarriage"
	if result["col1"] != expected {
		t.Errorf("Control characters not properly handled: got %q, want %q", result["col1"], expected)
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
	record.Values[5] = RawValue{IsJSON: false, Data: []byte(`[1,2,3]`)}

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
	if result["array"] != "[1,2,3]" {
		t.Errorf("Array value incorrect: %v", result["array"])
	}
}
