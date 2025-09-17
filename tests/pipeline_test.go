package pipeline

import (
	"context"
	"testing"
)

func TestSimpleDataRow(t *testing.T) {
	columns := []string{"id", "name", "age"}
	values := []interface{}{1, "John", 30}

	row := NewSimpleDataRow(columns, values)

	// 测试获取列名
	gotColumns := row.GetColumns()
	if len(gotColumns) != len(columns) {
		t.Errorf("Expected %d columns, got %d", len(columns), len(gotColumns))
	}

	// 测试获取值
	if row.GetValue("name") != "John" {
		t.Errorf("Expected name 'John', got %v", row.GetValue("name"))
	}

	// 测试设置值
	row.SetValue("email", "john@example.com")
	if row.GetValue("email") != "john@example.com" {
		t.Errorf("Expected email 'john@example.com', got %v", row.GetValue("email"))
	}

	// 测试转换为 CSV
	csv := row.ToCSV()
	if len(csv) != 4 { // 原来3列 + 新增1列
		t.Errorf("Expected 4 CSV fields, got %d", len(csv))
	}

	// 测试克隆
	cloned := row.Clone()
	cloned.SetValue("age", 31)

	if row.GetValue("age") == cloned.GetValue("age") {
		t.Error("Clone should be independent of original")
	}

	// 测试大小估算
	size := row.Size()
	if size <= 0 {
		t.Error("Size should be greater than 0")
	}
}

func TestSimpleDataBatch(t *testing.T) {
	batch := NewSimpleDataBatch()

	if !batch.IsEmpty() {
		t.Error("New batch should be empty")
	}

	// 添加数据行
	row1 := NewSimpleDataRow([]string{"id", "name"}, []interface{}{1, "Alice"})
	row2 := NewSimpleDataRow([]string{"id", "name"}, []interface{}{2, "Bob"})

	batch.AddRow(row1)
	batch.AddRow(row2)

	if batch.Size() != 2 {
		t.Errorf("Expected batch size 2, got %d", batch.Size())
	}

	if batch.IsEmpty() {
		t.Error("Batch should not be empty after adding rows")
	}

	// 测试字节大小
	bytes := batch.Bytes()
	if bytes <= 0 {
		t.Error("Batch bytes should be greater than 0")
	}

	// 测试克隆
	cloned := batch.Clone()
	if cloned.Size() != batch.Size() {
		t.Error("Cloned batch should have same size")
	}

	// 测试清空
	batch.Clear()
	if !batch.IsEmpty() {
		t.Error("Batch should be empty after clear")
	}

	if cloned.IsEmpty() {
		t.Error("Cloned batch should not be affected by original clear")
	}
}

func TestColumnMappingProcessor(t *testing.T) {
	processor := NewColumnMappingProcessor(nil)

	// 配置列映射
	config := &ProcessorConfig{
		Name: "test_mapping",
		Type: "column_mapping",
		Parameters: map[string]interface{}{
			"mapping": map[string]interface{}{
				"old_name": "new_name",
				"old_id":   "new_id",
			},
		},
	}

	err := processor.Configure(config)
	if err != nil {
		t.Fatalf("Failed to configure processor: %v", err)
	}

	// 创建测试数据
	row := NewSimpleDataRow(
		[]string{"old_id", "old_name", "unchanged"},
		[]interface{}{1, "test", "value"},
	)

	batch := NewSimpleDataBatch()
	batch.AddRow(row)

	// 处理数据
	ctx := context.Background()
	result, err := processor.Process(ctx, batch)
	if err != nil {
		t.Fatalf("Failed to process batch: %v", err)
	}

	if result.Size() != 1 {
		t.Errorf("Expected 1 row in result, got %d", result.Size())
	}

	resultRow := result.GetRows()[0]

	// 检查映射后的列名
	if resultRow.GetValue("new_id") != 1 {
		t.Errorf("Expected new_id to be 1, got %v", resultRow.GetValue("new_id"))
	}

	if resultRow.GetValue("new_name") != "test" {
		t.Errorf("Expected new_name to be 'test', got %v", resultRow.GetValue("new_name"))
	}

	if resultRow.GetValue("unchanged") != "value" {
		t.Errorf("Expected unchanged to be 'value', got %v", resultRow.GetValue("unchanged"))
	}

	// 原列名应该不存在
	if resultRow.GetValue("old_id") != nil {
		t.Error("old_id should not exist in result")
	}
}

func TestRowFilterProcessor(t *testing.T) {
	processor := NewRowFilterProcessor(nil)

	// 配置过滤条件
	config := &ProcessorConfig{
		Name: "test_filter",
		Type: "row_filter",
		Parameters: map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"column":   "age",
					"operator": "gt",
					"value":    18,
				},
				map[string]interface{}{
					"column":   "status",
					"operator": "eq",
					"value":    "active",
				},
			},
		},
	}

	err := processor.Configure(config)
	if err != nil {
		t.Fatalf("Failed to configure processor: %v", err)
	}

	// 创建测试数据
	batch := NewSimpleDataBatch()

	// 应该通过过滤的行
	row1 := NewSimpleDataRow(
		[]string{"age", "status", "name"},
		[]interface{}{25, "active", "Alice"},
	)

	// 应该被过滤掉的行（age <= 18）
	row2 := NewSimpleDataRow(
		[]string{"age", "status", "name"},
		[]interface{}{16, "active", "Bob"},
	)

	// 应该被过滤掉的行（status != "active"）
	row3 := NewSimpleDataRow(
		[]string{"age", "status", "name"},
		[]interface{}{25, "inactive", "Charlie"},
	)

	batch.AddRow(row1)
	batch.AddRow(row2)
	batch.AddRow(row3)

	// 处理数据
	ctx := context.Background()
	result, err := processor.Process(ctx, batch)
	if err != nil {
		t.Fatalf("Failed to process batch: %v", err)
	}

	if result.Size() != 1 {
		t.Errorf("Expected 1 row in result, got %d", result.Size())
	}

	resultRow := result.GetRows()[0]
	if resultRow.GetValue("name") != "Alice" {
		t.Errorf("Expected remaining row to be Alice, got %v", resultRow.GetValue("name"))
	}
}

func TestDataValidatorProcessor(t *testing.T) {
	processor := NewDataValidatorProcessor(nil)

	// 配置验证规则
	config := &ProcessorConfig{
		Name: "test_validator",
		Type: "data_validator",
		Parameters: map[string]interface{}{
			"rules": []interface{}{
				map[string]interface{}{
					"column":     "email",
					"required":   true,
					"type":       "string",
					"min_length": 5,
					"pattern":    `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`,
				},
				map[string]interface{}{
					"column":    "age",
					"required":  true,
					"type":      "int",
					"min_value": 0,
					"max_value": 150,
				},
			},
		},
	}

	err := processor.Configure(config)
	if err != nil {
		t.Fatalf("Failed to configure processor: %v", err)
	}

	tests := []struct {
		name      string
		row       DataRow
		expectErr bool
	}{
		{
			name: "valid data",
			row: NewSimpleDataRow(
				[]string{"email", "age"},
				[]interface{}{"user@example.com", 25},
			),
			expectErr: false,
		},
		{
			name: "invalid email",
			row: NewSimpleDataRow(
				[]string{"email", "age"},
				[]interface{}{"invalid-email", 25},
			),
			expectErr: true,
		},
		{
			name: "missing required field",
			row: NewSimpleDataRow(
				[]string{"age"},
				[]interface{}{25},
			),
			expectErr: true,
		},
		{
			name: "age out of range",
			row: NewSimpleDataRow(
				[]string{"email", "age"},
				[]interface{}{"user@example.com", 200},
			),
			expectErr: true,
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := NewSimpleDataBatch()
			batch.AddRow(tt.row)

			_, err := processor.Process(ctx, batch)
			if tt.expectErr && err == nil {
				t.Error("Expected validation error, got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("Expected no validation error, got %v", err)
			}
		})
	}
}

func TestSimplePipeline(t *testing.T) {
	pipeline := NewSimplePipeline(nil)

	// 添加列映射处理器
	mappingProcessor := NewColumnMappingProcessor(nil)
	mappingConfig := &ProcessorConfig{
		Name: "mapping",
		Type: "column_mapping",
		Parameters: map[string]interface{}{
			"mapping": map[string]interface{}{
				"old_name": "new_name",
			},
		},
	}
	mappingProcessor.Configure(mappingConfig)
	pipeline.AddProcessor(mappingProcessor)

	// 添加过滤处理器
	filterProcessor := NewRowFilterProcessor(nil)
	filterConfig := &ProcessorConfig{
		Name: "filter",
		Type: "row_filter",
		Parameters: map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"column":   "age",
					"operator": "gt",
					"value":    18,
				},
			},
		},
	}
	filterProcessor.Configure(filterConfig)
	pipeline.AddProcessor(filterProcessor)

	// 创建测试数据
	batch := NewSimpleDataBatch()
	batch.AddRow(NewSimpleDataRow(
		[]string{"old_name", "age"},
		[]interface{}{"Alice", 25},
	))
	batch.AddRow(NewSimpleDataRow(
		[]string{"old_name", "age"},
		[]interface{}{"Bob", 16},
	))

	// 处理数据
	ctx := context.Background()
	result, err := pipeline.Process(ctx, batch)
	if err != nil {
		t.Fatalf("Failed to process through pipeline: %v", err)
	}

	// 应该只有一行通过（Alice，年龄>18）
	if result.Size() != 1 {
		t.Errorf("Expected 1 row in result, got %d", result.Size())
	}

	resultRow := result.GetRows()[0]

	// 检查列映射是否生效
	if resultRow.GetValue("new_name") != "Alice" {
		t.Errorf("Expected new_name to be 'Alice', got %v", resultRow.GetValue("new_name"))
	}

	// 原列名应该不存在
	if resultRow.GetValue("old_name") != nil {
		t.Error("old_name should not exist in result")
	}
}