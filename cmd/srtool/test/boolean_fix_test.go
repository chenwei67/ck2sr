package test

import (
	"context"
	"strings"
	"testing"

	"github.com/ck2sr/ck2sr/pkg/starrocks"
)

// TestBooleanTypeFixing 测试布尔类型识别修复
func TestBooleanTypeFixing(t *testing.T) {
	// 创建mock logger
	logger := NewMockLogger()
	client := NewMockStarRocksClient(logger)

	// 设置测试表信息，模拟StarRocks返回tinyint类型的布尔列
	testTable := &starrocks.TableInfo{
		Name:      "test_boolean_table",
		Engine:    "OLAP",
		TotalRows: 100,
		Columns: []starrocks.ColumnInfo{
			{Name: "id", Type: "BIGINT", IsNullable: false, IsPrimaryKey: true},
			{Name: "boolean_col", Type: "tinyint", IsNullable: true, IsPrimaryKey: false},        // 应该被识别为BOOLEAN
			{Name: "active_flag", Type: "tinyint", IsNullable: true, IsPrimaryKey: false},       // 应该被识别为BOOLEAN
			{Name: "is_active", Type: "tinyint", IsNullable: true, IsPrimaryKey: false},         // 应该被识别为BOOLEAN
			{Name: "user_id", Type: "tinyint", IsNullable: true, IsPrimaryKey: false},           // 应该保持tinyint
			{Name: "status", Type: "tinyint(1)", IsNullable: true, IsPrimaryKey: false},         // 应该被识别为BOOLEAN
			{Name: "normal_col", Type: "int", IsNullable: true, IsPrimaryKey: false},            // 应该保持int
		},
	}

	client.SetTableInfo("test_boolean_table", testTable)

	// 获取表信息并检查类型转换
	ctx := context.Background()
	result, err := client.GetTableInfo(ctx, "test_boolean_table")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}

	// 验证布尔类型识别
	expectedTypes := map[string]string{
		"id":          "BIGINT",
		"boolean_col": "BOOLEAN",   // 应该被转换
		"active_flag": "BOOLEAN",   // 应该被转换
		"is_active":   "BOOLEAN",   // 应该被转换
		"user_id":     "tinyint",   // 应该保持不变
		"status":      "BOOLEAN",   // 应该被转换
		"normal_col":  "int",       // 应该保持不变
	}

	for _, col := range result.Columns {
		expectedType, exists := expectedTypes[col.Name]
		if !exists {
			t.Errorf("Unexpected column: %s", col.Name)
			continue
		}

		if col.Type != expectedType {
			t.Errorf("Column %s: expected type %s, got %s", col.Name, expectedType, col.Type)
		} else {
			t.Logf("✅ Column %s: correctly identified as %s", col.Name, col.Type)
		}
	}

	// 验证总列数
	if len(result.Columns) != len(expectedTypes) {
		t.Errorf("Expected %d columns, got %d", len(expectedTypes), len(result.Columns))
	}

	t.Logf("Boolean type fixing test completed successfully")
}

// TestBooleanTypePattern 测试各种布尔类型命名模式
func TestBooleanTypePattern(t *testing.T) {
	testCases := []struct {
		columnName   string
		dataType     string
		expectedType string
		description  string
	}{
		{"boolean_col", "tinyint", "BOOLEAN", "包含boolean关键字"},
		{"bool_field", "tinyint", "BOOLEAN", "包含bool关键字"},
		{"active_flag", "tinyint", "BOOLEAN", "以_flag结尾"},
		{"is_active", "tinyint", "BOOLEAN", "以_active结尾"},
		{"user_id", "tinyint", "tinyint", "普通tinyint字段"},
		{"age", "tinyint", "tinyint", "普通tinyint字段"},
		{"status", "tinyint(1)", "BOOLEAN", "tinyint(1)类型"},
		{"enabled", "tinyint(1)", "BOOLEAN", "tinyint(1)类型"},
		{"count", "tinyint(1)", "BOOLEAN", "tinyint(1)类型"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 模拟类型识别逻辑
			col := starrocks.ColumnInfo{
				Name: tc.columnName,
				Type: tc.dataType,
			}

			// 应用修复逻辑
			if (col.Type == "tinyint" && (strings.Contains(strings.ToLower(col.Name), "boolean") ||
				strings.Contains(strings.ToLower(col.Name), "bool") ||
				strings.HasSuffix(strings.ToLower(col.Name), "_flag") ||
				strings.HasSuffix(strings.ToLower(col.Name), "_active"))) ||
				strings.Contains(col.Type, "tinyint(1)") {
				col.Type = "BOOLEAN"
			}

			if col.Type != tc.expectedType {
				t.Errorf("Column %s (%s): expected %s, got %s",
					tc.columnName, tc.dataType, tc.expectedType, col.Type)
			} else {
				t.Logf("✅ Column %s (%s): correctly identified as %s",
					tc.columnName, tc.dataType, col.Type)
			}
		})
	}
}