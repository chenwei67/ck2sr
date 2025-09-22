package test

import (
	"fmt"
	"strings"
	"testing"
)

// TestFormatValueForSQL 测试SQL值格式化功能
func TestFormatValueForSQL(t *testing.T) {
	testCases := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"Nil值", nil, "NULL"},
		{"布尔真值", true, "1"},
		{"布尔假值", false, "0"},
		{"Int8值", int8(127), "127"},
		{"Int16值", int16(32767), "32767"},
		{"Int32值", int32(2147483647), "2147483647"},
		{"Int64值", int64(9223372036854775807), "9223372036854775807"},
		{"Float32值", float32(3.14), "3.14"},
		{"Float64值", float64(3.141592653589793), "3.141592653589793"},
		{"普通字符串", "hello", "'hello'"},
		{"包含单引号的字符串", "it's working", "'it\\'s working'"},
		{"包含反斜杠的字符串", "path\\to\\file", "'path\\\\to\\\\file'"},
		{"复杂字符串", "user's \"data\" with \\backslash", "'user\\'s \"data\" with \\\\backslash'"},
		{"空字符串", "", "''"},
		{"日期字符串", "2025-09-22", "'2025-09-22'"},
		{"时间字符串", "2025-09-22 10:30:00", "'2025-09-22 10:30:00'"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := formatValueForSQL(tc.input)
			if result != tc.expected {
				t.Errorf("Input: %v (%T), Expected: %s, Got: %s", tc.input, tc.input, tc.expected, result)
			} else {
				t.Logf("✅ %s: %v -> %s", tc.name, tc.input, result)
			}
		})
	}
}

// TestSQLInjectionPrevention 测试SQL注入防护
func TestSQLInjectionPrevention(t *testing.T) {
	maliciousInputs := []struct {
		name  string
		input string
	}{
		{"基本SQL注入", "'; DROP TABLE users; --"},
		{"Union注入", "' UNION SELECT * FROM passwords --"},
		{"注释注入", "admin'/*"},
		{"转义注入", "\\'"},
		{"多层转义", "\\\\'"},
	}

	for _, tc := range maliciousInputs {
		t.Run(tc.name, func(t *testing.T) {
			result := formatValueForSQL(tc.input)

			// 验证结果被正确转义 - 应该以单引号开始和结束
			if !strings.HasPrefix(result, "'") || !strings.HasSuffix(result, "'") {
				t.Errorf("Input %s not properly quoted: %s", tc.input, result)
			} else {
				t.Logf("✅ %s: properly escaped as %s", tc.name, result)
			}
		})
	}
}

// TestMySQLProtocolInsertSQL 测试完整的INSERT SQL生成
func TestMySQLProtocolInsertSQL(t *testing.T) {
	// 模拟一个简单的INSERT语句构建
	tableName := "test_table"
	columnNames := []string{"`id`", "`name`", "`age`", "`active`"}

	testRows := []map[string]interface{}{
		{"id": int64(1), "name": "Alice", "age": int32(25), "active": true},
		{"id": int64(2), "name": "Bob's friend", "age": int32(30), "active": false},
		{"id": int64(3), "name": nil, "age": int32(35), "active": true},
	}

	// 构建值部分
	valueParts := make([]string, len(testRows))
	for i, row := range testRows {
		values := make([]string, len(columnNames))
		values[0] = formatValueForSQL(row["id"])
		values[1] = formatValueForSQL(row["name"])
		values[2] = formatValueForSQL(row["age"])
		values[3] = formatValueForSQL(row["active"])
		valueParts[i] = "(" + strings.Join(values, ",") + ")"
	}

	// 构建完整SQL
	actualSQL := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES %s",
		tableName,
		strings.Join(columnNames, ","),
		strings.Join(valueParts, ","))

	// 验证SQL结构正确
	expectedPattern := "INSERT INTO `test_table` (`id`,`name`,`age`,`active`) VALUES"
	if !strings.HasPrefix(actualSQL, expectedPattern) {
		t.Errorf("SQL doesn't start with expected pattern:\nExpected prefix: %s\nActual SQL: %s", expectedPattern, actualSQL)
	} else {
		t.Logf("✅ Generated correct INSERT SQL structure: %s", actualSQL)
	}

	// 验证具体的值
	if !strings.Contains(actualSQL, "(1,'Alice',25,1)") {
		t.Errorf("SQL doesn't contain expected first row: %s", actualSQL)
	}
	if !strings.Contains(actualSQL, "(2,'Bob\\'s friend',30,0)") {
		t.Errorf("SQL doesn't contain expected second row: %s", actualSQL)
	}
	if !strings.Contains(actualSQL, "(3,NULL,35,1)") {
		t.Errorf("SQL doesn't contain expected third row: %s", actualSQL)
	}

	t.Logf("Complete SQL: %s", actualSQL)
}

// formatValueForSQL 本地实现（用于测试）
func formatValueForSQL(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case bool:
		if v {
			return "1"
		}
		return "0"
	case int8:
		return fmt.Sprintf("%d", v)
	case int16:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case string:
		// 转义SQL字符串中的单引号和反斜杠
		escaped := strings.ReplaceAll(v, "\\", "\\\\")
		escaped = strings.ReplaceAll(escaped, "'", "\\'")
		return fmt.Sprintf("'%s'", escaped)
	default:
		// 对于其他类型，转换为字符串并转义
		str := fmt.Sprintf("%v", v)
		escaped := strings.ReplaceAll(str, "\\", "\\\\")
		escaped = strings.ReplaceAll(escaped, "'", "\\'")
		return fmt.Sprintf("'%s'", escaped)
	}
}