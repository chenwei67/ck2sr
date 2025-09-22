package starrocks

import (
	"testing"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/sirupsen/logrus"
)

func TestBuildCreateTableSQL(t *testing.T) {
	// 创建测试客户端
	client := &Client{
		logger: logrus.New(),
	}

	// 创建测试schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	}, nil)

	// 测试生成SQL
	sql, err := client.buildCreateTableSQL("test_table", schema)
	if err != nil {
		t.Fatalf("buildCreateTableSQL failed: %v", err)
	}

	t.Logf("Generated SQL:\n%s", sql)

	// 基本验证
	if sql == "" {
		t.Error("Generated SQL is empty")
	}

	// 检查SQL中不应该有语法错误的模式
	if contains(sql, ",\n  PRIMARY KEY") {
		t.Error("SQL contains problematic comma before PRIMARY KEY")
	}

	// 检查SQL包含必要元素
	if !contains(sql, "CREATE TABLE IF NOT EXISTS `test_table`") {
		t.Error("SQL missing table creation")
	}

	if !contains(sql, "`id` BIGINT NOT NULL") {
		t.Error("SQL missing id column definition")
	}

	if !contains(sql, "PRIMARY KEY (`id`)") {
		t.Error("SQL missing primary key definition")
	}

	if !contains(sql, "ENGINE=OLAP") {
		t.Error("SQL missing engine definition")
	}
}

func TestBuildCreateTableSQLWithoutPrimaryKey(t *testing.T) {
	// 创建测试客户端
	client := &Client{
		logger: logrus.New(),
	}

	// 创建没有主键的测试schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "description", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// 测试生成SQL
	sql, err := client.buildCreateTableSQL("test_table2", schema)
	if err != nil {
		t.Fatalf("buildCreateTableSQL failed: %v", err)
	}

	t.Logf("Generated SQL (no PK):\n%s", sql)

	// 验证使用DUPLICATE KEY模型
	if !contains(sql, "DUPLICATE KEY(`name`)") {
		t.Error("SQL missing DUPLICATE KEY definition")
	}

	if !contains(sql, "DISTRIBUTED BY HASH(`name`)") {
		t.Error("SQL missing distribution definition")
	}
}

func TestBuildCreateTableSQLEmptySchema(t *testing.T) {
	client := &Client{
		logger: logrus.New(),
	}

	// 空schema测试
	schema := arrow.NewSchema([]arrow.Field{}, nil)

	_, err := client.buildCreateTableSQL("empty_table", schema)
	if err == nil {
		t.Error("Expected error for empty schema")
	}

	if err.Error() != "schema has no fields" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

// 辅助函数
func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 &&
		   len(s) >= len(substr) &&
		   findSubstring(s, substr)
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}