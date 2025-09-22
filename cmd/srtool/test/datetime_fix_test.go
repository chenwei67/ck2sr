package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/ck2sr/ck2sr/pkg/starrocks"
)

// TestDateTimeTypeMapping 测试日期时间类型映射修复
func TestDateTimeTypeMapping(t *testing.T) {
	// 创建mock logger
	logger := NewMockLogger()
	client := NewMockStarRocksClient(logger)

	// 设置测试表信息，模拟StarRocks返回的不同日期时间类型
	testTable := &starrocks.TableInfo{
		Name:      "test_datetime_table",
		Engine:    "OLAP",
		TotalRows: 100,
		Columns: []starrocks.ColumnInfo{
			{Name: "id", Type: "BIGINT", IsNullable: false, IsPrimaryKey: true},
			{Name: "date_col", Type: "date", IsNullable: true, IsPrimaryKey: false},           // 应该映射为Date32
			{Name: "datetime_col", Type: "datetime", IsNullable: true, IsPrimaryKey: false},   // 应该映射为Timestamp
			{Name: "timestamp_col", Type: "datetime", IsNullable: true, IsPrimaryKey: false},  // 应该映射为Timestamp (用户报告的情况)
		},
	}

	client.SetTableInfo("test_datetime_table", testTable)

	// 获取表信息并检查Schema转换
	ctx := context.Background()
	result, err := client.GetTableInfo(ctx, "test_datetime_table")
	if err != nil {
		t.Fatalf("Failed to get table info: %v", err)
	}

	// 测试类型转换逻辑
	for _, col := range result.Columns {
		arrowType, err := convertStarRocksTypeToArrow(col.Type)
		if err != nil {
			t.Errorf("Failed to convert type %s: %v", col.Type, err)
			continue
		}

		switch col.Name {
		case "id":
			if arrowType.ID() != arrow.INT64 {
				t.Errorf("Column %s: expected Int64, got %s", col.Name, arrowType)
			}
		case "date_col":
			if arrowType.ID() != arrow.DATE32 {
				t.Errorf("Column %s: expected Date32, got %s", col.Name, arrowType)
			} else {
				t.Logf("✅ Column %s (%s): correctly mapped to Date32", col.Name, col.Type)
			}
		case "datetime_col", "timestamp_col":
			if arrowType.ID() != arrow.TIMESTAMP {
				t.Errorf("Column %s: expected Timestamp, got %s", col.Name, arrowType)
			} else {
				t.Logf("✅ Column %s (%s): correctly mapped to Timestamp", col.Name, col.Type)
			}
		}
	}

	t.Logf("DateTime type mapping test completed successfully")
}

// convertStarRocksTypeToArrow 复制核心转换逻辑用于测试
func convertStarRocksTypeToArrow(starRocksType string) (arrow.DataType, error) {
	upperType := strings.ToUpper(starRocksType)

	switch {
	// 优先检查BOOLEAN类型（包括被转换过的）
	case strings.Contains(upperType, "BOOLEAN") || strings.Contains(upperType, "BOOL"):
		return arrow.FixedWidthTypes.Boolean, nil
	case strings.Contains(upperType, "BIGINT"):
		return arrow.PrimitiveTypes.Int64, nil
	case strings.Contains(upperType, "SMALLINT"):
		return arrow.PrimitiveTypes.Int16, nil
	case strings.Contains(upperType, "TINYINT"):
		return arrow.PrimitiveTypes.Int8, nil
	case strings.Contains(upperType, "INT") || strings.Contains(upperType, "INTEGER"):
		return arrow.PrimitiveTypes.Int32, nil
	case strings.Contains(upperType, "FLOAT"):
		return arrow.PrimitiveTypes.Float32, nil
	case strings.Contains(upperType, "DOUBLE"):
		return arrow.PrimitiveTypes.Float64, nil
	// 先检查DATETIME和TIMESTAMP，再检查DATE，避免DATE匹配到DATETIME
	case strings.Contains(upperType, "DATETIME") || strings.Contains(upperType, "TIMESTAMP"):
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case strings.Contains(upperType, "DATE"):
		return arrow.FixedWidthTypes.Date32, nil
	case strings.Contains(upperType, "VARCHAR") || strings.Contains(upperType, "CHAR") || strings.Contains(upperType, "TEXT"):
		return arrow.BinaryTypes.String, nil
	default:
		// 默认使用字符串类型
		return arrow.BinaryTypes.String, nil
	}
}

// TestTimeDataConversion 测试时间数据格式转换
func TestTimeDataConversion(t *testing.T) {
	testCases := []struct {
		fieldName    string
		fieldType    arrow.DataType
		inputValue   interface{}
		shouldSucceed bool
		description  string
	}{
		{"date_col", arrow.FixedWidthTypes.Date32, "2025-09-22", true, "日期字符串转Date32"},
		{"date_col", arrow.FixedWidthTypes.Date32, time.Now(), true, "Time对象转Date32"},
		{"datetime_col", arrow.FixedWidthTypes.Timestamp_us, "2025-09-22 10:07:54", true, "日期时间字符串转Timestamp"},
		{"timestamp_col", arrow.FixedWidthTypes.Timestamp_us, "2025-09-22 10:07:54", true, "时间戳字符串转Timestamp"},
		{"timestamp_col", arrow.FixedWidthTypes.Timestamp_us, time.Now(), true, "Time对象转Timestamp"},
		{"date_col", arrow.FixedWidthTypes.Date32, "2025-09-22 10:07:54", false, "错误：时间字符串转Date32（应该失败）"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 测试类型转换函数
			switch tc.fieldType.ID() {
			case arrow.DATE32:
				_, err := convertToDate32(tc.inputValue)
				if tc.shouldSucceed && err != nil {
					t.Errorf("Expected success but got error: %v", err)
				} else if !tc.shouldSucceed && err == nil {
					t.Errorf("Expected error but conversion succeeded")
				} else if tc.shouldSucceed {
					t.Logf("✅ %s: conversion succeeded", tc.description)
				} else {
					t.Logf("✅ %s: correctly failed with error: %v", tc.description, err)
				}
			case arrow.TIMESTAMP:
				_, err := convertToTimestamp(tc.inputValue)
				if tc.shouldSucceed && err != nil {
					t.Errorf("Expected success but got error: %v", err)
				} else if !tc.shouldSucceed && err == nil {
					t.Errorf("Expected error but conversion succeeded")
				} else if tc.shouldSucceed {
					t.Logf("✅ %s: conversion succeeded", tc.description)
				} else {
					t.Logf("✅ %s: correctly failed with error: %v", tc.description, err)
				}
			}
		})
	}
}

// 复制转换函数用于测试
func convertToDate32(value interface{}) (arrow.Date32, error) {
	switch v := value.(type) {
	case arrow.Date32:
		return v, nil
	case string:
		// 解析日期字符串格式 YYYY-MM-DD
		if t, err := time.Parse("2006-01-02", v); err == nil {
			daysSinceEpoch := arrow.Date32(t.Unix() / 86400)
			return daysSinceEpoch, nil
		}
		return 0, fmt.Errorf("invalid date format: %s", v)
	case time.Time:
		daysSinceEpoch := arrow.Date32(v.Unix() / 86400)
		return daysSinceEpoch, nil
	}
	return 0, fmt.Errorf("cannot convert %T to arrow.Date32", value)
}

func convertToTimestamp(value interface{}) (arrow.Timestamp, error) {
	switch v := value.(type) {
	case arrow.Timestamp:
		return v, nil
	case string:
		// 解析日期时间字符串格式 YYYY-MM-DD HH:MM:SS
		if t, err := time.Parse("2006-01-02 15:04:05", v); err == nil {
			return arrow.Timestamp(t.UnixMicro()), nil
		}
		return 0, fmt.Errorf("invalid datetime format: %s", v)
	case time.Time:
		return arrow.Timestamp(v.UnixMicro()), nil
	case int64:
		// Unix微秒时间戳
		return arrow.Timestamp(v), nil
	}
	return 0, fmt.Errorf("cannot convert %T to arrow.Timestamp", value)
}