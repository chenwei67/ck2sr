package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCalculateRecordSize(t *testing.T) {
	tests := []struct {
		name        string
		record      interface{}
		wantErr     bool
		description string
	}{
		{
			name:        "nil record",
			record:      nil,
			wantErr:     false,
			description: "nil记录应该返回0字节",
		},
		{
			name: "simple map",
			record: map[string]interface{}{
				"id":   1,
				"name": "test",
			},
			wantErr:     false,
			description: "简单map应该成功计算",
		},
		{
			name: "complex map",
			record: map[string]interface{}{
				"id":   12345,
				"name": "test user",
				"age":  30,
				"tags": []string{"tag1", "tag2", "tag3"},
				"metadata": map[string]interface{}{
					"created_at": "2024-01-01",
					"updated_at": "2024-01-02",
				},
			},
			wantErr:     false,
			description: "复杂嵌套结构应该成功计算",
		},
		{
			name: "string record",
			record: "simple string",
			wantErr: false,
			description: "字符串记录应该成功计算",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			size, err := CalculateRecordSize(tt.record)

			if tt.wantErr {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
				if tt.record != nil {
					assert.Greater(t, size, int64(0), "非nil记录应该有正数大小")
				} else {
					assert.Equal(t, int64(0), size, "nil记录应该返回0")
				}
			}
		})
	}
}

func TestCalculateBatchSize(t *testing.T) {
	tests := []struct {
		name        string
		records     []interface{}
		wantErr     bool
		description string
	}{
		{
			name:        "empty batch",
			records:     []interface{}{},
			wantErr:     false,
			description: "空批次应该返回0",
		},
		{
			name: "single record",
			records: []interface{}{
				map[string]interface{}{"id": 1, "name": "test"},
			},
			wantErr:     false,
			description: "单条记录批次应该成功计算",
		},
		{
			name: "multiple records",
			records: []interface{}{
				map[string]interface{}{"id": 1, "name": "test1"},
				map[string]interface{}{"id": 2, "name": "test2"},
				map[string]interface{}{"id": 3, "name": "test3"},
			},
			wantErr:     false,
			description: "多条记录批次应该成功计算",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			totalSize, err := CalculateBatchSize(tt.records)

			if tt.wantErr {
				assert.Error(t, err, tt.description)
			} else {
				assert.NoError(t, err, tt.description)
				if len(tt.records) > 0 {
					assert.Greater(t, totalSize, int64(0), "非空批次应该有正数大小")
				} else {
					assert.Equal(t, int64(0), totalSize, "空批次应该返回0")
				}
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		name     string
		bytes    int64
		expected string
	}{
		{"zero bytes", 0, "0 B"},
		{"bytes", 500, "500 B"},
		{"kilobytes", 1024, "1.0 KB"},
		{"megabytes", 1048576, "1.0 MB"},
		{"gigabytes", 1073741824, "1.00 GB"},
		{"mixed KB", 5120, "5.0 KB"},
		{"mixed MB", 5242880, "5.0 MB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatBytes(tt.bytes)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func BenchmarkCalculateRecordSize(b *testing.B) {
	record := map[string]interface{}{
		"id":   12345,
		"name": "benchmark test",
		"age":  30,
		"tags": []string{"tag1", "tag2", "tag3"},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CalculateRecordSize(record)
	}
}

func BenchmarkCalculateBatchSize(b *testing.B) {
	records := make([]interface{}, 100)
	for i := 0; i < 100; i++ {
		records[i] = map[string]interface{}{
			"id":   i,
			"name": "benchmark test",
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CalculateBatchSize(records)
	}
}
