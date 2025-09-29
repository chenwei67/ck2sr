package protocol

import (
	"context"
	"errors"
	"testing"
)

// MockDataReader 实现 DataReader 接口用于测试
type MockDataReader struct {
	records []interface{}
	index   int
	closed  bool
}

func NewMockDataReader(records []interface{}) *MockDataReader {
	return &MockDataReader{
		records: records,
		index:   -1,
	}
}

func (m *MockDataReader) Next() bool {
	if m.closed {
		return false
	}
	if m.index+1 < len(m.records) {
		m.index++
		return true
	}
	return false
}

func (m *MockDataReader) GetRecord() (interface{}, error) {
	if m.closed {
		return nil, errors.New("reader is closed")
	}
	if m.index < 0 || m.index >= len(m.records) {
		return nil, errors.New("no current record")
	}
	return m.records[m.index], nil
}

func (m *MockDataReader) Close() error {
	m.closed = true
	return nil
}

// MockOffsetReader 实现 OffsetReader 接口用于测试
type MockOffsetReader struct {
	*MockDataReader
	offset int64
}

func NewMockOffsetReader(records []interface{}) *MockOffsetReader {
	return &MockOffsetReader{
		MockDataReader: NewMockDataReader(records),
		offset:         0,
	}
}

func (m *MockOffsetReader) SetOffset(offset int64) error {
	if offset < 0 || offset >= int64(len(m.records)) {
		return errors.New("offset out of range")
	}
	m.offset = offset
	m.index = int(offset) - 1
	return nil
}

func (m *MockOffsetReader) GetOffset() int64 {
	return m.offset
}

// MockBatchReader 实现 BatchReader 接口用于测试
type MockBatchReader struct {
	*MockDataReader
}

func NewMockBatchReader(records []interface{}) *MockBatchReader {
	return &MockBatchReader{
		MockDataReader: NewMockDataReader(records),
	}
}

func (m *MockBatchReader) GetBatch(size int) ([]interface{}, error) {
	if m.closed {
		return nil, errors.New("reader is closed")
	}

	var batch []interface{}
	for i := 0; i < size && m.Next(); i++ {
		record, err := m.GetRecord()
		if err != nil {
			return nil, err
		}
		batch = append(batch, record)
	}
	return batch, nil
}

// MockCountableReader 实现 CountableReader 接口用于测试
type MockCountableReader struct {
	*MockDataReader
}

func NewMockCountableReader(records []interface{}) *MockCountableReader {
	return &MockCountableReader{
		MockDataReader: NewMockDataReader(records),
	}
}

func (m *MockCountableReader) Count(ctx context.Context) (int64, error) {
	if m.closed {
		return 0, errors.New("reader is closed")
	}
	return int64(len(m.records)), nil
}

// TestDataReader 测试基本的 DataReader 接口
func TestDataReader(t *testing.T) {
	testData := []interface{}{
		map[string]interface{}{"id": 1, "name": "Alice"},
		map[string]interface{}{"id": 2, "name": "Bob"},
		map[string]interface{}{"id": 3, "name": "Charlie"},
	}

	reader := NewMockDataReader(testData)

	// 测试读取所有记录
	recordCount := 0
	for reader.Next() {
		record, err := reader.GetRecord()
		if err != nil {
			t.Fatalf("GetRecord() failed: %v", err)
		}

		expectedRecord := testData[recordCount]
		if !compareRecords(record, expectedRecord) {
			t.Errorf("Record %d mismatch. Got: %v, Expected: %v", recordCount, record, expectedRecord)
		}
		recordCount++
	}

	if recordCount != len(testData) {
		t.Errorf("Expected %d records, got %d", len(testData), recordCount)
	}

	// 测试关闭后的行为
	err := reader.Close()
	if err != nil {
		t.Fatalf("Close() failed: %v", err)
	}

	if reader.Next() {
		t.Error("Next() should return false after Close()")
	}

	_, err = reader.GetRecord()
	if err == nil {
		t.Error("GetRecord() should fail after Close()")
	}
}

// TestOffsetReader 测试 OffsetReader 接口
func TestOffsetReader(t *testing.T) {
	testData := []interface{}{
		map[string]interface{}{"id": 1, "name": "Alice"},
		map[string]interface{}{"id": 2, "name": "Bob"},
		map[string]interface{}{"id": 3, "name": "Charlie"},
		map[string]interface{}{"id": 4, "name": "David"},
	}

	reader := NewMockOffsetReader(testData)

	// 测试设置偏移量
	err := reader.SetOffset(2)
	if err != nil {
		t.Fatalf("SetOffset(2) failed: %v", err)
	}

	if reader.GetOffset() != 2 {
		t.Errorf("Expected offset 2, got %d", reader.GetOffset())
	}

	// 从偏移量开始读取
	if !reader.Next() {
		t.Error("Next() should return true after setting offset")
	}

	record, err := reader.GetRecord()
	if err != nil {
		t.Fatalf("GetRecord() failed: %v", err)
	}

	expectedRecord := testData[2]
	if !compareRecords(record, expectedRecord) {
		t.Errorf("Record mismatch after offset. Got: %v, Expected: %v", record, expectedRecord)
	}

	// 测试无效偏移量
	err = reader.SetOffset(-1)
	if err == nil {
		t.Error("SetOffset(-1) should fail")
	}

	err = reader.SetOffset(int64(len(testData)))
	if err == nil {
		t.Error("SetOffset() with out-of-range value should fail")
	}
}

// TestBatchReader 测试 BatchReader 接口
func TestBatchReader(t *testing.T) {
	testData := []interface{}{
		map[string]interface{}{"id": 1, "name": "Alice"},
		map[string]interface{}{"id": 2, "name": "Bob"},
		map[string]interface{}{"id": 3, "name": "Charlie"},
		map[string]interface{}{"id": 4, "name": "David"},
		map[string]interface{}{"id": 5, "name": "Eve"},
	}

	reader := NewMockBatchReader(testData)

	// 测试批量读取
	batchSize := 2
	batch, err := reader.GetBatch(batchSize)
	if err != nil {
		t.Fatalf("GetBatch(%d) failed: %v", batchSize, err)
	}

	if len(batch) != batchSize {
		t.Errorf("Expected batch size %d, got %d", batchSize, len(batch))
	}

	// 验证批次内容
	for i, record := range batch {
		expectedRecord := testData[i]
		if !compareRecords(record, expectedRecord) {
			t.Errorf("Batch record %d mismatch. Got: %v, Expected: %v", i, record, expectedRecord)
		}
	}

	// 测试读取剩余记录（不足一个完整批次）
	remainingSize := len(testData) - batchSize
	batch, err = reader.GetBatch(10) // 请求比剩余记录更多的数量
	if err != nil {
		t.Fatalf("GetBatch() for remaining records failed: %v", err)
	}

	if len(batch) != remainingSize {
		t.Errorf("Expected remaining batch size %d, got %d", remainingSize, len(batch))
	}
}

// TestCountableReader 测试 CountableReader 接口
func TestCountableReader(t *testing.T) {
	testData := []interface{}{
		map[string]interface{}{"id": 1, "name": "Alice"},
		map[string]interface{}{"id": 2, "name": "Bob"},
		map[string]interface{}{"id": 3, "name": "Charlie"},
	}

	reader := NewMockCountableReader(testData)

	// 测试计数功能
	ctx := context.Background()
	count, err := reader.Count(ctx)
	if err != nil {
		t.Fatalf("Count() failed: %v", err)
	}

	expectedCount := int64(len(testData))
	if count != expectedCount {
		t.Errorf("Expected count %d, got %d", expectedCount, count)
	}

	// 关闭后测试计数
	reader.Close()
	_, err = reader.Count(ctx)
	if err == nil {
		t.Error("Count() should fail after Close()")
	}
}

// compareRecords 比较两个记录是否相等
func compareRecords(r1, r2 interface{}) bool {
	m1, ok1 := r1.(map[string]interface{})
	m2, ok2 := r2.(map[string]interface{})

	if !ok1 || !ok2 {
		return r1 == r2
	}

	if len(m1) != len(m2) {
		return false
	}

	for k, v1 := range m1 {
		v2, exists := m2[k]
		if !exists || v1 != v2 {
			return false
		}
	}

	return true
}

// Benchmark tests
func BenchmarkDataReader(b *testing.B) {
	testData := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		testData[i] = map[string]interface{}{
			"id":   i,
			"name": "User" + string(rune(i)),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := NewMockDataReader(testData)
		count := 0
		for reader.Next() {
			_, err := reader.GetRecord()
			if err != nil {
				b.Fatal(err)
			}
			count++
		}
		reader.Close()
	}
}