package reader

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/sunkaimr/ck2sr/internal/config"
)

// TestClickHouseReaderWithStarRocksWriter 测试 ClickHouseReader 与 StarRocksHTTPWriter 的集成
// 这是集成测试，验证数据流从 ClickHouse Reader 到 StarRocks Writer 的完整路径
func TestClickHouseReaderWithStarRocksWriter(t *testing.T) {
	// 创建模拟的 ClickHouse 数据记录
	columns := []string{"id", "name", "age", "tags", "metadata", "created_at"}
	dataTypes := []string{"Int32", "String", "Int32", "Array(String)", "Map(String, String)", "DateTime"}
	metadata := NewColumnMetadata(columns, dataTypes)

	// 创建记录池
	pool := NewRecordPool(metadata)

	// 模拟从 ClickHouse 读取的多条记录
	records := make([]interface{}, 0, 3)

	// 记录1：普通数据
	record1 := pool.Get()
	record1.Values[0] = int32(1)
	record1.Values[1] = "Alice"
	record1.Values[2] = int32(30)
	record1.Values[3] = []interface{}{"tag1", "tag2", "tag3"}
	record1.Values[4] = map[string]interface{}{"city": "Beijing", "country": "China"}
	record1.Values[5] = "2024-10-24T12:30:45Z"
	records = append(records, record1)

	// 记录2：包含NULL值
	record2 := pool.Get()
	record2.Values[0] = int32(2)
	record2.Values[1] = "Bob"
	record2.Values[2] = nil // NULL值
	record2.Values[3] = []interface{}{}
	record2.Values[4] = map[string]interface{}{"status": "active"}
	record2.Values[5] = "2024-10-24T13:00:00Z"
	records = append(records, record2)

	// 记录3：包含RawValue（模拟延迟JSON解析）
	record3 := pool.Get()
	record3.Values[0] = int32(3)
	record3.Values[1] = "Charlie"
	record3.Values[2] = int32(25)
	record3.Values[3] = RawValue{
		IsJSON: true,
		Data:   []byte(`["tag4", "tag5"]`),
	}
	record3.Values[4] = map[string]interface{}{"role": "admin"}
	record3.Values[5] = "2024-10-24T14:00:00Z"
	records = append(records, record3)

	// 序列化为JSON（模拟StarRocksHTTPWriter的Write操作）
	jsonData, err := json.Marshal(records)
	assert.NoError(t, err)
	assert.NotEmpty(t, jsonData)

	// 验证JSON格式正确
	var parsedRecords []map[string]interface{}
	err = json.Unmarshal(jsonData, &parsedRecords)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(parsedRecords))

	// 验证记录1
	assert.Equal(t, float64(1), parsedRecords[0]["id"])
	assert.Equal(t, "Alice", parsedRecords[0]["name"])
	assert.Equal(t, float64(30), parsedRecords[0]["age"])

	// 验证tags数组
	tags, ok := parsedRecords[0]["tags"].([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 3, len(tags))

	// 验证metadata map
	metaMap, ok := parsedRecords[0]["metadata"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "Beijing", metaMap["city"])

	// 验证记录2（包含NULL）
	assert.Equal(t, float64(2), parsedRecords[1]["id"])
	assert.Equal(t, "Bob", parsedRecords[1]["name"])
	_, hasAge := parsedRecords[1]["age"] // NULL值不应该在JSON中
	assert.False(t, hasAge, "NULL values should be omitted in JSON")

	// 验证记录3（包含RawValue）
	assert.Equal(t, float64(3), parsedRecords[2]["id"])
	rawTags, ok := parsedRecords[2]["tags"].([]interface{})
	assert.True(t, ok, "RawValue should be parsed as array")
	assert.Equal(t, 2, len(rawTags))
	assert.Equal(t, "tag4", rawTags[0])
	assert.Equal(t, "tag5", rawTags[1])

	t.Logf("Integration test passed. JSON output (%d bytes):\n%s", len(jsonData), string(jsonData))
}

// TestClickHouseReaderRecordLifecycle 测试 Record 对象的完整生命周期
func TestClickHouseReaderRecordLifecycle(t *testing.T) {
	columns := []string{"id", "name", "score"}
	dataTypes := []string{"Int32", "String", "Float64"}
	metadata := NewColumnMetadata(columns, dataTypes)
	pool := NewRecordPool(metadata)

	// 1. 从池中获取
	record := pool.Get()
	assert.NotNil(t, record)
	assert.Equal(t, 3, len(record.Values))

	// 2. 填充数据
	record.Values[0] = int32(100)
	record.Values[1] = "TestUser"
	record.Values[2] = float64(98.5)

	// 3. 序列化为JSON
	jsonBytes, err := json.Marshal(record)
	assert.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(jsonBytes, &parsed)
	assert.NoError(t, err)
	assert.Equal(t, float64(100), parsed["id"])
	assert.Equal(t, "TestUser", parsed["name"])
	assert.Equal(t, 98.5, parsed["score"])

	// 4. 归还到池
	pool.Put(record)

	// 5. 再次获取（验证对象复用）
	record2 := pool.Get()
	assert.NotNil(t, record2)

	// 验证已重置
	for _, val := range record2.Values {
		assert.Nil(t, val)
	}
}

// TestClickHouseReaderPerformanceSimulation 性能模拟测试
// 模拟大批量数据的读取和序列化
func TestClickHouseReaderPerformanceSimulation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance simulation test in short mode")
	}

	columns := []string{"id", "name", "value", "tags"}
	dataTypes := []string{"Int64", "String", "Float64", "Array(String)"}
	metadata := NewColumnMetadata(columns, dataTypes)
	pool := NewRecordPool(metadata)

	const batchSize = 1000
	records := make([]interface{}, 0, batchSize)

	// 模拟读取1000条记录
	for i := 0; i < batchSize; i++ {
		record := pool.Get()
		record.Values[0] = int64(i)
		record.Values[1] = "user_" + string(rune(i%26+'a'))
		record.Values[2] = float64(i) * 1.5
		record.Values[3] = []interface{}{"tag1", "tag2"}
		records = append(records, record)
	}

	// 序列化为JSON
	jsonData, err := json.Marshal(records)
	assert.NoError(t, err)
	assert.NotEmpty(t, jsonData)

	t.Logf("Performance simulation: Serialized %d records to %d bytes JSON", batchSize, len(jsonData))

	// 验证JSON有效性
	var parsed []map[string]interface{}
	err = json.Unmarshal(jsonData, &parsed)
	assert.NoError(t, err)
	assert.Equal(t, batchSize, len(parsed))

	// 归还所有记录到池
	for _, r := range records {
		if rec, ok := r.(*Record); ok {
			pool.Put(rec)
		}
	}
}

// TestClickHouseReaderColumnFiltering 测试列过滤功能与Writer的集成
func TestClickHouseReaderColumnFiltering(t *testing.T) {
	logger := createTestLogger()
	reader := &ClickHouseReader{
		config: &config.DataSourceConfig{},
		logger: logger,
		excludeColumns: []string{"password", "internal_id"},
		fixedValues: map[string]interface{}{
			"tenant_id": "tenant_123",
		},
	}

	// 创建包含敏感列的记录
	columns := []string{"id", "username", "password", "email", "internal_id", "tenant_id"}
	dataTypes := []string{"Int32", "String", "String", "String", "String", "String"}
	metadata := NewColumnMetadata(columns, dataTypes)
	pool := NewRecordPool(metadata)

	record := pool.Get()
	record.Values[0] = int32(1)
	record.Values[1] = "alice"
	record.Values[2] = "secret123" // 应该被过滤
	record.Values[3] = "alice@example.com"
	record.Values[4] = "internal_999" // 应该被过滤
	record.Values[5] = nil // 待设置固定值

	// 模拟Reader的过滤逻辑
	for i, colName := range metadata.Names {
		if reader.isColumnExcluded(colName) {
			record.Values[i] = nil
		}
	}

	// 应用固定值
	for colName, fixedVal := range reader.fixedValues {
		_ = record.Set(colName, fixedVal)
	}

	// 序列化为JSON
	jsonBytes, err := json.Marshal(record)
	assert.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(jsonBytes, &parsed)
	assert.NoError(t, err)

	// 验证敏感列已过滤
	_, hasPassword := parsed["password"]
	assert.False(t, hasPassword, "password should be filtered")

	_, hasInternalId := parsed["internal_id"]
	assert.False(t, hasInternalId, "internal_id should be filtered")

	// 验证固定值已设置
	assert.Equal(t, "tenant_123", parsed["tenant_id"])

	// 验证普通列正常
	assert.Equal(t, float64(1), parsed["id"])
	assert.Equal(t, "alice", parsed["username"])
	assert.Equal(t, "alice@example.com", parsed["email"])
}

// TestClickHouseReaderComplexDataIntegration 测试复杂数据类型的完整集成
func TestClickHouseReaderComplexDataIntegration(t *testing.T) {
	columns := []string{"event_id", "user_data", "metrics", "config"}
	dataTypes := []string{"Int64", "Map(String, String)", "Array(Float64)", "JSON"}
	metadata := NewColumnMetadata(columns, dataTypes)
	pool := NewRecordPool(metadata)

	record := pool.Get()
	record.Values[0] = int64(12345)
	record.Values[1] = map[string]interface{}{
		"user_id":   "user_001",
		"user_name": "Alice",
		"region":    "Asia",
	}
	record.Values[2] = []interface{}{float64(1.5), float64(2.3), float64(3.7)}
	record.Values[3] = RawValue{
		IsJSON: true,
		Data:   []byte(`{"enabled": true, "timeout": 30}`),
	}

	// 序列化为JSON
	jsonBytes, err := json.Marshal(record)
	assert.NoError(t, err)

	var parsed map[string]interface{}
	err = json.Unmarshal(jsonBytes, &parsed)
	assert.NoError(t, err)

	// 验证Map类型
	userData, ok := parsed["user_data"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "user_001", userData["user_id"])

	// 验证Array类型
	metrics, ok := parsed["metrics"].([]interface{})
	assert.True(t, ok)
	assert.Equal(t, 3, len(metrics))

	// 验证JSON类型（RawValue）
	config, ok := parsed["config"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, true, config["enabled"])
	assert.Equal(t, float64(30), config["timeout"])

	t.Logf("Complex data integration test passed. JSON: %s", string(jsonBytes))
}

// BenchmarkClickHouseReaderToJSON 基准测试：Record序列化为JSON的性能
func BenchmarkClickHouseReaderToJSON(b *testing.B) {
	columns := []string{"id", "name", "score", "tags"}
	dataTypes := []string{"Int64", "String", "Float64", "Array(String)"}
	metadata := NewColumnMetadata(columns, dataTypes)
	pool := NewRecordPool(metadata)

	record := pool.Get()
	record.Values[0] = int64(123)
	record.Values[1] = "TestUser"
	record.Values[2] = float64(98.5)
	record.Values[3] = []interface{}{"tag1", "tag2", "tag3"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(record)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRecordPoolGetPut 基准测试：对象池性能
func BenchmarkRecordPoolGetPut(b *testing.B) {
	columns := []string{"id", "name", "score"}
	dataTypes := []string{"Int64", "String", "Float64"}
	metadata := NewColumnMetadata(columns, dataTypes)
	pool := NewRecordPool(metadata)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := pool.Get()
		record.Values[0] = int64(i)
		record.Values[1] = "user"
		record.Values[2] = float64(100.0)
		pool.Put(record)
	}
}

// MockStarRocksHTTPWriter 模拟StarRocks HTTP Writer用于集成测试
type MockStarRocksHTTPWriter struct {
	writtenData []byte
	callCount   int
}

func (w *MockStarRocksHTTPWriter) Write(ctx context.Context, records interface{}) error {
	data, err := json.Marshal(records)
	if err != nil {
		return err
	}
	w.writtenData = append(w.writtenData, data...)
	w.callCount++
	return nil
}

func (w *MockStarRocksHTTPWriter) Flush(ctx context.Context) error {
	return nil
}

func (w *MockStarRocksHTTPWriter) Close() error {
	return nil
}

func (w *MockStarRocksHTTPWriter) GetWrittenData() []byte {
	return w.writtenData
}

// TestClickHouseReaderToStarRocksWriterPipeline 测试完整的数据管道
func TestClickHouseReaderToStarRocksWriterPipeline(t *testing.T) {
	ctx := context.Background()

	// 创建模拟数据
	columns := []string{"id", "name", "amount"}
	dataTypes := []string{"Int32", "String", "Float64"}
	metadata := NewColumnMetadata(columns, dataTypes)
	pool := NewRecordPool(metadata)

	// 模拟从ClickHouse读取的数据
	sourceRecords := make([]interface{}, 0, 5)
	for i := 0; i < 5; i++ {
		record := pool.Get()
		record.Values[0] = int32(i + 1)
		record.Values[1] = "user_" + string(rune(i+'a'))
		record.Values[2] = float64(i+1) * 10.5
		sourceRecords = append(sourceRecords, record)
	}

	// 创建模拟Writer
	writer := &MockStarRocksHTTPWriter{
		writtenData: make([]byte, 0),
	}

	// 执行写入
	err := writer.Write(ctx, sourceRecords)
	assert.NoError(t, err)
	assert.Equal(t, 1, writer.callCount)

	// 验证写入的数据
	writtenData := writer.GetWrittenData()
	assert.NotEmpty(t, writtenData)

	var parsedRecords []map[string]interface{}
	err = json.Unmarshal(writtenData, &parsedRecords)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(parsedRecords))

	// 验证第一条记录
	assert.Equal(t, float64(1), parsedRecords[0]["id"])
	assert.Equal(t, "user_a", parsedRecords[0]["name"])
	assert.Equal(t, 10.5, parsedRecords[0]["amount"])

	t.Logf("Pipeline test passed. Wrote %d records (%d bytes)", len(parsedRecords), len(writtenData))
}

// TestClickHouseReaderMemoryEfficiency 测试内存效率
func TestClickHouseReaderMemoryEfficiency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory efficiency test in short mode")
	}

	columns := []string{"id", "data"}
	dataTypes := []string{"Int64", "String"}
	metadata := NewColumnMetadata(columns, dataTypes)
	pool := NewRecordPool(metadata)

	// 模拟大量记录的创建和回收
	const iterations = 10000
	var buffer bytes.Buffer

	for i := 0; i < iterations; i++ {
		record := pool.Get()
		record.Values[0] = int64(i)
		record.Values[1] = "data_value"

		// 序列化
		jsonBytes, err := json.Marshal(record)
		assert.NoError(t, err)
		buffer.Write(jsonBytes)

		// 归还到池
		pool.Put(record)
	}

	t.Logf("Memory efficiency test: Processed %d records, total JSON size: %d bytes", iterations, buffer.Len())
}
