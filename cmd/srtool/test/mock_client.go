package test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/ck2sr/ck2sr/pkg/starrocks"
)

// StarRocksConfig 简化的StarRocks配置（用于测试）
type StarRocksConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
}

// MockStarRocksClientImpl mock StarRocks客户端的具体实现
type MockStarRocksClientImpl struct {
	Config             *StarRocksConfig
	Logger             *MockLogger
	ConnectionError    error
	TableInfos         map[string]*starrocks.TableInfo
	TableNotFoundError error
	RowCounts          map[string]int64
	QueryResults       map[string]*flight.FlightInfo
	QueryError         error
	DoGetStreams       map[string]flight.FlightService_DoGetClient
	DoGetError         error
	DataWriters        map[string]*MockArrowDataWriter
	StreamWriters      map[string]*MockArrowStreamWriter
	CreateTableError   error
	CreatedTables      []string
	CloseCalled        bool
}

// NewMockStarRocksClient 创建mock StarRocks客户端
func NewMockStarRocksClient(logger *MockLogger) *MockStarRocksClientImpl {
	return &MockStarRocksClientImpl{
		Logger:        logger,
		TableInfos:    make(map[string]*starrocks.TableInfo),
		RowCounts:     make(map[string]int64),
		QueryResults:  make(map[string]*flight.FlightInfo),
		DoGetStreams:  make(map[string]flight.FlightService_DoGetClient),
		DataWriters:   make(map[string]*MockArrowDataWriter),
		StreamWriters: make(map[string]*MockArrowStreamWriter),
		CreatedTables: make([]string, 0),
	}
}

// TestConnection 模拟测试连接
func (m *MockStarRocksClientImpl) TestConnection(ctx context.Context) error {
	if m.ConnectionError != nil {
		m.Logger.Errorf("Connection test failed: %v", m.ConnectionError)
		return m.ConnectionError
	}
	m.Logger.Info("Connection test successful")
	return nil
}

// GetTableInfo 模拟获取表信息
func (m *MockStarRocksClientImpl) GetTableInfo(ctx context.Context, tableName string) (*starrocks.TableInfo, error) {
	if m.TableNotFoundError != nil && !m.isTableExists(tableName) {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	if info, exists := m.TableInfos[tableName]; exists {
		// 应用布尔类型修复逻辑
		for i := range info.Columns {
			col := &info.Columns[i]
			// 处理StarRocks中BOOLEAN类型可能被存储为tinyint(1)的情况
			if (col.Type == "tinyint" && (strings.Contains(strings.ToLower(col.Name), "boolean") ||
				strings.Contains(strings.ToLower(col.Name), "bool") ||
				strings.HasSuffix(strings.ToLower(col.Name), "_flag") ||
				strings.HasSuffix(strings.ToLower(col.Name), "_active"))) ||
				strings.Contains(col.Type, "tinyint(1)") {
				col.Type = "BOOLEAN"
			}
		}
		m.Logger.Infof("Retrieved table info for %s: %d columns", tableName, len(info.Columns))
		return info, nil
	}

	// 如果表不存在但没有设置错误，返回默认表信息
	defaultTableInfo := m.createDefaultTableInfo(tableName)
	m.TableInfos[tableName] = defaultTableInfo
	return defaultTableInfo, nil
}

// CountRows 模拟统计行数
func (m *MockStarRocksClientImpl) CountRows(ctx context.Context, tableName string, whereClause string) (int64, error) {
	key := tableName
	if whereClause != "" {
		key = fmt.Sprintf("%s_%s", tableName, whereClause)
	}

	if count, exists := m.RowCounts[key]; exists {
		m.Logger.Infof("Row count for %s: %d", key, count)
		return count, nil
	}

	// 默认返回1000行
	defaultCount := int64(1000)
	m.RowCounts[key] = defaultCount
	return defaultCount, nil
}

// ExecuteQuery 模拟执行查询
func (m *MockStarRocksClientImpl) ExecuteQuery(ctx context.Context, query string) (*flight.FlightInfo, error) {
	if m.QueryError != nil {
		m.Logger.Errorf("Query execution failed: %v", m.QueryError)
		return nil, m.QueryError
	}

	// 根据查询类型返回不同的结果
	queryKey := strings.ToLower(query)
	if info, exists := m.QueryResults[queryKey]; exists {
		m.Logger.Infof("Query executed successfully: %s", query)
		return info, nil
	}

	// 默认返回单个端点的FlightInfo
	defaultInfo := &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: &flight.Ticket{
					Ticket: []byte("mock_ticket_1"),
				},
			},
		},
	}
	m.QueryResults[queryKey] = defaultInfo
	return defaultInfo, nil
}

// DoGet 模拟获取数据流
func (m *MockStarRocksClientImpl) DoGet(ctx context.Context, ticket *flight.Ticket) (flight.FlightService_DoGetClient, error) {
	if m.DoGetError != nil {
		return nil, m.DoGetError
	}

	ticketKey := string(ticket.Ticket)
	if stream, exists := m.DoGetStreams[ticketKey]; exists {
		return stream, nil
	}

	// 返回默认的mock流
	mockStream := &MockFlightStream{
		Data: [][]byte{
			[]byte("mock_arrow_data_1"),
			[]byte("mock_arrow_data_2"),
		},
	}
	m.DoGetStreams[ticketKey] = mockStream
	return mockStream, nil
}

// NewArrowStreamWriterWithAutoCreate 模拟创建Arrow流式写入器
func (m *MockStarRocksClientImpl) NewArrowStreamWriterWithAutoCreate(tableName string, sourceSchema *arrow.Schema) (*starrocks.ArrowStreamWriter, error) {
	if m.CreateTableError != nil {
		return nil, m.CreateTableError
	}

	// 检查表是否需要自动创建
	if !m.isTableExists(tableName) && sourceSchema != nil {
		m.autoCreateTable(tableName, sourceSchema)
	}

	mockWriter := &MockArrowStreamWriter{
		TableName: tableName,
		Schema:    sourceSchema,
	}
	m.StreamWriters[tableName] = mockWriter
	m.Logger.Infof("Created Arrow stream writer for table %s", tableName)

	// 注意：这里返回的是接口，实际实现中需要类型适配
	// 在真实测试中，需要通过依赖注入或接口抽象来解决
	return nil, fmt.Errorf("mock implementation: cannot return concrete type")
}

// NewArrowDataWriterWithAutoCreate 模拟创建Arrow数据写入器
func (m *MockStarRocksClientImpl) NewArrowDataWriterWithAutoCreate(tableName string, batchSize int, sourceSchema *arrow.Schema) (*starrocks.ArrowDataWriter, error) {
	if m.CreateTableError != nil {
		return nil, m.CreateTableError
	}

	// 检查表是否需要自动创建
	if !m.isTableExists(tableName) && sourceSchema != nil {
		m.autoCreateTable(tableName, sourceSchema)
	}

	mockWriter := &MockArrowDataWriter{
		TableName:   tableName,
		BatchSize:   batchSize,
		Schema:      sourceSchema,
		WrittenRows: make([]map[string]interface{}, 0),
	}
	m.DataWriters[tableName] = mockWriter
	m.Logger.Infof("Created Arrow data writer for table %s with batch size %d", tableName, batchSize)

	// 注意：同样的类型适配问题
	return nil, fmt.Errorf("mock implementation: cannot return concrete type")
}

// Close 模拟关闭客户端
func (m *MockStarRocksClientImpl) Close() error {
	m.CloseCalled = true
	m.Logger.Info("StarRocks client closed")
	return nil
}

// 辅助方法：检查表是否存在
func (m *MockStarRocksClientImpl) isTableExists(tableName string) bool {
	_, exists := m.TableInfos[tableName]
	return exists
}

// 辅助方法：自动创建表
func (m *MockStarRocksClientImpl) autoCreateTable(tableName string, schema *arrow.Schema) {
	tableInfo := m.createTableInfoFromSchema(tableName, schema)
	m.TableInfos[tableName] = tableInfo
	m.CreatedTables = append(m.CreatedTables, tableName)
	m.Logger.Infof("Auto-created table %s with %d columns", tableName, len(tableInfo.Columns))
}

// 辅助方法：从Schema创建表信息
func (m *MockStarRocksClientImpl) createTableInfoFromSchema(tableName string, schema *arrow.Schema) *starrocks.TableInfo {
	columns := make([]starrocks.ColumnInfo, schema.NumFields())

	for i := 0; i < schema.NumFields(); i++ {
		field := schema.Field(i)
		columns[i] = starrocks.ColumnInfo{
			Name:         field.Name,
			Type:         m.convertArrowTypeToStarRocks(field.Type),
			DefaultValue: "",
			Comment:      "",
			IsNullable:   field.Nullable,
			IsPrimaryKey: false,
		}
	}

	return &starrocks.TableInfo{
		Name:       tableName,
		Engine:     "OLAP",
		TotalRows:  0,
		TotalBytes: 0,
		Columns:    columns,
		CreateTime: time.Now(),
		Comment:    "Auto-created by mock",
	}
}

// 辅助方法：创建默认表信息
func (m *MockStarRocksClientImpl) createDefaultTableInfo(tableName string) *starrocks.TableInfo {
	return &starrocks.TableInfo{
		Name:      tableName,
		Engine:    "OLAP",
		TotalRows: 1000,
		Columns: []starrocks.ColumnInfo{
			{Name: "id", Type: "BIGINT", IsNullable: false, IsPrimaryKey: true},
			{Name: "name", Type: "VARCHAR(255)", IsNullable: true, IsPrimaryKey: false},
			{Name: "created_at", Type: "DATETIME", IsNullable: true, IsPrimaryKey: false},
		},
		CreateTime: time.Now(),
		Comment:    "Mock table",
	}
}

// 辅助方法：Arrow类型转StarRocks类型
func (m *MockStarRocksClientImpl) convertArrowTypeToStarRocks(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.INT8:
		return "TINYINT"
	case arrow.INT16:
		return "SMALLINT"
	case arrow.INT32:
		return "INT"
	case arrow.INT64:
		return "BIGINT"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING:
		return "VARCHAR(65533)"
	case arrow.DATE32:
		return "DATE"
	case arrow.TIMESTAMP:
		return "DATETIME"
	default:
		return "VARCHAR(255)"
	}
}

// SetTableInfo 设置表信息（测试辅助方法）
func (m *MockStarRocksClientImpl) SetTableInfo(tableName string, info *starrocks.TableInfo) {
	m.TableInfos[tableName] = info
}

// SetRowCount 设置行数（测试辅助方法）
func (m *MockStarRocksClientImpl) SetRowCount(tableName string, count int64) {
	m.RowCounts[tableName] = count
}

// SetQueryResult 设置查询结果（测试辅助方法）
func (m *MockStarRocksClientImpl) SetQueryResult(query string, info *flight.FlightInfo) {
	m.QueryResults[strings.ToLower(query)] = info
}

// GetCreatedTables 获取创建的表列表（测试辅助方法）
func (m *MockStarRocksClientImpl) GetCreatedTables() []string {
	return m.CreatedTables
}

// GetDataWriter 获取数据写入器（测试辅助方法）
func (m *MockStarRocksClientImpl) GetDataWriter(tableName string) *MockArrowDataWriter {
	return m.DataWriters[tableName]
}

// GetStreamWriter 获取流式写入器（测试辅助方法）
func (m *MockStarRocksClientImpl) GetStreamWriter(tableName string) *MockArrowStreamWriter {
	return m.StreamWriters[tableName]
}
