package test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/ck2sr/ck2sr/pkg/starrocks"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/metadata"
)

// MockStarRocksClient mock StarRocks客户端接口
type MockStarRocksClient interface {
	TestConnection(ctx context.Context) error
	GetTableInfo(ctx context.Context, tableName string) (*starrocks.TableInfo, error)
	CountRows(ctx context.Context, tableName string, whereClause string) (int64, error)
	ExecuteQuery(ctx context.Context, query string) (*flight.FlightInfo, error)
	DoGet(ctx context.Context, ticket *flight.Ticket) (flight.FlightService_DoGetClient, error)
	NewArrowStreamWriterWithAutoCreate(tableName string, sourceSchema *arrow.Schema) (*starrocks.ArrowStreamWriter, error)
	NewArrowDataWriterWithAutoCreate(tableName string, batchSize int, sourceSchema *arrow.Schema) (*starrocks.ArrowDataWriter, error)
	Close() error
}

// MockDB mock数据库连接接口
type MockDB interface {
	QueryRowContext(ctx context.Context, query string, args ...interface{}) MockRow
	ExecContext(ctx context.Context, query string, args ...interface{}) (driver.Result, error)
	PingContext(ctx context.Context) error
	Close() error
}

// MockRow mock数据库行接口
type MockRow interface {
	Scan(dest ...interface{}) error
}

// MockArrowDataWriter mock Arrow数据写入器
type MockArrowDataWriter struct {
	TableName   string
	BatchSize   int
	Schema      *arrow.Schema
	RowCount    int
	FlushCount  int
	WriteError  error
	FlushError  error
	CloseError  error
	WrittenRows []map[string]interface{}
}

// WriteRowMap 模拟写入行数据
func (m *MockArrowDataWriter) WriteRowMap(rowData map[string]interface{}) error {
	if m.WriteError != nil {
		return m.WriteError
	}
	m.RowCount++
	m.WrittenRows = append(m.WrittenRows, rowData)
	return nil
}

// Flush 模拟刷新数据
func (m *MockArrowDataWriter) Flush() error {
	if m.FlushError != nil {
		return m.FlushError
	}
	m.FlushCount++
	return nil
}

// Close 模拟关闭写入器
func (m *MockArrowDataWriter) Close() error {
	if m.CloseError != nil {
		return m.CloseError
	}
	return nil
}

// MockArrowStreamWriter mock Arrow流式写入器
type MockArrowStreamWriter struct {
	TableName    string
	Schema       *arrow.Schema
	RecordCount  int64
	RowCount     int64
	ByteCount    int64
	WriteError   error
	FinalizeError error
	WrittenRecords []arrow.Record
}

// WriteArrowRecord 模拟写入Arrow记录
func (m *MockArrowStreamWriter) WriteArrowRecord(ctx context.Context, record arrow.Record) error {
	if m.WriteError != nil {
		return m.WriteError
	}
	m.RecordCount++
	m.RowCount += record.NumRows()
	m.ByteCount += 1024 // 模拟字节数
	m.WrittenRecords = append(m.WrittenRecords, record)
	record.Retain() // 保持引用
	return nil
}

// Finalize 模拟完成写入
func (m *MockArrowStreamWriter) Finalize(ctx context.Context) (*starrocks.ArrowWriteResult, error) {
	if m.FinalizeError != nil {
		return nil, m.FinalizeError
	}
	return &starrocks.ArrowWriteResult{
		RowsWritten:  m.RowCount,
		BytesWritten: m.ByteCount,
		Duration:     time.Millisecond * 100,
		Success:      true,
	}, nil
}

// Close 模拟关闭写入器
func (m *MockArrowStreamWriter) Close() error {
	// 释放记录引用
	for _, record := range m.WrittenRecords {
		record.Release()
	}
	return nil
}

// MockFlightStream mock Flight数据流
type MockFlightStream struct {
	Data    [][]byte
	Index   int
	Error   error
}

// Recv 模拟接收数据
func (m *MockFlightStream) Recv() (*flight.FlightData, error) {
	if m.Error != nil {
		return nil, m.Error
	}
	if m.Index >= len(m.Data) {
		return nil, fmt.Errorf("EOF")
	}
	data := &flight.FlightData{
		DataBody: m.Data[m.Index],
	}
	m.Index++
	return data, nil
}

// CloseSend 模拟关闭发送（实现FlightService_DoGetClient接口）
func (m *MockFlightStream) CloseSend() error {
	return nil
}

// Header 模拟获取头部信息
func (m *MockFlightStream) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

// Trailer 模拟获取尾部信息
func (m *MockFlightStream) Trailer() metadata.MD {
	return metadata.MD{}
}

// Context 模拟获取上下文
func (m *MockFlightStream) Context() context.Context {
	return context.Background()
}

// SendMsg 模拟发送消息
func (m *MockFlightStream) SendMsg(msg interface{}) error {
	return nil
}

// RecvMsg 模拟接收消息
func (m *MockFlightStream) RecvMsg(msg interface{}) error {
	return nil
}

// MockLogger mock日志记录器
type MockLogger struct {
	*logrus.Logger
	InfoMessages    []string
	WarningMessages []string
	ErrorMessages   []string
	DebugMessages   []string
}

// NewMockLogger 创建mock日志记录器
func NewMockLogger() *MockLogger {
	return &MockLogger{
		Logger: logrus.New(),
	}
}

// Info 记录info日志
func (m *MockLogger) Info(args ...interface{}) {
	m.InfoMessages = append(m.InfoMessages, args[0].(string))
	m.Logger.Info(args...)
}

// Infof 记录info格式化日志
func (m *MockLogger) Infof(format string, args ...interface{}) {
	m.InfoMessages = append(m.InfoMessages, format)
	m.Logger.Infof(format, args...)
}

// Warn 记录warning日志
func (m *MockLogger) Warn(args ...interface{}) {
	m.WarningMessages = append(m.WarningMessages, args[0].(string))
	m.Logger.Warn(args...)
}

// Warnf 记录warning格式化日志
func (m *MockLogger) Warnf(format string, args ...interface{}) {
	m.WarningMessages = append(m.WarningMessages, format)
	m.Logger.Warnf(format, args...)
}

// Error 记录error日志
func (m *MockLogger) Error(args ...interface{}) {
	m.ErrorMessages = append(m.ErrorMessages, args[0].(string))
	m.Logger.Error(args...)
}

// Errorf 记录error格式化日志
func (m *MockLogger) Errorf(format string, args ...interface{}) {
	m.ErrorMessages = append(m.ErrorMessages, format)
	m.Logger.Errorf(format, args...)
}

// Debug 记录debug日志
func (m *MockLogger) Debug(args ...interface{}) {
	m.DebugMessages = append(m.DebugMessages, args[0].(string))
	m.Logger.Debug(args...)
}

// Debugf 记录debug格式化日志
func (m *MockLogger) Debugf(format string, args ...interface{}) {
	m.DebugMessages = append(m.DebugMessages, format)
	m.Logger.Debugf(format, args...)
}