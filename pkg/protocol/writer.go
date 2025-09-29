package protocol

import (
	"context"
)

// DataWriter 数据写入器接口
// 提供统一的数据写入抽象，支持批量写入和资源管理
type DataWriter interface {
	// Write 写入一批记录
	// records: 记录切片，每个记录为interface{}类型，通常为map[string]interface{}
	Write(ctx context.Context, records []interface{}) error

	// Flush 刷新缓冲区
	// 强制将缓存的数据写入目标存储
	Flush(ctx context.Context) error

	// Close 关闭写入器，释放资源
	Close() error
}

// StreamWriter Stream Load 写入器接口
// 扩展DataWriter，提供StarRocks Stream Load能力
type StreamWriter interface {
	DataWriter

	// StreamLoad 执行 Stream Load
	// data: 要写入的原始数据字节
	// 返回StarRocks的Stream Load响应信息
	StreamLoad(ctx context.Context, data []byte) (*StreamLoadResponse, error)
}

// BulkWriter 批量写入器接口
// 扩展DataWriter，提供批量写入和统计能力
type BulkWriter interface {
	DataWriter

	// WriteBatch 批量写入
	// records: 记录切片，每个记录为interface{}类型
	WriteBatch(ctx context.Context, records []interface{}) error

	// GetStats 获取写入统计
	// 返回写入过程的性能统计信息
	GetStats() Stats
}