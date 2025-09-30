package protocol

import (
	"context"
)

// DataWriter 数据写入器接口
// 提供统一的数据写入抽象，支持批量写入和资源管理
type DataWriter interface {
	// Write 写入一批记录
	// records: 记录切片，每个记录为interface{}类型，通常为map[string]interface{}
	Write(ctx context.Context, records interface{}) error

	// Flush 刷新缓冲区
	// 强制将缓存的数据写入目标存储
	Flush(ctx context.Context) error

	// Close 关闭写入器，释放资源
	Close() error
}
