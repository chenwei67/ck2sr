package protocol

import (
	"context"
)

// DataReader 数据读取器接口
// 提供统一的数据读取抽象，支持迭代访问数据
type DataReader interface {
	// Next 移动到下一条记录
	// 返回true表示有数据，false表示已到末尾
	Next() bool

	// Err Next过程中是否有错误
	Err() error
	
	// GetRecord 获取当前记录
	// 返回interface{}类型的记录数据，通常为map[string]interface{}
	GetRecord() (interface{}, error)

	// 释放已使用完的record内存，请在GetRecord()返回的数据处理完后调用
	ReleaseRecords(records []interface{})

	// Close 关闭读取器，释放资源
	Close() error
}

// BatchReader 批量数据读取器接口
// 扩展DataReader，提供批量读取能力
type BatchReader interface {
	DataReader

	// GetBatch 批量获取记录
	// size: 期望的批次大小
	// 返回实际读取的记录切片
	GetBatch(size int) ([]interface{}, error)
}

// CountableReader 可计数的数据读取器接口
// 扩展DataReader，提供数据计数能力
type CountableReader interface {
	DataReader

	// Count 获取总记录数
	// 用于进度计算和统计
	Count(ctx context.Context) (int64, error)
}

// OffsetReader 支持偏移量的数据读取器接口
// 扩展DataReader，支持从指定位置开始读取
type OffsetReader interface {
	DataReader

	// SetOffset 设置读取起始偏移量
	// offset: 起始位置，通常为行号
	SetOffset(offset int64) error

	// GetOffset 获取当前读取位置
	GetOffset() int64
}
