package utils

import (
    "encoding/json"
    "fmt"
)

// CalculateRecordSize 计算记录的字节大小
// 通过将记录序列化为JSON来估算其内存占用
// 参数:
//   - record: 记录数据，通常为 map[string]interface{} 或结构体
// 返回:
//   - int64: 记录的字节大小
//   - error: 序列化失败时返回错误
func CalculateRecordSize(record interface{}) (int64, error) {
    if record == nil {
        return 0, nil
    }

    // 将记录序列化为JSON
    data, err := json.Marshal(record)
    if err != nil {
        return 0, fmt.Errorf("failed to marshal record: %w", err)
    }

    // 返回JSON字节数组的长度
    return int64(len(data)), nil
}

func EncodeRecordJSON(record interface{}) ([]byte, error) {
    return json.Marshal(record)
}

// CalculateBatchSize 计算批次数据的总字节大小
// 参数:
//   - records: 记录列表
// 返回:
//   - int64: 批次的总字节大小
//   - error: 计算失败时返回错误
func CalculateBatchSize(records []interface{}) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	var totalSize int64
	for _, record := range records {
		size, err := CalculateRecordSize(record)
		if err != nil {
			return 0, err
		}
		totalSize += size
	}

	return totalSize, nil
}
