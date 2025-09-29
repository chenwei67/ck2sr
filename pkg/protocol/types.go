package protocol

import (
	"time"
)

// ColumnInfo 列信息结构
// 描述数据库表列的元数据信息
type ColumnInfo struct {
	Name         string  `json:"name"`          // 列名
	DataType     string  `json:"data_type"`     // 数据类型
	IsNullable   bool    `json:"is_nullable"`   // 是否可空
	DefaultValue *string `json:"default_value"` // 默认值
	Comment      string  `json:"comment"`       // 列注释
	CharLength   *int64  `json:"char_length"`   // 字符长度
	NumPrecision *int64  `json:"num_precision"` // 数值精度
	NumScale     *int64  `json:"num_scale"`     // 数值标度
}

// ResultSet 查询结果集接口
// 提供统一的结果集访问方法
type ResultSet interface {
	Next() bool                               // 移动到下一行
	Scan(dest ...interface{}) error          // 扫描当前行到目标变量
	Columns() ([]string, error)              // 获取列名列表
	Close() error                            // 关闭结果集
}

// Stats 数据传输统计信息
// 记录数据处理过程中的性能和进度统计
type Stats struct {
	TotalRows      int64         `json:"total_rows"`       // 总处理行数
	TotalBytes     int64         `json:"total_bytes"`      // 总处理字节数
	Duration       time.Duration `json:"duration"`         // 处理持续时间
	RowsPerSecond  float64       `json:"rows_per_second"`  // 每秒处理行数
	BytesPerSecond float64       `json:"bytes_per_second"` // 每秒处理字节数
}

// StreamLoadResponse Stream Load响应结构
// StarRocks Stream Load操作的响应信息
type StreamLoadResponse struct {
	TxnId                  int64  `json:"TxnId"`                  // 事务ID
	Label                  string `json:"Label"`                  // 标签
	Status                 string `json:"Status"`                 // 状态
	Message                string `json:"Message"`                // 消息
	NumberTotalRows        int64  `json:"NumberTotalRows"`        // 总行数
	NumberLoadedRows       int64  `json:"NumberLoadedRows"`       // 已加载行数
	NumberFilteredRows     int64  `json:"NumberFilteredRows"`     // 过滤行数
	NumberUnselectedRows   int64  `json:"NumberUnselectedRows"`   // 未选择行数
	LoadBytes              int64  `json:"LoadBytes"`              // 加载字节数
	LoadTimeMs             int64  `json:"LoadTimeMs"`             // 加载时间(毫秒)
	BeginTxnTimeMs         int64  `json:"BeginTxnTimeMs"`         // 开始事务时间
	StreamLoadPlanTimeMs   int64  `json:"StreamLoadPlanTimeMs"`   // 计划时间
	ReadDataTimeMs         int64  `json:"ReadDataTimeMs"`         // 读取数据时间
	WriteDataTimeMs        int64  `json:"WriteDataTimeMs"`        // 写入数据时间
	CommitAndPublishTimeMs int64  `json:"CommitAndPublishTimeMs"` // 提交发布时间
}