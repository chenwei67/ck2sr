package reader

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// RawValue 表示未解析的原始值（用于延迟解析 JSON）
type RawValue struct {
	IsJSON bool   // 标识是否为 JSON 类型
	Data   []byte // 原始字节数据
}

// Record 强类型记录结构
// 使用切片存储值，避免 map 的内存开销和反射性能损耗
type Record struct {
	// Columns 列元数据（共享，不需要每行复制）
	Columns *ColumnMetadata

	// Values 按列顺序存储的值
	// 使用 interface{} 以支持多种类型，但避免嵌套 map
	Values []interface{}

	// columnIndex 列名到索引的映射（共享，延迟初始化）
	columnIndex map[string]int
}

// ColumnMetadata 列元数据（所有记录共享）
type ColumnMetadata struct {
	Names     []string       // 列名列表
	DataTypes []string       // 列类型列表
	Index     map[string]int // 列名到索引的映射
	mu        sync.RWMutex   // 保护并发初始化
}

// NewColumnMetadata 创建新的列元数据
func NewColumnMetadata(names []string, dataTypes []string) *ColumnMetadata {
	cm := &ColumnMetadata{
		Names:     names,
		DataTypes: dataTypes,
		Index:     make(map[string]int, len(names)),
	}

	// 构建索引
	for i, name := range names {
		cm.Index[name] = i
	}

	return cm
}

// GetColumnIndex 获取列索引（线程安全）
func (cm *ColumnMetadata) GetColumnIndex(columnName string) (int, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	idx, ok := cm.Index[columnName]
	return idx, ok
}

// NewRecord 创建新记录
func NewRecord(columns *ColumnMetadata) *Record {
	return &Record{
		Columns: columns,
		Values:  make([]interface{}, len(columns.Names)),
	}
}

// Get 按列名获取值
func (r *Record) Get(columnName string) (interface{}, error) {
	idx, ok := r.Columns.GetColumnIndex(columnName)
	if !ok {
		return nil, fmt.Errorf("column %s not found", columnName)
	}
	return r.Values[idx], nil
}

// Set 按列名设置值
func (r *Record) Set(columnName string, value interface{}) error {
	idx, ok := r.Columns.GetColumnIndex(columnName)
	if !ok {
		return fmt.Errorf("column %s not found", columnName)
	}
	r.Values[idx] = value
	return nil
}

// GetByIndex 按索引获取值（更高效）
func (r *Record) GetByIndex(index int) interface{} {
	if index < 0 || index >= len(r.Values) {
		return nil
	}
	return r.Values[index]
}

// SetByIndex 按索引设置值（更高效）
func (r *Record) SetByIndex(index int, value interface{}) {
	if index >= 0 && index < len(r.Values) {
		r.Values[index] = value
	}
}

// Reset 重置记录（用于对象池复用）
func (r *Record) Reset() {
	for i := range r.Values {
		r.Values[i] = nil
	}
}

// ToMap 转换为 map（向后兼容，性能较差，避免使用）
func (r *Record) ToMap() map[string]interface{} {
	result := make(map[string]interface{}, len(r.Columns.Names))
	for i, name := range r.Columns.Names {
		result[name] = r.Values[i]
	}
	return result
}

// MarshalJSON 实现 JSON 序列化接口
// 优化：避免中间 map 分配，直接序列化为 JSON
func (r *Record) MarshalJSON() ([]byte, error) {
	// 预估 JSON 大小，减少扩容
	estimatedSize := 128 // 初始容量
	for i := range r.Columns.Names {
		val := r.Values[i]
		if val == nil {
			continue // 跳过nil值
		}
		estimatedSize += len(r.Columns.Names[i]) + 4

		switch v := val.(type) {
		case string:
			estimatedSize += len(v) * 2 // 预留转义空间
		case RawValue:
			estimatedSize += len(v.Data) + 10
		default:
			estimatedSize += 20
		}
	}

	buf := make([]byte, 0, estimatedSize)
	buf = append(buf, '{')

	firstField := true // 用于控制逗号

	for i, name := range r.Columns.Names {
		val := r.Values[i]

		// 🔧 修复1：跳过 nil 值（被过滤或NULL的列）
		if val == nil {
			continue
		}

		// 添加逗号分隔符（非首字段）
		if !firstField {
			buf = append(buf, ',')
		}
		firstField = false

		// 写入列名（安全转义）
		buf = append(buf, '"')
		buf = appendEscapedString(buf, name)
		buf = append(buf, '"', ':')

		// 🔧 修复2：处理 RawValue
		if rawVal, ok := val.(RawValue); ok {
			if rawVal.IsJSON {
				// 直接写入原始 JSON 字节（假设已验证有效）
				buf = append(buf, strings.ReplaceAll(string(rawVal.Data), "'", "\"")...)
			}
			continue
		}

		// 🔧 修复3：处理 []byte 类型（避免 base64 编码）
		// 对于非 RawValue 的 []byte，将其转换为字符串
		// 这修复了字符串被 base64 编码的问题
		if bytesVal, ok := val.([]byte); ok {
			// 将 []byte 转换为字符串并序列化
			strVal := string(bytesVal)
			strBytes, err := json.Marshal(strVal)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal []byte as string for column %s: %w", name, err)
			}
			buf = append(buf, strBytes...)
			continue
		}

		// 其他类型使用标准 JSON 序列化
		valBytes, err := json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal value for column %s: %w", name, err)
		}
		buf = append(buf, valBytes...)
	}

	buf = append(buf, '}')
	return buf, nil
}

// appendEscapedString 追加转义后的字符串（用于列名）
func appendEscapedString(buf []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"', '\\':
			buf = append(buf, '\\', c)
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			if c < 0x20 {
				// 控制字符转义为 \uXXXX
				buf = append(buf, '\\', 'u', '0', '0')
				buf = append(buf, hexDigit(c>>4), hexDigit(c&0xF))
			} else {
				buf = append(buf, c)
			}
		}
	}
	return buf
}

// appendEscapedBytes 追加转义后的字节数组（用于RawValue的非JSON数据）
func appendEscapedBytes(buf []byte, data []byte) []byte {
	for _, c := range data {
		switch c {
		case '"', '\\':
			buf = append(buf, '\\', c)
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			if c < 0x20 {
				// 控制字符转义为 \uXXXX
				buf = append(buf, '\\', 'u', '0', '0')
				buf = append(buf, hexDigit(c>>4), hexDigit(c&0xF))
			} else {
				buf = append(buf, c)
			}
		}
	}
	return buf
}

// hexDigit 返回十六进制数字字符
func hexDigit(n byte) byte {
	if n < 10 {
		return '0' + n
	}
	return 'a' + (n - 10)
}

// RecordPool 记录对象池
type RecordPool struct {
	pool    sync.Pool
	columns *ColumnMetadata
}

// NewRecordPool 创建新的记录对象池
func NewRecordPool(columns *ColumnMetadata) *RecordPool {
	return &RecordPool{
		columns: columns,
		pool: sync.Pool{
			New: func() interface{} {
				return NewRecord(columns)
			},
		},
	}
}

// Get 从对象池获取记录
func (rp *RecordPool) Get() *Record {
	record := rp.pool.Get().(*Record)
	record.Reset() // 确保记录已重置
	return record
}

// Put 归还记录到对象池
func (rp *RecordPool) Put(record *Record) {
	if record != nil {
		rp.pool.Put(record)
	}
}
