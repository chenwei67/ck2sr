package utils

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ParseArray 尝试解析字符串为数组或JSON对象
func ParseArray(strVal string) (interface{}, error) {
	strVal = strings.TrimSpace(strVal)

	// 检查是否是数组格式 [...]
	if strings.HasPrefix(strVal, "[") && strings.HasSuffix(strVal, "]") {
		var arr []interface{}
		err := json.Unmarshal([]byte(strVal), &arr)
		if err == nil {
			return arr, nil
		}
		// 如果标准JSON解析失败，尝试处理ClickHouse特殊格式
		return ParseClickHouseArray(strVal)
	}

	// 检查是否是JSON对象格式 {...}
	if strings.HasPrefix(strVal, "{") && strings.HasSuffix(strVal, "}") {
		var obj map[string]interface{}
		err := json.Unmarshal([]byte(strVal), &obj)
		if err == nil {
			return obj, nil
		}
	}

	return nil, fmt.Errorf("not an array or JSON object")
}

// ParseClickHouseArray 解析ClickHouse特殊格式的数组
// ClickHouse数组格式示例: [1,2,3] 或 ['a','b','c'] 或 [1.5,2.5,3.5]
func ParseClickHouseArray(strVal string) (interface{}, error) {
	if len(strVal) < 2 {
		return nil, fmt.Errorf("invalid array format")
	}

	content := strVal[1 : len(strVal)-1]
	content = strings.TrimSpace(content)

	// 空数组
	if content == "" {
		return []interface{}{}, nil
	}

	elements := SplitArrayElements(content)

	result := make([]interface{}, 0, len(elements))
	for _, elem := range elements {
		elem = strings.TrimSpace(elem)

		// 处理字符串元素（带引号）
		if (strings.HasPrefix(elem, "'") && strings.HasSuffix(elem, "'")) ||
			(strings.HasPrefix(elem, "\"") && strings.HasSuffix(elem, "\"")) {
			unquoted := elem[1 : len(elem)-1]
			unquoted = strings.ReplaceAll(unquoted, "\\'", "'")
			unquoted = strings.ReplaceAll(unquoted, "\\\"", "\"")
			result = append(result, unquoted)
			continue
		}

		// 尝试解析为整数
		if intVal, err := strconv.ParseInt(elem, 10, 64); err == nil {
			result = append(result, intVal)
			continue
		}

		// 尝试解析为浮点数
		if floatVal, err := strconv.ParseFloat(elem, 64); err == nil {
			result = append(result, floatVal)
			continue
		}

		// 尝试解析为布尔值
		if elem == "true" || elem == "false" {
			result = append(result, elem == "true")
			continue
		}

		// 默认作为字符串
		result = append(result, elem)
	}

	return result, nil
}

// SplitArrayElements 按逗号分割数组元素，考虑引号内的逗号
func SplitArrayElements(content string) []string {
	var elements []string
	var current strings.Builder
	inQuote := false
	quoteChar := rune(0)

	for i, ch := range content {
		switch ch {
		case '\'', '"':
			if !inQuote {
				inQuote = true
				quoteChar = ch
			} else if ch == quoteChar {
				if i > 0 && content[i-1] != '\\' {
					inQuote = false
					quoteChar = 0
				}
			}
			current.WriteRune(ch)
		case ',':
			if !inQuote {
				elements = append(elements, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		elements = append(elements, current.String())
	}

	return elements
}

// IsArray 判断字符串是否为数组格式
func IsArray(strVal string) bool {
	strVal = strings.TrimSpace(strVal)
	return strings.HasPrefix(strVal, "[") && strings.HasSuffix(strVal, "]")
}

// IsJSON 判断字符串是否为JSON对象格式
func IsJSON(strVal string) bool {
	strVal = strings.TrimSpace(strVal)
	return strings.HasPrefix(strVal, "{") && strings.HasSuffix(strVal, "}")
}