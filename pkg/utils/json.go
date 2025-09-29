package utils

import (
	"encoding/json"
	"fmt"
)

// MarshalJSON 将数据序列化为JSON
func MarshalJSON(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

// UnmarshalJSON 将JSON反序列化为数据
func UnmarshalJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// MarshalJSONIndent 将数据序列化为格式化的JSON
func MarshalJSONIndent(data interface{}, prefix, indent string) ([]byte, error) {
	return json.MarshalIndent(data, prefix, indent)
}

// PrettyJSON 美化JSON字符串
func PrettyJSON(jsonStr string) (string, error) {
	var obj interface{}
	if err := json.Unmarshal([]byte(jsonStr), &obj); err != nil {
		return "", fmt.Errorf("invalid JSON: %w", err)
	}

	bytes, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}