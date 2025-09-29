package utils

import (
	"fmt"
	"strings"
	"time"
)

// ParseTimeRange 解析时间范围
func ParseTimeRange(startTime, endTime string) (time.Time, time.Time, error) {
	layout := "2006-01-02 15:04:05"

	var start, end time.Time
	var err error

	if startTime != "" {
		start, err = time.Parse(layout, startTime)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid start time: %w", err)
		}
	}

	if endTime != "" {
		end, err = time.Parse(layout, endTime)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid end time: %w", err)
		}
	}

	return start, end, nil
}

// FormatDuration 格式化时间间隔
func FormatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1fm", d.Minutes())
	}
	return fmt.Sprintf("%.1fh", d.Hours())
}

// ParseAndFormatDate 解析并格式化日期
func ParseAndFormatDate(dateStr string) string {
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, dateStr); err == nil {
			return t.Format("2006-01-02")
		}
	}

	return strings.TrimSpace(dateStr)
}

// FormatBytes 格式化字节数
func FormatBytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	} else if bytes < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(bytes)/1024)
	} else if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.1f MB", float64(bytes)/1024/1024)
	} else {
		return fmt.Sprintf("%.2f GB", float64(bytes)/1024/1024/1024)
	}
}