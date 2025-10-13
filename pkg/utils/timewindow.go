package utils

import (
	"fmt"
	"time"
)

// ParseTimeOfDay 解析时间字符串（HH:MM格式）为当天的具体时间
// 返回的时间为今天的指定时刻
func ParseTimeOfDay(timeStr string) (time.Time, error) {
	if timeStr == "" {
		return time.Time{}, fmt.Errorf("time string is empty")
	}

	// 解析 HH:MM 格式
	t, err := time.Parse("15:04", timeStr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time format (expected HH:MM): %w", err)
	}

	// 将解析的时间应用到今天
	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(), t.Hour(), t.Minute(), 0, 0, now.Location()), nil
}

// IsInTimeWindow 检查当前时间是否在时间窗口内
// 支持跨日时间窗口（如 23:00 - 03:00）
func IsInTimeWindow(startTime, endTime string) (bool, error) {
	if startTime == "" || endTime == "" {
		return true, nil // 空时间窗口表示不限制
	}

	start, err := ParseTimeOfDay(startTime)
	if err != nil {
		return false, fmt.Errorf("failed to parse start time: %w", err)
	}

	end, err := ParseTimeOfDay(endTime)
	if err != nil {
		return false, fmt.Errorf("failed to parse end time: %w", err)
	}

	now := time.Now()

	// 检查是否为跨日时间窗口
	if end.Before(start) {
		// 跨日窗口：例如 23:00 - 03:00
		// 当前时间在 [23:00, 24:00) 或 [00:00, 03:00] 范围内
		return now.After(start) || now.Before(end) || now.Equal(start) || now.Equal(end), nil
	}

	// 同日窗口：例如 01:00 - 05:00
	return (now.After(start) || now.Equal(start)) && (now.Before(end) || now.Equal(end)), nil
}

// WaitForTimeWindow 阻塞等待直到进入时间窗口
func WaitForTimeWindow(startTime, endTime string) (time.Duration, error) {
	if startTime == "" || endTime == "" {
		return 0, nil // 无时间窗口限制，立即返回
	}

	// 计算等待时间
	waitDuration, err := CalculateWaitDuration(startTime, endTime)
	if err != nil {
		return 0, err
	}

	// 阻塞等待
	time.Sleep(waitDuration)
	return waitDuration, nil
}

// CalculateWaitDuration 计算距离下一个时间窗口开始的等待时间
func CalculateWaitDuration(startTime, endTime string) (time.Duration, error) {
	start, err := ParseTimeOfDay(startTime)
	if err != nil {
		return 0, fmt.Errorf("failed to parse start time: %w", err)
	}

	end, err := ParseTimeOfDay(endTime)
	if err != nil {
		return 0, fmt.Errorf("failed to parse end time: %w", err)
	}

	now := time.Now()

	// 如果是跨日窗口
	if end.Before(start) {
		// 检查当前是否在窗口的后半段（今天的 [00:00, endTime]）
		if now.Before(end) {
			// 已在窗口内（早晨部分）
			return 0, nil
		}
		// 检查当前是否在窗口的前半段（今天的 [startTime, 24:00)）
		if now.After(start) || now.Equal(start) {
			// 已在窗口内（夜晚部分）
			return 0, nil
		}
		// 当前在两个窗口之间（今天的 [endTime, startTime)）
		// 等待到今天的 startTime
		return start.Sub(now), nil
	}

	// 同日窗口
	if now.Before(start) {
		// 等待到今天的开始时间
		return start.Sub(now), nil
	} else if now.After(end) {
		// 已过今天的窗口，等待到明天的开始时间
		tomorrow := start.Add(24 * time.Hour)
		return tomorrow.Sub(now), nil
	}

	// 已在窗口内
	return 0, nil
}

// FormatTimeWindow 格式化时间窗口为可读字符串
func FormatTimeWindow(startTime, endTime string) string {
	if startTime == "" || endTime == "" {
		return "全天候（无时间窗口限制）"
	}
	return fmt.Sprintf("%s - %s", startTime, endTime)
}
