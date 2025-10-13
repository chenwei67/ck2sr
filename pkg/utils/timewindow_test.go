package utils

import (
	"testing"
	"time"
)

func TestParseTimeOfDay(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"Valid time 1", "01:00", false},
		{"Valid time 2", "23:59", false},
		{"Valid time 3", "00:00", false},
		{"Valid time 4", "1:00", false},  // Go time.Parse 支持单位数小时
		{"Empty string", "", true},
		{"Invalid format 1", "25:00", true},
		{"Invalid format 2", "12:60", true},
		{"Invalid format 3", "abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseTimeOfDay(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseTimeOfDay() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIsInTimeWindow(t *testing.T) {
	tests := []struct {
		name      string
		startTime string
		endTime   string
		wantErr   bool
	}{
		{"Same day window", "01:00", "05:00", false},
		{"Cross-midnight window", "23:00", "03:00", false},
		{"Empty times", "", "", false},
		{"Invalid start", "25:00", "05:00", true},
		{"Invalid end", "01:00", "25:00", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := IsInTimeWindow(tt.startTime, tt.endTime)
			if (err != nil) != tt.wantErr {
				t.Errorf("IsInTimeWindow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCalculateWaitDuration(t *testing.T) {
	now := time.Now()

	// 测试同日窗口（未来时间）
	futureTime := now.Add(2 * time.Hour)
	startTime := futureTime.Format("15:04")
	endTime := futureTime.Add(4 * time.Hour).Format("15:04")

	duration, err := CalculateWaitDuration(startTime, endTime)
	if err != nil {
		t.Errorf("CalculateWaitDuration() error = %v", err)
	}

	// 等待时间应该约为2小时（允许1分钟误差）
	expectedMin := 2*time.Hour - time.Minute
	expectedMax := 2*time.Hour + time.Minute
	if duration < expectedMin || duration > expectedMax {
		t.Errorf("CalculateWaitDuration() = %v, want between %v and %v", duration, expectedMin, expectedMax)
	}
}

func TestFormatTimeWindow(t *testing.T) {
	tests := []struct {
		name      string
		startTime string
		endTime   string
		want      string
	}{
		{"Normal window", "01:00", "05:00", "01:00 - 05:00"},
		{"Empty window", "", "", "全天候（无时间窗口限制）"},
		{"Partial empty 1", "01:00", "", "全天候（无时间窗口限制）"},
		{"Partial empty 2", "", "05:00", "全天候（无时间窗口限制）"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatTimeWindow(tt.startTime, tt.endTime)
			if got != tt.want {
				t.Errorf("FormatTimeWindow() = %v, want %v", got, tt.want)
			}
		})
	}
}
