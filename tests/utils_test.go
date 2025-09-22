package tests

import (
	"context"
	"testing"
	"time"
)

func TestBytesToHumanReadable(t *testing.T) {
	tests := []struct {
		bytes    int64
		expected string
	}{
		{512, "512 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1048576, "1.0 MB"},
		{1073741824, "1.0 GB"},
		{0, "0 B"},
	}

	for _, tt := range tests {
		result := BytesToHumanReadable(tt.bytes)
		if result != tt.expected {
			t.Errorf("BytesToHumanReadable(%d) = %s, expected %s", tt.bytes, result, tt.expected)
		}
	}
}

func TestRateToHumanReadable(t *testing.T) {
	rate := 1048576.0 // 1MB/s
	expected := "1.0 MB/s"
	result := RateToHumanReadable(rate)
	if result != expected {
		t.Errorf("RateToHumanReadable(%.0f) = %s, expected %s", rate, result, expected)
	}
}

func TestDurationToHumanReadable(t *testing.T) {
	tests := []struct {
		duration time.Duration
		expected string
	}{
		{30 * time.Second, "30.0s"},
		{2 * time.Minute, "2.0m"},
		{1*time.Hour + 30*time.Minute, "1.5h"},
		{25 * time.Hour, "1.0d"},
	}

	for _, tt := range tests {
		result := DurationToHumanReadable(tt.duration)
		if result != tt.expected {
			t.Errorf("DurationToHumanReadable(%v) = %s, expected %s", tt.duration, result, tt.expected)
		}
	}
}

func TestCalculateProgress(t *testing.T) {
	tests := []struct {
		completed int64
		total     int64
		expected  float64
	}{
		{250, 1000, 25.0},
		{0, 1000, 0.0},
		{1000, 1000, 100.0},
		{500, 0, 0.0}, // 避免除零
	}

	for _, tt := range tests {
		result := CalculateProgress(tt.completed, tt.total)
		if result != tt.expected {
			t.Errorf("CalculateProgress(%d, %d) = %.1f, expected %.1f", tt.completed, tt.total, result, tt.expected)
		}
	}
}

func TestEstimateRemainingTime(t *testing.T) {
	// 测试基本情况
	completed := int64(250)
	total := int64(1000)
	elapsed := 10 * time.Second

	remaining := EstimateRemainingTime(completed, total, elapsed)

	// 250/1000 在 10秒内完成，剩余 750 应该需要 30秒
	expected := 30 * time.Second
	if remaining != expected {
		t.Errorf("EstimateRemainingTime(%d, %d, %v) = %v, expected %v", completed, total, elapsed, remaining, expected)
	}

	// 测试边界情况
	remaining = EstimateRemainingTime(0, 1000, elapsed)
	if remaining != 0 {
		t.Error("Remaining time should be 0 when completed is 0")
	}

	remaining = EstimateRemainingTime(1000, 1000, elapsed)
	if remaining != 0 {
		t.Error("Remaining time should be 0 when completed equals total")
	}
}

func TestMD5Hash(t *testing.T) {
	text := "hello world"
	hash := MD5Hash(text)

	if len(hash) != 32 {
		t.Errorf("MD5 hash should be 32 characters long, got %d", len(hash))
	}

	// 相同输入应该产生相同输出
	hash2 := MD5Hash(text)
	if hash != hash2 {
		t.Error("MD5 hash should be deterministic")
	}

	// 不同输入应该产生不同输出
	hash3 := MD5Hash("hello world!")
	if hash == hash3 {
		t.Error("Different inputs should produce different hashes")
	}
}

func TestCRC32Hash(t *testing.T) {
	text := "hello world"
	hash := CRC32Hash(text)

	// 相同输入应该产生相同输出
	hash2 := CRC32Hash(text)
	if hash != hash2 {
		t.Error("CRC32 hash should be deterministic")
	}

	// 不同输入应该产生不同输出
	hash3 := CRC32Hash("hello world!")
	if hash == hash3 {
		t.Error("Different inputs should produce different hashes")
	}
}

func TestRetry(t *testing.T) {
	ctx := context.Background()
	config := RetryConfig{
		MaxRetries:    3,
		InitialDelay:  10 * time.Millisecond,
		MaxDelay:      100 * time.Millisecond,
		BackoffFactor: 2.0,
	}

	// 测试成功情况
	attempts := 0
	err := Retry(ctx, config, func() error {
		attempts++
		if attempts < 3 {
			return fmt.Errorf("attempt %d failed", attempts)
		}
		return nil
	})

	if err != nil {
		t.Errorf("Retry should succeed, got error: %v", err)
	}

	if attempts != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}

	// 测试失败情况
	attempts = 0
	err = Retry(ctx, config, func() error {
		attempts++
		return fmt.Errorf("always fails")
	})

	if err == nil {
		t.Error("Retry should fail when operation always fails")
	}

	if attempts != 4 { // 1 + 3 重试
		t.Errorf("Expected 4 attempts, got %d", attempts)
	}

	// 测试上下文取消
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	err = Retry(cancelCtx, config, func() error {
		return fmt.Errorf("should not run")
	})

	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}

func TestRateLimiter(t *testing.T) {
	ctx := context.Background()

	// 创建速率限制器
	limiter := NewRateLimiter(1000, 100, 10) // 1000 bytes/s, 100 rows/s, burst 10

	// 测试字节限制
	start := time.Now()
	err := limiter.WaitForBytes(ctx, 10)
	if err != nil {
		t.Fatalf("WaitForBytes failed: %v", err)
	}
	elapsed := time.Since(start)

	// 应该几乎立即返回（在 burst 范围内）
	if elapsed > 100*time.Millisecond {
		t.Errorf("WaitForBytes took too long: %v", elapsed)
	}

	// 测试行数限制
	err = limiter.WaitForRows(ctx, 5)
	if err != nil {
		t.Fatalf("WaitForRows failed: %v", err)
	}

	// 测试允许检查
	if !limiter.AllowBytes(5) {
		t.Error("Should allow small byte request")
	}

	if !limiter.AllowRows(5) {
		t.Error("Should allow small row request")
	}

	// 测试无限制情况
	unlimitedLimiter := NewRateLimiter(0, 0, 0)
	err = unlimitedLimiter.WaitForBytes(ctx, 1000000)
	if err != nil {
		t.Errorf("Unlimited limiter should not fail: %v", err)
	}
}

func TestMetricsCollector(t *testing.T) {
	collector := NewMetricsCollector()

	// 测试初始状态
	bytes, rows := collector.GetTotalProcessed()
	if bytes != 0 || rows != 0 {
		t.Error("Initial processed counts should be zero")
	}

	elapsed := collector.GetElapsed()
	if elapsed <= 0 {
		t.Error("Elapsed time should be positive")
	}

	// 更新指标
	collector.Update(1024, 10)
	collector.Update(2048, 20)

	bytes, rows = collector.GetTotalProcessed()
	if bytes != 3072 || rows != 30 {
		t.Errorf("Expected 3072 bytes and 30 rows, got %d bytes and %d rows", bytes, rows)
	}

	// 等待一小段时间以测试速率计算
	time.Sleep(10 * time.Millisecond)

	avgBytesPerSec, avgRowsPerSec := collector.GetAverageRate()
	if avgBytesPerSec <= 0 || avgRowsPerSec <= 0 {
		t.Error("Average rates should be positive")
	}

	// 测试重置
	collector.Reset()
	bytes, rows = collector.GetTotalProcessed()
	if bytes != 0 || rows != 0 {
		t.Error("Processed counts should be zero after reset")
	}
}

func TestStringConversions(t *testing.T) {
	// 测试 StringToInt64
	tests := []struct {
		input    string
		expected int64
		hasError bool
	}{
		{"123", 123, false},
		{"0", 0, false},
		{"-456", -456, false},
		{"", 0, false},
		{"abc", 0, true},
		{"123.45", 0, true},
	}

	for _, tt := range tests {
		result, err := StringToInt64(tt.input)
		if tt.hasError && err == nil {
			t.Errorf("StringToInt64(%s) should return error", tt.input)
		}
		if !tt.hasError && err != nil {
			t.Errorf("StringToInt64(%s) should not return error: %v", tt.input, err)
		}
		if !tt.hasError && result != tt.expected {
			t.Errorf("StringToInt64(%s) = %d, expected %d", tt.input, result, tt.expected)
		}
	}

	// 测试 StringToFloat64
	floatTests := []struct {
		input    string
		expected float64
		hasError bool
	}{
		{"123.45", 123.45, false},
		{"0", 0.0, false},
		{"-456.78", -456.78, false},
		{"", 0.0, false},
		{"abc", 0.0, true},
	}

	for _, tt := range floatTests {
		result, err := StringToFloat64(tt.input)
		if tt.hasError && err == nil {
			t.Errorf("StringToFloat64(%s) should return error", tt.input)
		}
		if !tt.hasError && err != nil {
			t.Errorf("StringToFloat64(%s) should not return error: %v", tt.input, err)
		}
		if !tt.hasError && result != tt.expected {
			t.Errorf("StringToFloat64(%s) = %f, expected %f", tt.input, result, tt.expected)
		}
	}
}

func TestMinMaxFunctions(t *testing.T) {
	// 测试 MinInt64
	if MinInt64(5, 3) != 3 {
		t.Error("MinInt64(5, 3) should return 3")
	}

	if MinInt64(-1, -5) != -5 {
		t.Error("MinInt64(-1, -5) should return -5")
	}

	// 测试 MaxInt64
	if MaxInt64(5, 3) != 5 {
		t.Error("MaxInt64(5, 3) should return 5")
	}

	if MaxInt64(-1, -5) != -1 {
		t.Error("MaxInt64(-1, -5) should return -1")
	}

	// 测试 MinInt
	if MinInt(10, 8) != 8 {
		t.Error("MinInt(10, 8) should return 8")
	}

	// 测试 MaxInt
	if MaxInt(10, 8) != 10 {
		t.Error("MaxInt(10, 8) should return 10")
	}
}

func TestMathUtilities(t *testing.T) {
	// 测试 RoundToInt64
	if RoundToInt64(3.4) != 3 {
		t.Error("RoundToInt64(3.4) should return 3")
	}

	if RoundToInt64(3.6) != 4 {
		t.Error("RoundToInt64(3.6) should return 4")
	}

	if RoundToInt64(-3.4) != -3 {
		t.Error("RoundToInt64(-3.4) should return -3")
	}

	// 测试 IsPowerOfTwo
	if !IsPowerOfTwo(1) {
		t.Error("1 should be power of two")
	}

	if !IsPowerOfTwo(8) {
		t.Error("8 should be power of two")
	}

	if IsPowerOfTwo(10) {
		t.Error("10 should not be power of two")
	}

	if IsPowerOfTwo(0) {
		t.Error("0 should not be power of two")
	}

	// 测试 NextPowerOfTwo
	if NextPowerOfTwo(1) != 2 {
		t.Error("NextPowerOfTwo(1) should return 2")
	}

	if NextPowerOfTwo(8) != 8 {
		t.Error("NextPowerOfTwo(8) should return 8")
	}

	if NextPowerOfTwo(10) != 16 {
		t.Error("NextPowerOfTwo(10) should return 16")
	}
}

func TestWaitWithTimeout(t *testing.T) {
	ctx := context.Background()

	// 测试成功情况
	err := WaitWithTimeout(ctx, time.Second, func() error {
		time.Sleep(100 * time.Millisecond)
		return nil
	})

	if err != nil {
		t.Errorf("WaitWithTimeout should succeed: %v", err)
	}

	// 测试超时情况
	err = WaitWithTimeout(ctx, 100*time.Millisecond, func() error {
		time.Sleep(time.Second)
		return nil
	})

	if err == nil {
		t.Error("WaitWithTimeout should timeout")
	}

	// 测试操作错误
	expectedErr := fmt.Errorf("operation failed")
	err = WaitWithTimeout(ctx, time.Second, func() error {
		return expectedErr
	})

	if err != expectedErr {
		t.Errorf("Expected operation error, got %v", err)
	}
}