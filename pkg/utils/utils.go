package utils

import (
	"context"
	"crypto/md5"
	"fmt"
	"hash/crc32"
	"math"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// BytesToHumanReadable 将字节数转换为人类可读的格式
func BytesToHumanReadable(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// RateToHumanReadable 将速率转换为人类可读的格式
func RateToHumanReadable(bytesPerSecond float64) string {
	return fmt.Sprintf("%s/s", BytesToHumanReadable(int64(bytesPerSecond)))
}

// DurationToHumanReadable 将时间间隔转换为人类可读的格式
func DurationToHumanReadable(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1fm", d.Minutes())
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1fh", d.Hours())
	}
	return fmt.Sprintf("%.1fd", d.Hours()/24)
}

// EstimateRemainingTime 估算剩余时间
func EstimateRemainingTime(completed, total int64, elapsed time.Duration) time.Duration {
	if completed == 0 || completed >= total {
		return 0
	}

	avgRate := float64(completed) / elapsed.Seconds()
	remaining := total - completed
	return time.Duration(float64(remaining)/avgRate) * time.Second
}

// CalculateProgress 计算进度百分比
func CalculateProgress(completed, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(completed) / float64(total) * 100
}

// MD5Hash 计算字符串的 MD5 哈希值
func MD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return fmt.Sprintf("%x", hash)
}

// CRC32Hash 计算字符串的 CRC32 哈希值
func CRC32Hash(text string) uint32 {
	return crc32.ChecksumIEEE([]byte(text))
}

// Retry 重试机制
type RetryConfig struct {
	MaxRetries    int
	InitialDelay  time.Duration
	MaxDelay      time.Duration
	BackoffFactor float64
}

// Retry 执行重试逻辑
func Retry(ctx context.Context, config RetryConfig, operation func() error) error {
	var lastErr error
	delay := config.InitialDelay

	for i := 0; i <= config.MaxRetries; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}

			// 计算下次延迟时间（指数退避）
			delay = time.Duration(float64(delay) * config.BackoffFactor)
			if delay > config.MaxDelay {
				delay = config.MaxDelay
			}
		}

		if err := operation(); err != nil {
			lastErr = err
			continue
		}

		return nil
	}

	return fmt.Errorf("operation failed after %d retries: %w", config.MaxRetries, lastErr)
}

// RateLimiter 速率限制器
type RateLimiter struct {
	bytesLimiter *rate.Limiter
	rowsLimiter  *rate.Limiter
}

// NewRateLimiter 创建新的速率限制器
func NewRateLimiter(bytesPerSecond, rowsPerSecond int, burstSize int) *RateLimiter {
	var bytesLimiter, rowsLimiter *rate.Limiter

	if bytesPerSecond > 0 {
		bytesLimiter = rate.NewLimiter(rate.Limit(bytesPerSecond), burstSize)
	}

	if rowsPerSecond > 0 {
		rowsLimiter = rate.NewLimiter(rate.Limit(rowsPerSecond), burstSize)
	}

	return &RateLimiter{
		bytesLimiter: bytesLimiter,
		rowsLimiter:  rowsLimiter,
	}
}

// WaitForBytes 等待字节配额
func (rl *RateLimiter) WaitForBytes(ctx context.Context, bytes int) error {
	if rl.bytesLimiter == nil {
		return nil
	}
	return rl.bytesLimiter.WaitN(ctx, bytes)
}

// WaitForRows 等待行数配额
func (rl *RateLimiter) WaitForRows(ctx context.Context, rows int) error {
	if rl.rowsLimiter == nil {
		return nil
	}
	return rl.rowsLimiter.WaitN(ctx, rows)
}

// AllowBytes 检查是否允许消费指定字节数
func (rl *RateLimiter) AllowBytes(bytes int) bool {
	if rl.bytesLimiter == nil {
		return true
	}
	return rl.bytesLimiter.AllowN(time.Now(), bytes)
}

// AllowRows 检查是否允许消费指定行数
func (rl *RateLimiter) AllowRows(rows int) bool {
	if rl.rowsLimiter == nil {
		return true
	}
	return rl.rowsLimiter.AllowN(time.Now(), rows)
}

// MetricsCollector 指标收集器
type MetricsCollector struct {
	startTime       time.Time
	lastUpdateTime  time.Time
	totalBytes      int64
	totalRows       int64
	currentBytes    int64
	currentRows     int64
	lastBytes       int64
	lastRows        int64
}

// NewMetricsCollector 创建新的指标收集器
func NewMetricsCollector() *MetricsCollector {
	now := time.Now()
	return &MetricsCollector{
		startTime:      now,
		lastUpdateTime: now,
	}
}

// Update 更新指标
func (mc *MetricsCollector) Update(bytes, rows int64) {
	now := time.Now()
	mc.currentBytes += bytes
	mc.currentRows += rows
	mc.totalBytes += bytes
	mc.totalRows += rows
	mc.lastUpdateTime = now
}

// GetCurrentRate 获取当前速率
func (mc *MetricsCollector) GetCurrentRate() (bytesPerSecond, rowsPerSecond float64) {
	now := time.Now()
	elapsed := now.Sub(mc.lastUpdateTime).Seconds()
	if elapsed == 0 {
		return 0, 0
	}

	bytesPerSecond = float64(mc.currentBytes-mc.lastBytes) / elapsed
	rowsPerSecond = float64(mc.currentRows-mc.lastRows) / elapsed

	mc.lastBytes = mc.currentBytes
	mc.lastRows = mc.currentRows
	mc.lastUpdateTime = now

	return
}

// GetAverageRate 获取平均速率
func (mc *MetricsCollector) GetAverageRate() (bytesPerSecond, rowsPerSecond float64) {
	elapsed := time.Since(mc.startTime).Seconds()
	if elapsed == 0 {
		return 0, 0
	}

	bytesPerSecond = float64(mc.totalBytes) / elapsed
	rowsPerSecond = float64(mc.totalRows) / elapsed

	return
}

// GetElapsed 获取已经过的时间
func (mc *MetricsCollector) GetElapsed() time.Duration {
	return time.Since(mc.startTime)
}

// GetTotalProcessed 获取总处理量
func (mc *MetricsCollector) GetTotalProcessed() (bytes, rows int64) {
	return mc.totalBytes, mc.totalRows
}

// Reset 重置指标
func (mc *MetricsCollector) Reset() {
	now := time.Now()
	mc.startTime = now
	mc.lastUpdateTime = now
	mc.totalBytes = 0
	mc.totalRows = 0
	mc.currentBytes = 0
	mc.currentRows = 0
	mc.lastBytes = 0
	mc.lastRows = 0
}

// StringToInt64 安全地将字符串转换为 int64
func StringToInt64(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// StringToFloat64 安全地将字符串转换为 float64
func StringToFloat64(s string) (float64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseFloat(s, 64)
}

// MinInt64 返回两个 int64 中的较小值
func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// MaxInt64 返回两个 int64 中的较大值
func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// MinInt 返回两个 int 中的较小值
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MaxInt 返回两个 int 中的较大值
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// RoundToInt64 将 float64 四舍五入为 int64
func RoundToInt64(f float64) int64 {
	return int64(math.Round(f))
}

// IsPowerOfTwo 检查数字是否为 2 的幂
func IsPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}

// NextPowerOfTwo 获取下一个 2 的幂
func NextPowerOfTwo(n int) int {
	if n <= 1 {
		return 2
	}
	if IsPowerOfTwo(n) {
		return n
	}

	result := 1
	for result < n {
		result <<= 1
	}
	return result
}

// SafeClose 安全关闭 channel
func SafeClose(ch chan struct{}) {
	select {
	case <-ch:
		// channel 已经关闭
	default:
		close(ch)
	}
}

// WaitWithTimeout 在指定超时时间内等待操作完成
func WaitWithTimeout(ctx context.Context, timeout time.Duration, operation func() error) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- operation()
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}