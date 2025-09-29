package utils

import (
	"context"
	"fmt"
	"time"
)

// RetryPolicy 重试策略
type RetryPolicy struct {
	MaxRetries    int
	InitialDelay  time.Duration
	MaxDelay      time.Duration
	BackoffFactor float64
}

// DefaultRetryPolicy 默认重试策略
var DefaultRetryPolicy = &RetryPolicy{
	MaxRetries:    3,
	InitialDelay:  2 * time.Second,
	MaxDelay:      60 * time.Second,
	BackoffFactor: 2.0,
}

// Execute 执行带重试的函数
func (p *RetryPolicy) Execute(ctx context.Context, fn func() error) error {
	var lastErr error
	delay := p.InitialDelay

	for attempt := 0; attempt <= p.MaxRetries; attempt++ {
		// 检查上下文是否已取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 执行函数
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		// 最后一次尝试失败，不再重试
		if attempt == p.MaxRetries {
			break
		}

		// 等待后重试
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		// 计算下次延迟（指数退避）
		delay = time.Duration(float64(delay) * p.BackoffFactor)
		if delay > p.MaxDelay {
			delay = p.MaxDelay
		}
	}

	return fmt.Errorf("retry failed after %d attempts: %w", p.MaxRetries+1, lastErr)
}

// ExecuteWithCallback 执行带重试和回调的函数
func (p *RetryPolicy) ExecuteWithCallback(
	ctx context.Context,
	fn func() error,
	onRetry func(attempt int, err error),
) error {
	var lastErr error
	delay := p.InitialDelay

	for attempt := 0; attempt <= p.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
			if onRetry != nil && attempt < p.MaxRetries {
				onRetry(attempt, err)
			}
		}

		if attempt == p.MaxRetries {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		delay = time.Duration(float64(delay) * p.BackoffFactor)
		if delay > p.MaxDelay {
			delay = p.MaxDelay
		}
	}

	return fmt.Errorf("retry failed after %d attempts: %w", p.MaxRetries+1, lastErr)
}