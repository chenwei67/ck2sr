package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
)

// RetryableFunc 可重试的函数类型
type RetryableFunc func() error

// Retry 根据策略配置执行带重试的操作
func Retry(ctx context.Context, cfg *config.RetryPolicyConfig, logger *logrus.Logger, operation string, fn RetryableFunc) error {
	if cfg.MaxAttempts <= 0 {
		// 不重试，直接执行一次
		return fn()
	}

	var lastErr error
	backoff := cfg.InitialBackoff

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		// 执行操作
		err := fn()
		if err == nil {
			// 成功，直接返回
			if attempt > 1 {
				logger.Infof("Operation '%s' succeeded after %d attempts", operation, attempt)
			}
			return nil
		}

		lastErr = err
		logger.Warnf("Operation '%s' failed (attempt %d/%d): %v", operation, attempt, cfg.MaxAttempts, err)

		// 如果还有重试机会，等待后重试
		if attempt < cfg.MaxAttempts {
			// 计算退避时间
			waitTime := backoff
			if cfg.Jitter {
				// 添加±25%的随机抖动
				jitter := time.Duration(float64(backoff) * (rand.Float64()*0.5 - 0.25))
				waitTime = backoff + jitter
			}

			// 确保不超过最大退避时间
			if waitTime > cfg.MaxBackoff {
				waitTime = cfg.MaxBackoff
			}

			logger.Infof("Retrying operation '%s' in %v...", operation, waitTime)

			// 等待，支持取消
			select {
			case <-ctx.Done():
				return fmt.Errorf("retry cancelled: %w", ctx.Err())
			case <-time.After(waitTime):
			}

			// 指数退避
			backoff = time.Duration(float64(backoff) * cfg.BackoffMultiplier)
			if backoff > cfg.MaxBackoff {
				backoff = cfg.MaxBackoff
			}
		}
	}

	return fmt.Errorf("operation '%s' failed after %d attempts: %w", operation, cfg.MaxAttempts, lastErr)
}

// CalculateBackoff 计算指数退避时间（供外部使用）
func CalculateBackoff(attempt int, cfg *config.RetryPolicyConfig) time.Duration {
	backoff := cfg.InitialBackoff
	multiplier := cfg.BackoffMultiplier

	// 计算指数退避
	backoffFloat := float64(backoff) * math.Pow(multiplier, float64(attempt-1))
	backoff = time.Duration(backoffFloat)

	// 添加抖动
	if cfg.Jitter {
		jitter := float64(backoff) * (rand.Float64()*0.5 - 0.25)
		backoff = time.Duration(float64(backoff) + jitter)
	}

	// 限制最大退避时间
	if backoff > cfg.MaxBackoff {
		backoff = cfg.MaxBackoff
	}

	return backoff
}
