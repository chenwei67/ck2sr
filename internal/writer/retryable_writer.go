package writer

import (
	"context"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/protocol"
	"github.com/sunkaimr/ck2sr/pkg/retry"
)

// RetryableWriter 带重试功能的Writer包装器
// 为ExecutableWriter添加自动重试能力，支持配置的重试策略
type RetryableWriter struct {
	underlying  ExecutableWriter
	retryPolicy *config.RetryPolicyConfig
	logger      *logrus.Logger
	table       string
}

// NewRetryableWriter 创建带重试功能的Writer
func NewRetryableWriter(underlying ExecutableWriter, retryPolicy *config.RetryPolicyConfig, logger *logrus.Logger) ExecutableWriter {
	return &RetryableWriter{
		underlying:  underlying,
		retryPolicy: retryPolicy,
		logger:      logger,
	}
}

// SetTable 设置目标表名（透传）
func (w *RetryableWriter) SetTable(table string) ExecutableWriter {
	w.table = table
	w.underlying.SetTable(table)
	return w
}

// Write 写入数据，支持重试
func (w *RetryableWriter) Write(ctx context.Context, records interface{}) error {
	operation := "Writer.Write"

	return retry.Retry(ctx, w.retryPolicy, w.logger, operation, func() error {
		return w.underlying.Write(ctx, records)
	})
}

// Flush 刷新数据，支持重试
func (w *RetryableWriter) Flush(ctx context.Context) error {
	operation := "Writer.Flush"

	return retry.Retry(ctx, w.retryPolicy, w.logger, operation, func() error {
		return w.underlying.Flush(ctx)
	})
}

// Close 关闭写入器（不重试，清理操作）
func (w *RetryableWriter) Close() error {
	return w.underlying.Close()
}

// WrapWriterWithRetry 为Writer包装重试功能
// 这是一个便捷函数，用于在创建Writer后立即包装重试功能
func WrapWriterWithRetry(writer ExecutableWriter, policy *config.RetryPolicyConfig, logger *logrus.Logger) ExecutableWriter {
	if policy == nil || policy.MaxAttempts <= 1 {
		// 如果没有配置重试策略或最大尝试次数<=1，直接返回原Writer
		return writer
	}
	return NewRetryableWriter(writer, policy, logger)
}

// 确保RetryableWriter实现了ExecutableWriter接口
var _ ExecutableWriter = (*RetryableWriter)(nil)
var _ protocol.DataWriter = (*RetryableWriter)(nil)
