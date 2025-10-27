package reader

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/protocol"
	"github.com/sunkaimr/ck2sr/pkg/retry"
)

// RetryableReader 带重试功能的Reader包装器
// 为ExecutableReader添加自动重试能力，支持配置的重试策略
type RetryableReader struct {
	underlying   ExecutableReader
	retryPolicy  *config.RetryPolicyConfig
	logger       *logrus.Logger
	currentQuery string
}

// NewRetryableReader 创建带重试功能的Reader
func NewRetryableReader(underlying ExecutableReader, retryPolicy *config.RetryPolicyConfig, logger *logrus.Logger) ExecutableReader {
	return &RetryableReader{
		underlying:  underlying,
		retryPolicy: retryPolicy,
		logger:      logger,
	}
}

// SetColumnFilter 设置列过滤配置（透传）
func (r *RetryableReader) SetColumnFilter(excludeColumns []string, fixedValues map[string]interface{}) ExecutableReader {
	r.underlying.SetColumnFilter(excludeColumns, fixedValues)
	return r
}

// SetQuery 设置查询语句（透传，记录查询用于日志）
func (r *RetryableReader) SetQuery(query string) ExecutableReader {
	r.currentQuery = query
	r.underlying.SetQuery(query)
	return r
}

// Execute 执行查询，支持重试
func (r *RetryableReader) Execute(ctx context.Context) error {
	operation := fmt.Sprintf("Reader.Execute [%s]", r.currentQuery)

	return retry.Retry(ctx, r.retryPolicy, r.logger, operation, func() error {
		return r.underlying.Execute(ctx)
	})
}

// Next 移动到下一条记录（不重试，因为这是迭代操作）
func (r *RetryableReader) Next() bool {
	return r.underlying.Next()
}

// GetRecord 获取当前记录，支持重试
func (r *RetryableReader) GetRecord() (interface{}, error) {
	var record interface{}
	var getErr error

	operation := "Reader.GetRecord"

	err := retry.Retry(context.Background(), r.retryPolicy, r.logger, operation, func() error {
		var err error
		record, err = r.underlying.GetRecord()
		getErr = err
		return err
	})

	if err != nil {
		return nil, err
	}

	return record, getErr
}

// ReleaseRecords 释放已经被使用完的记录（透传）
func (r *RetryableReader) ReleaseRecords(records []interface{}) {
	r.underlying.ReleaseRecords(records)
}

// Close 关闭读取器（不重试，清理操作）
func (r *RetryableReader) Close() error {
	return r.underlying.Close()
}

// WrapReaderWithRetry 为Reader包装重试功能
// 这是一个便捷函数，用于在创建Reader后立即包装重试功能
func WrapReaderWithRetry(reader ExecutableReader, policy *config.RetryPolicyConfig, logger *logrus.Logger) ExecutableReader {
	if policy == nil || policy.MaxAttempts <= 1 {
		// 如果没有配置重试策略或最大尝试次数<=1，直接返回原Reader
		return reader
	}
	return NewRetryableReader(reader, policy, logger)
}

// 确保RetryableReader实现了ExecutableReader接口
var _ ExecutableReader = (*RetryableReader)(nil)
var _ protocol.DataReader = (*RetryableReader)(nil)
