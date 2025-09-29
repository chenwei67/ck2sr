package clickhouse

import (
	"context"
	"fmt"

	"github.com/sunkaimr/ck2sr/pkg/protocol"
)

// Client ClickHouse 统一客户端
type Client struct {
	config *Config
	mysql  *MySQLClient
	http   *HTTPClient
}

// NewClient 创建 ClickHouse 客户端
func NewClient(config *Config) (*Client, error) {
	client := &Client{
		config: config,
	}

	var err error

	// 初始化 MySQL 客户端（如果配置了）
	if config.MySQL != nil {
		client.mysql, err = NewMySQLClient(config.MySQL)
		if err != nil {
			return nil, fmt.Errorf("failed to create MySQL client: %w", err)
		}
	}

	// 初始化 HTTP 客户端（如果配置了）
	if config.HTTP != nil {
		client.http, err = NewHTTPClient(config.HTTP)
		if err != nil {
			// 如果 MySQL 客户端已创建，需要关闭
			if client.mysql != nil {
				client.mysql.Close()
			}
			return nil, fmt.Errorf("failed to create HTTP client: %w", err)
		}
	}

	// 至少需要一个协议客户端
	if client.mysql == nil && client.http == nil {
		return nil, fmt.Errorf("at least one protocol (MySQL or HTTP) must be configured")
	}

	return client, nil
}

// GetMySQLClient 获取 MySQL 客户端
func (c *Client) GetMySQLClient() *MySQLClient {
	return c.mysql
}

// GetHTTPClient 获取 HTTP 客户端
func (c *Client) GetHTTPClient() *HTTPClient {
	return c.http
}

// HasMySQL 是否有 MySQL 协议
func (c *Client) HasMySQL() bool {
	return c.mysql != nil
}

// HasHTTP 是否有 HTTP 协议
func (c *Client) HasHTTP() bool {
	return c.http != nil
}

// GetTableSchema 获取表结构（使用 MySQL 协议）
func (c *Client) GetTableSchema(ctx context.Context, database, table string) ([]protocol.ColumnInfo, error) {
	if c.mysql == nil {
		return nil, fmt.Errorf("MySQL protocol not configured")
	}
	return c.mysql.GetTableSchema(ctx, database, table)
}

// Count 统计行数（使用 MySQL 协议）
func (c *Client) Count(ctx context.Context, query string) (int64, error) {
	if c.mysql == nil {
		return 0, fmt.Errorf("MySQL protocol not configured")
	}
	return c.mysql.Count(ctx, query)
}

// Close 关闭所有连接
func (c *Client) Close() error {
	var errs []error

	if c.mysql != nil {
		if err := c.mysql.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close MySQL client: %w", err))
		}
	}

	if c.http != nil {
		if err := c.http.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close HTTP client: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing client: %v", errs)
	}

	return nil
}