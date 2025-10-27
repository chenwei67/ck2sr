package clickhouse

import (
	"context"
	"fmt"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	_ "github.com/go-sql-driver/mysql"
)

// MySQLClient MySQL 协议客户端
type MySQLClient struct {
	config *MySQLConfig
	db     driver.Conn
}

// NewMySQLClient 创建 MySQL 客户端
func NewMySQLClient(config *MySQLConfig) (*MySQLClient, error) {
	options := &clickhousego.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: clickhousego.Auth{
			Username: config.Username,
			Password: config.Password,
		},
		DialTimeout:      config.Timeout,
		MaxOpenConns:     10,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Hour,
		ConnOpenStrategy: clickhousego.ConnOpenInOrder,
	}

	// 建立链接
	db, err := clickhousego.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to open CK SQL connection: %w", err)
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()

	if err := db.Ping(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &MySQLClient{
		config: config,
		db:     db,
	}, nil
}

// Count 执行计数查询
func (c *MySQLClient) Count(ctx context.Context, query string) (uint64, error) {
	var count uint64
	row := c.db.QueryRow(ctx, query)
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to execute count query: %w", err)
	}
	return count, nil
}

// Query 执行查询
func (c *MySQLClient) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	return c.db.Query(ctx, query, args)
}

// Execute 执行SQL语句
func (c *MySQLClient) Execute(ctx context.Context, query string) error {
	return c.db.Exec(ctx, query)
}

// Close 关闭连接
func (c *MySQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
