package clickhouse

import (
	"time"
)

// Config ClickHouse 配置
type Config struct {
	Name  string
	MySQL *MySQLConfig
	HTTP  *HTTPConfig
}

// MySQLConfig MySQL 协议配置
type MySQLConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Timeout  time.Duration
}

// HTTPConfig HTTP 协议配置
type HTTPConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Timeout  time.Duration
}

// QueryOptions 查询选项
type QueryOptions struct {
	Format    string // 查询结果格式 (JSON, TabSeparated, etc.)
	MaxRows   int    // 最大返回行数
	Timeout   time.Duration
	Settings  map[string]string // ClickHouse settings
}