package starrocks

import (
	"time"
)

// Config StarRocks 配置
type Config struct {
	Name      string
	MySQL     *MySQLConfig
	FlightSQL *FlightSQLConfig
	HTTP      *HTTPConfig
}

// MySQLConfig MySQL 协议配置
type MySQLConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Timeout  time.Duration
}

// FlightSQLConfig FlightSQL 协议配置
type FlightSQLConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Timeout  time.Duration
	TLS      TLSConfig
}

// TLSConfig TLS 配置
type TLSConfig struct {
	Enabled    bool
	CertFile   string
	KeyFile    string
	CAFile     string
	SkipVerify bool
}

// HTTPConfig HTTP 协议配置
type HTTPConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	Timeout  time.Duration
}

// StreamLoadOptions Stream Load 选项
type StreamLoadOptions struct {
	Database   string
	Table      string
	Label      string
	Format     string // JSON, CSV
	MaxBytes   int64
	Timeout    time.Duration
	Properties map[string]string
}