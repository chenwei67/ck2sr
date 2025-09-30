package writer

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/clickhouse"
)

type ClickHouseHTTPWriter struct {
	config *config.DataSourceConfig
	client *clickhouse.HTTPClient
	buffer []interface{}
	table  string
	ctx    context.Context
}

func NewClickHouseHTTPWriter(cfg *config.DataSourceConfig, cli *clickhouse.HTTPClient) (*ClickHouseHTTPWriter, error) {
	ret := &ClickHouseHTTPWriter{
		config: cfg,
		buffer: make([]interface{}, 0),
		ctx:    context.Background(),
	}
	ret.SetClient(cli)
	return ret, nil
}

func (w *ClickHouseHTTPWriter) SetClient(client *clickhouse.HTTPClient) {
	w.client = client
}

func (w *ClickHouseHTTPWriter) SetTable(table string) ExecutableWriter {
	w.table = table
	return w
}

// Write 写入一批记录到缓冲区（ClickHouse HTTP Writer）
// records: 记录切片，每个记录为interface{}类型
func (w *ClickHouseHTTPWriter) Write(ctx context.Context, records interface{}) error {
	// todo: 实现ClickHouse HTTP写入逻辑
	return nil
}

func (w *ClickHouseHTTPWriter) Flush(ctx context.Context) error {
	// ClickHouse HTTP Writer不需要显式刷新，Writer接口直接发送数据
	return nil
}

func (w *ClickHouseHTTPWriter) Close() error {
	return nil
}

type ClickHouseMySQLWriter struct {
	config *config.DataSourceConfig
	client *clickhouse.MySQLClient
	buffer []interface{}
	table  string
	ctx    context.Context
}

func NewClickHouseMySQLWriter(cfg *config.DataSourceConfig, cli *clickhouse.MySQLClient) (*ClickHouseMySQLWriter, error) {
	ret := &ClickHouseMySQLWriter{
		config: cfg,
		buffer: make([]interface{}, 0),
		ctx:    context.Background(),
	}
	ret.SetClient(cli)
	return ret, nil
}

func (w *ClickHouseMySQLWriter) SetClient(client *clickhouse.MySQLClient) {
	w.client = client
}

func (w *ClickHouseMySQLWriter) SetTable(table string) ExecutableWriter {
	w.table = table
	return w
}

// Write 写入一批记录到缓冲区（ClickHouse MySQL Writer）
// records: 记录切片，每个记录为interface{}类型
func (w *ClickHouseMySQLWriter) Write(ctx context.Context, records interface{}) error {
	// todo: 实现ClickHouse MySQL写入逻辑
	return nil
}

func (w *ClickHouseMySQLWriter) Flush(ctx context.Context) error {
	// ClickHouse MySQL Writer不需要显式刷新，Writer接口直接发送数据
	return nil
}

func (w *ClickHouseMySQLWriter) Close() error {
	return nil
}

type MySQLWriter struct {
	config *config.DataSourceConfig
	db     *sql.DB
	buffer []interface{}
	table  string
	ctx    context.Context
}

func NewMySQLWriter(cfg *config.DataSourceConfig) (*MySQLWriter, error) {
	return &MySQLWriter{
		config: cfg,
		buffer: make([]interface{}, 0),
		ctx:    context.Background(),
	}, nil
}

func (w *MySQLWriter) Connect(dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open mysql connection: %w", err)
	}
	w.db = db
	return nil
}

func (w *MySQLWriter) SetTable(table string) {
	w.table = table
}

// Write 写入一批记录到缓冲区（MySQL Writer）
// records: 记录切片，每个记录为interface{}类型
func (w *MySQLWriter) Write(ctx context.Context, records []interface{}) error {
	w.buffer = append(w.buffer, records...)
	return nil
}

func (w *MySQLWriter) Flush(ctx context.Context) error {
	if len(w.buffer) == 0 {
		return nil
	}

	w.buffer = w.buffer[:0]
	return nil
}

func (w *MySQLWriter) Close() error {
	if w.db != nil {
		return w.db.Close()
	}
	return nil
}
