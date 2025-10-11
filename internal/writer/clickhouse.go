package writer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/clickhouse"
)

// ClickHouseHTTPWriter ClickHouse HTTP协议写入器
// 使用JSONEachRow格式通过HTTP接口写入数据
type ClickHouseHTTPWriter struct {
	config *config.DataSourceConfig
	client *clickhouse.HTTPClient
	logger *logrus.Logger
	buffer []interface{}
	table  string
	ctx    context.Context
}

func NewClickHouseHTTPWriter(cfg *config.DataSourceConfig, cli *clickhouse.HTTPClient, logger *logrus.Logger) (*ClickHouseHTTPWriter, error) {
	ret := &ClickHouseHTTPWriter{
		config: cfg,
		logger: logger,
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

// Write 写入一批记录（ClickHouse HTTP Writer）
// records: 记录切片，每个记录为interface{}类型（通常为map[string]interface{}）
// 使用JSONEachRow格式，每条记录转换为一行JSON对象
func (w *ClickHouseHTTPWriter) Write(ctx context.Context, records interface{}) error {
	// 将records转换为切片
	recordSlice, ok := records.([]interface{})
	if !ok {
		return fmt.Errorf("records must be a slice of interface{}")
	}

	if len(recordSlice) == 0 {
		w.logger.Debug("No records to write, skipping")
		return nil
	}

	// 构建JSONEachRow格式的数据（每行一个JSON对象）
	var buf bytes.Buffer
	for i, record := range recordSlice {
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("failed to marshal record %d: %w", i, err)
		}
		buf.Write(data)
		buf.WriteByte('\n') // JSONEachRow格式要求每行一个JSON对象
	}

	// 打印HTTP请求body数据用于调试
	w.logger.Debugf("HTTP insert %d records, %d bytes data to %s.%s",
		len(recordSlice), buf.Len(), w.config.Database, w.table)
	// w.logger.Debugf("HTTP insert data: %s", buf.String()) // 数据量大时不适合打印全部内容

	// 使用HTTP接口插入数据
	if err := w.client.Insert(ctx, w.config.Database, w.table, &buf); err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	return nil
}

func (w *ClickHouseHTTPWriter) Flush(ctx context.Context) error {
	// ClickHouse HTTP Writer不需要显式刷新，Write接口直接发送数据
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
