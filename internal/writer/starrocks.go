package writer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/starrocks"
)

type StarRocksHTTPWriter struct {
	config *config.DataSourceConfig
	client *starrocks.HTTPClient
	buffer []interface{}
	table  string
	ctx    context.Context
}

func NewStarRocksHTTPWriter(cfg *config.DataSourceConfig, cli *starrocks.HTTPClient) (*StarRocksHTTPWriter, error) {
	ret := &StarRocksHTTPWriter{
		config: cfg,
		buffer: make([]interface{}, 0),
		ctx:    context.Background(),
	}
	ret.SetClient(cli)
	return ret, nil
}

func (w *StarRocksHTTPWriter) SetClient(client *starrocks.HTTPClient) {
	w.client = client
}

func (w *StarRocksHTTPWriter) SetTable(table string) ExecutableWriter {
	w.table = table
	return w
}

// Write 写入一批记录到缓冲区
// records: 记录切片，每个记录为interface{}类型
func (w *StarRocksHTTPWriter) Write(ctx context.Context, records interface{}) error {
	options := &starrocks.StreamLoadOptions{
		Database: w.config.Database,
		Table:    w.table,
		Format:   "json",
	}

	data, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	buf := bytes.NewBuffer(data)
	_, err = w.client.StreamLoad(ctx, options, buf)
	if err != nil {
		return fmt.Errorf("failed to stream load: %w", err)
	}

	return nil
}

func (w *StarRocksHTTPWriter) Flush(ctx context.Context) error {
	// StarRocks HTTP Writer不需要显式刷新，Writer接口直接发送数据
	return nil
}

func (w *StarRocksHTTPWriter) Close() error {
	return nil
}

type StarRocksMySQLWriter struct {
	config *config.DataSourceConfig
	client *starrocks.MySQLClient
	buffer []interface{}
	table  string
	ctx    context.Context
}

func NewStarRocksMySQLWriter(cfg *config.DataSourceConfig, cli *starrocks.MySQLClient) (*StarRocksMySQLWriter, error) {
	ret := &StarRocksMySQLWriter{
		config: cfg,
		buffer: make([]interface{}, 0),
		ctx:    context.Background(),
	}
	ret.SetClient(cli)
	return ret, nil
}

func (w *StarRocksMySQLWriter) SetClient(client *starrocks.MySQLClient) {
	w.client = client
}

func (w *StarRocksMySQLWriter) SetTable(table string) ExecutableWriter {
	w.table = table
	return w
}

// Write 写入一批记录到缓冲区（MySQL Writer）
// records: 记录切片，每个记录为interface{}类型
func (w *StarRocksMySQLWriter) Write(ctx context.Context, records interface{}) error {
	// todo: 实现MySQL写入逻辑
	return nil
}

func (w *StarRocksMySQLWriter) Flush(ctx context.Context) error {
	// MySQL Writer不需要显式刷新，Writer接口直接发送数据
	return nil
}

func (w *StarRocksMySQLWriter) Close() error {
	return nil
}
