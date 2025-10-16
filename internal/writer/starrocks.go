package writer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/starrocks"
)

type StarRocksHTTPWriter struct {
	config *config.DataSourceConfig
	client *starrocks.HTTPClient
	logger *logrus.Logger
	buffer []interface{}
	table  string
	ctx    context.Context
}

func NewStarRocksHTTPWriter(cfg *config.DataSourceConfig, cli *starrocks.HTTPClient, logger *logrus.Logger) (*StarRocksHTTPWriter, error) {
	ret := &StarRocksHTTPWriter{
		config: cfg,
		logger: logger,
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
// P0 优化方案3：利用 Record.MarshalJSON() 避免二次序列化
// Reader端返回的Record已包含RawValue，MarshalJSON会直接写入JSON字节，无需解析
func (w *StarRocksHTTPWriter) Write(ctx context.Context, records interface{}) error {
	options := &starrocks.StreamLoadOptions{
		Database: w.config.Database,
		Table:    w.table,
		Format:   "json",
	}

	// P0 优化：json.Marshal会调用Record.MarshalJSON()
	// RawValue字段会被直接写入，避免二次JSON序列化
	data, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	// 打印HTTP请求body数据用于调试
	w.logger.Infof("stream write %d bytes data", len(data))
	// w.logger.Debugf("stream write data %+v", string(data)) // 数据量大时不适合打印全部内容，否则会卡死标准输出

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

func NewStarRocksMySQLWriter(cfg *config.DataSourceConfig, cli *starrocks.MySQLClient, logger *logrus.Logger) (*StarRocksMySQLWriter, error) {
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
