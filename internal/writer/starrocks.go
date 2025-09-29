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

func NewStarRocksHTTPWriter(cfg *config.DataSourceConfig) (*StarRocksHTTPWriter, error) {
	return &StarRocksHTTPWriter{
		config: cfg,
		buffer: make([]interface{}, 0),
		ctx:    context.Background(),
	}, nil
}

func (w *StarRocksHTTPWriter) SetClient(client *starrocks.HTTPClient) {
	w.client = client
}

func (w *StarRocksHTTPWriter) SetTable(table string) {
	w.table = table
}

// Write 写入一批记录到缓冲区
// records: 记录切片，每个记录为interface{}类型
func (w *StarRocksHTTPWriter) Write(ctx context.Context, records []interface{}) error {
	w.buffer = append(w.buffer, records...)
	return nil
}

func (w *StarRocksHTTPWriter) Flush(ctx context.Context) error {
	if len(w.buffer) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, record := range w.buffer {
		data, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("failed to marshal record: %w", err)
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	options := &starrocks.StreamLoadOptions{
		Database: w.config.Database,
		Table:    w.table,
		Format:   "json",
	}

	_, err := w.client.StreamLoad(ctx, options, buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to stream load: %w", err)
	}

	w.buffer = w.buffer[:0]
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

func NewStarRocksMySQLWriter(cfg *config.DataSourceConfig) (*StarRocksMySQLWriter, error) {
	return &StarRocksMySQLWriter{
		config: cfg,
		buffer: make([]interface{}, 0),
		ctx:    context.Background(),
	}, nil
}

func (w *StarRocksMySQLWriter) SetClient(client *starrocks.MySQLClient) {
	w.client = client
}

func (w *StarRocksMySQLWriter) SetTable(table string) {
	w.table = table
}

// Write 写入一批记录到缓冲区（MySQL Writer）
// records: 记录切片，每个记录为interface{}类型
func (w *StarRocksMySQLWriter) Write(ctx context.Context, records []interface{}) error {
	w.buffer = append(w.buffer, records...)
	return nil
}

func (w *StarRocksMySQLWriter) Flush(ctx context.Context) error {
	if len(w.buffer) == 0 {
		return nil
	}

	w.buffer = w.buffer[:0]
	return nil
}

func (w *StarRocksMySQLWriter) Close() error {
	return nil
}