package writer

import (
	"context"
	"fmt"

	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/protocol"
)

type WriterFactory interface {
	Create(config *config.DataSourceConfig) (protocol.DataWriter, error)
}

type DefaultWriterFactory struct{}

func NewWriterFactory() WriterFactory {
	return &DefaultWriterFactory{}
}

func (f *DefaultWriterFactory) Create(cfg *config.DataSourceConfig) (protocol.DataWriter, error) {
	switch cfg.Vendor {
	case "starrocks":
		return f.createStarRocksWriter(cfg)
	case "clickhouse":
		return f.createClickHouseWriter(cfg)
	case "mysql":
		return f.createMySQLWriter(cfg)
	default:
		return nil, fmt.Errorf("unsupported vendor: %s", cfg.Vendor)
	}
}

func (f *DefaultWriterFactory) createStarRocksWriter(cfg *config.DataSourceConfig) (protocol.DataWriter, error) {
	switch cfg.Protocol {
	case "http":
		return NewStarRocksHTTPWriter(cfg)
	case "mysql":
		return NewStarRocksMySQLWriter(cfg)
	default:
		return nil, fmt.Errorf("unsupported starrocks protocol: %s", cfg.Protocol)
	}
}

func (f *DefaultWriterFactory) createClickHouseWriter(cfg *config.DataSourceConfig) (protocol.DataWriter, error) {
	switch cfg.Protocol {
	case "http":
		return NewClickHouseHTTPWriter(cfg)
	case "mysql":
		return NewClickHouseMySQLWriter(cfg)
	default:
		return nil, fmt.Errorf("unsupported clickhouse protocol: %s", cfg.Protocol)
	}
}

func (f *DefaultWriterFactory) createMySQLWriter(cfg *config.DataSourceConfig) (protocol.DataWriter, error) {
	return NewMySQLWriter(cfg)
}

type WriterAdapter struct {
	ctx    context.Context
	writer protocol.DataWriter
}

func NewWriterAdapter(ctx context.Context, writer protocol.DataWriter) *WriterAdapter {
	return &WriterAdapter{
		ctx:    ctx,
		writer: writer,
	}
}

// Write 写入记录
func (a *WriterAdapter) Write(ctx context.Context, records []interface{}) error {
	return a.writer.Write(ctx, records)
}

func (a *WriterAdapter) Flush(ctx context.Context) error {
	return a.writer.Flush(ctx)
}

func (a *WriterAdapter) Close() error {
	return a.writer.Close()
}