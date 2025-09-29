package reader

import (
	"context"
	"fmt"

	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/protocol"
)

type ReaderFactory interface {
	Create(config *config.DataSourceConfig) (protocol.DataReader, error)
}

type DefaultReaderFactory struct{}

func NewReaderFactory() ReaderFactory {
	return &DefaultReaderFactory{}
}

func (f *DefaultReaderFactory) Create(cfg *config.DataSourceConfig) (protocol.DataReader, error) {
	switch cfg.Vendor {
	case "clickhouse":
		return f.createClickHouseReader(cfg)
	case "starrocks":
		return f.createStarRocksReader(cfg)
	default:
		return nil, fmt.Errorf("unsupported vendor: %s", cfg.Vendor)
	}
}

func (f *DefaultReaderFactory) createClickHouseReader(cfg *config.DataSourceConfig) (protocol.DataReader, error) {
	switch cfg.Protocol {
	case "mysql":
		return NewClickHouseMySQLReader(cfg)
	case "http":
		return NewClickHouseHTTPReader(cfg)
	default:
		return nil, fmt.Errorf("unsupported clickhouse protocol: %s", cfg.Protocol)
	}
}

func (f *DefaultReaderFactory) createStarRocksReader(cfg *config.DataSourceConfig) (protocol.DataReader, error) {
	switch cfg.Protocol {
	case "mysql":
		return NewStarRocksMySQLReader(cfg)
	case "flightsql":
		return NewStarRocksFlightSQLReader(cfg)
	default:
		return nil, fmt.Errorf("unsupported starrocks protocol: %s", cfg.Protocol)
	}
}


type ReaderAdapter struct {
	ctx    context.Context
	reader protocol.DataReader
	query  string
}

func NewReaderAdapter(ctx context.Context, reader protocol.DataReader, query string) *ReaderAdapter {
	return &ReaderAdapter{
		ctx:    ctx,
		reader: reader,
		query:  query,
	}
}

func (a *ReaderAdapter) Next() bool {
	return a.reader.Next()
}

// GetRecord 获取当前记录
func (a *ReaderAdapter) GetRecord() (interface{}, error) {
	return a.reader.GetRecord()
}

func (a *ReaderAdapter) Close() error {
	return a.reader.Close()
}