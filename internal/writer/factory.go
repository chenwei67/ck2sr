package writer

import (
	"fmt"

	"github.com/sunkaimr/ck2sr/internal/client"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/protocol"
)

// ExecutableWriter 可控制的写入器接口
type ExecutableWriter interface {
	protocol.DataWriter
	SetTable(table string) ExecutableWriter
}

type WriterFactory interface {
	Create(config *config.DataSourceConfig, ckCliMgr *client.ClickHouseCliMgr, srCliMgr *client.StarRocksCliMgr) (ExecutableWriter, error)
}

type DefaultWriterFactory struct{}

func NewWriterFactory() WriterFactory {
	return &DefaultWriterFactory{}
}

func (f *DefaultWriterFactory) Create(cfg *config.DataSourceConfig, ckCliMgr *client.ClickHouseCliMgr, srCliMgr *client.StarRocksCliMgr) (ExecutableWriter, error) {
	switch cfg.Vendor {
	case "starrocks":
		return f.createStarRocksWriter(cfg, srCliMgr)
	case "clickhouse":
		return f.createClickHouseWriter(cfg, ckCliMgr)
	default:
		return nil, fmt.Errorf("unsupported vendor: %s", cfg.Vendor)
	}
}

func (f *DefaultWriterFactory) createStarRocksWriter(cfg *config.DataSourceConfig, srCliMgr *client.StarRocksCliMgr) (ExecutableWriter, error) {
	switch cfg.Protocol {
	case "http":
		cli, err := srCliMgr.GetHTTPClient(cfg.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get StarRocks %s HTTP client: %w", cfg.Name, err)
		}
		return NewStarRocksHTTPWriter(cfg, cli)
	case "mysql":
		cli, err := srCliMgr.GetMySQLClient(cfg.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get StarRocks %s MySQL client: %w", cfg.Name, err)
		}
		return NewStarRocksMySQLWriter(cfg, cli)
	default:
		return nil, fmt.Errorf("unsupported starrocks protocol: %s", cfg.Protocol)
	}
}

func (f *DefaultWriterFactory) createClickHouseWriter(cfg *config.DataSourceConfig, ckCliMgr *client.ClickHouseCliMgr) (ExecutableWriter, error) {
	switch cfg.Protocol {
	case "http":
		cli, err := ckCliMgr.GetHTTPClient(cfg.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get ClickHouse %s HTTP client: %w", cfg.Name, err)
		}
		return NewClickHouseHTTPWriter(cfg, cli)
	case "mysql":
		cli, err := ckCliMgr.GetMySQLClient(cfg.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get ClickHouse %s MySQL client: %w", cfg.Name, err)
		}
		return NewClickHouseMySQLWriter(cfg, cli)
	default:
		return nil, fmt.Errorf("unsupported clickhouse protocol: %s", cfg.Protocol)
	}
}
