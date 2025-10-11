package reader

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/client"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/protocol"
)

// ExecutableReader 可执行的读取器接口
type ExecutableReader interface {
	protocol.DataReader
	SetColumnFilter(excludeColumns []string, fixedValues map[string]interface{}) ExecutableReader
	SetQuery(query string) ExecutableReader
	Execute(ctx context.Context) error
}

type ReaderFactory interface {
	CreateExecutable(config *config.DataSourceConfig, ckCliMgr *client.ClickHouseCliMgr, srCliMgr *client.StarRocksCliMgr, logger *logrus.Logger) (ExecutableReader, error)
}

type DefaultReaderFactory struct{}

func NewReaderFactory() ReaderFactory {
	return &DefaultReaderFactory{}
}

func (f *DefaultReaderFactory) Create(cfg *config.DataSourceConfig, ckCliMgr *client.ClickHouseCliMgr, srCliMgr *client.StarRocksCliMgr, logger *logrus.Logger) (ExecutableReader, error) {
	return f.CreateExecutable(cfg, ckCliMgr, srCliMgr, logger)
}

func (f *DefaultReaderFactory) CreateExecutable(cfg *config.DataSourceConfig, ckCliMgr *client.ClickHouseCliMgr, srCliMgr *client.StarRocksCliMgr, logger *logrus.Logger) (ExecutableReader, error) {
	switch cfg.Vendor {
	case "clickhouse":
		return f.createClickHouseReader(cfg, ckCliMgr, logger)
	case "starrocks":
		return f.createStarRocksReader(cfg, srCliMgr, logger)
	default:
		return nil, fmt.Errorf("unsupported vendor: %s", cfg.Vendor)
	}
}

func (f *DefaultReaderFactory) createClickHouseReader(cfg *config.DataSourceConfig, ckCliMgr *client.ClickHouseCliMgr, logger *logrus.Logger) (ExecutableReader, error) {
	switch cfg.Protocol {
	case "mysql":
		cli, err := ckCliMgr.GetMySQLClient(cfg.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get ClickHouse %s MySQL client: %w", cfg.Name, err)
		}
		return NewClickHouseMySQLReader(cfg, cli, logger)
	case "http":
		cli, err := ckCliMgr.GetHTTPClient(cfg.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get ClickHouse %s HTTP client: %w", cfg.Name, err)
		}
		return NewClickHouseHTTPReader(cfg, cli, logger)
	default:
		return nil, fmt.Errorf("unsupported clickhouse protocol: %s", cfg.Protocol)
	}
}

func (f *DefaultReaderFactory) createStarRocksReader(cfg *config.DataSourceConfig, srCliMgr *client.StarRocksCliMgr, logger *logrus.Logger) (ExecutableReader, error) {
	switch cfg.Protocol {
	case "mysql":
		cli, err := srCliMgr.GetMySQLClient(cfg.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get StarRocks %s MySQL client: %w", cfg.Name, err)
		}
		return NewStarRocksMySQLReader(cfg, cli, logger)
	case "flightsql":
		cli, err := srCliMgr.GetFlightSQLClient(cfg.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to get StarRocks %s FlightSQL client: %w", cfg.Name, err)
		}
		return NewStarRocksFlightSQLReader(cfg, cli, logger)
	default:
		return nil, fmt.Errorf("unsupported starrocks protocol: %s", cfg.Protocol)
	}
}
