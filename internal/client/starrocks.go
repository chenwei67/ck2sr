package client

import (
	"fmt"

	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/starrocks"
)

type StarRocksCliMgr struct {
	cli map[string]*starrocks.Client
}

func NewStarRocksClientMgr(config []config.StarRocksConfig) (*StarRocksCliMgr, error) {
	ret := &StarRocksCliMgr{
		cli: make(map[string]*starrocks.Client),
	}

	for _, cfg := range config {
		if _, exists := ret.cli[cfg.Name]; exists {
			return nil, fmt.Errorf("duplicate starrocks client name: %s", cfg.Name)
		}

		c, err := generateStarRocksConfig(&cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to generate starrocks config %s: %w", cfg.Name, err)
		}

		cli, err := starrocks.NewClient(c)
		if err != nil {
			return nil, fmt.Errorf("failed to create starrocks client %s: %w", cfg.Name, err)
		}
		ret.cli[cfg.Name] = cli
	}

	return ret, nil
}

func generateStarRocksConfig(cfg *config.StarRocksConfig) (*starrocks.Config, error) {
	c := &starrocks.Config{}
	if cfg.MySQL != nil {
		c.MySQL = &starrocks.MySQLConfig{
			Host:     cfg.MySQL.Host,
			Port:     cfg.MySQL.Port,
			Username: cfg.MySQL.Username,
			Password: cfg.MySQL.Password,
		}
	}
	if cfg.HTTP != nil {
		c.HTTP = &starrocks.HTTPConfig{
			Host:     cfg.HTTP.Host,
			Port:     cfg.HTTP.Port,
			Username: cfg.HTTP.Username,
			Password: cfg.HTTP.Password,
		}
	}
	if cfg.FlightSQL != nil {
		c.FlightSQL = &starrocks.FlightSQLConfig{
			Host:     cfg.FlightSQL.Host,
			Port:     cfg.FlightSQL.Port,
			Username: cfg.FlightSQL.Username,
			Password: cfg.FlightSQL.Password,
			TLS: starrocks.TLSConfig{
				Enabled:    cfg.FlightSQL.TLS.Enabled,
				SkipVerify: cfg.FlightSQL.TLS.SkipVerify,
				CAFile:     cfg.FlightSQL.TLS.CAFile,
				CertFile:   cfg.FlightSQL.TLS.CertFile,
				KeyFile:    cfg.FlightSQL.TLS.KeyFile,
			},
		}
	}

	if c.MySQL == nil && c.HTTP == nil && c.FlightSQL == nil {
		return nil, fmt.Errorf("at least one protocol (MySQL or HTTP or FlightSQL) must be configured")
	}
	return c, nil
}

func (m *StarRocksCliMgr) GetMySQLClient(name string) (*starrocks.MySQLClient, error) {
	if item, ok := m.cli[name]; ok {
		return item.MySQL(), nil
	}
	return nil, fmt.Errorf("starrocks client %s not found", name)
}

func (m *StarRocksCliMgr) GetHTTPClient(name string) (*starrocks.HTTPClient, error) {
	if item, ok := m.cli[name]; ok {
		return item.HTTP(), nil
	}
	return nil, fmt.Errorf("starrocks client %s not found", name)
}

func (m *StarRocksCliMgr) GetFlightSQLClient(name string) (*starrocks.FlightSQLClient, error) {
	if item, ok := m.cli[name]; ok {
		return item.FlightSQL(), nil
	}
	return nil, fmt.Errorf("starrocks client %s not found", name)
}

func (m *StarRocksCliMgr) Close() {
	for _, cli := range m.cli {
		if cli != nil {
			cli.Close()
		}
	}
}
