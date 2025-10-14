package client

import (
	"fmt"

	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/clickhouse"
)

type ClickHouseCliMgr struct {
	cli map[string]*clickhouse.Client
}

func NewClickHouseClientMgr(config []config.ClickHouseConfig) (*ClickHouseCliMgr, error) {
	ret := &ClickHouseCliMgr{
		cli: make(map[string]*clickhouse.Client),
	}

	for _, cfg := range config {
		if _, exists := ret.cli[cfg.Name]; exists {
			return nil, fmt.Errorf("duplicate clickhouse client name: %s", cfg.Name)
		}

		c, err := generateClickHouseConfig(&cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to generate clickhouse config %s: %w", cfg.Name, err)
		}

		cli, err := clickhouse.NewClient(c)
		if err != nil {
			return nil, fmt.Errorf("failed to create clickhouse client %s: %w", cfg.Name, err)
		}
		ret.cli[cfg.Name] = cli
	}

	return ret, nil
}

func generateClickHouseConfig(cfg *config.ClickHouseConfig) (*clickhouse.Config, error) {
	c := &clickhouse.Config{}
	if cfg.MySQL != nil {
		c.MySQL = &clickhouse.MySQLConfig{
			Host:     cfg.MySQL.Host,
			Port:     cfg.MySQL.Port,
			Username: cfg.MySQL.Username,
			Password: cfg.MySQL.Password,
			Timeout:  cfg.MySQL.Timeout,
		}
	}
	if cfg.HTTP != nil {
		c.HTTP = &clickhouse.HTTPConfig{
			Host:     cfg.HTTP.Host,
			Port:     cfg.HTTP.Port,
			Username: cfg.HTTP.Username,
			Password: cfg.HTTP.Password,
			Timeout:  cfg.HTTP.Timeout,
		}
	}
	if c.MySQL == nil && c.HTTP == nil {
		return nil, fmt.Errorf("at least one protocol (MySQL or HTTP) must be configured")
	}
	return c, nil
}

func (m *ClickHouseCliMgr) GetMySQLClient(name string) (*clickhouse.MySQLClient, error) {
	if item, ok := m.cli[name]; ok {
		return item.GetMySQLClient(), nil
	}
	return nil, fmt.Errorf("clickhouse client %s not found", name)
}

func (m *ClickHouseCliMgr) GetHTTPClient(name string) (*clickhouse.HTTPClient, error) {
	if item, ok := m.cli[name]; ok {
		return item.GetHTTPClient(), nil
	}
	return nil, fmt.Errorf("clickhouse client %s not found", name)
}

func (m *ClickHouseCliMgr) Close() {
	for _, cli := range m.cli {
		if cli != nil {
			cli.Close()
		}
	}
}
