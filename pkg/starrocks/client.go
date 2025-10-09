package starrocks

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/pkg/protocol"
)

type Client struct {
	config    *Config
	mysql     *MySQLClient
	flightsql *FlightSQLClient
	http      *HTTPClient
}

func NewClient(config *Config, logger *logrus.Logger) (*Client, error) {
	client := &Client{
		config: config,
	}

	var err error

	if config.MySQL != nil {
		client.mysql, err = NewMySQLClient(config.MySQL)
		if err != nil {
			return nil, fmt.Errorf("failed to create mysql client: %w", err)
		}
	}

	if config.FlightSQL != nil {
		if config.FlightSQL.TLS.Enabled {
			client.flightsql, err = NewFlightSQLClientWithTLS(config.FlightSQL)
		} else {
			client.flightsql, err = NewFlightSQLClient(config.FlightSQL)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to create flightsql client: %w", err)
		}
	}

	if config.HTTP != nil {
		client.http, err = NewHTTPClient(config.HTTP, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create http client: %w", err)
		}
	}

	return client, nil
}

func (c *Client) MySQL() *MySQLClient {
	return c.mysql
}

func (c *Client) FlightSQL() *FlightSQLClient {
	return c.flightsql
}

func (c *Client) HTTP() *HTTPClient {
	return c.http
}

func (c *Client) GetTableSchema(ctx context.Context, database, table string) ([]protocol.ColumnInfo, error) {
	if c.mysql != nil {
		return c.mysql.GetTableSchema(ctx, database, table)
	}

	if c.flightsql != nil {
		return c.flightsql.GetTableSchema(ctx, database, table)
	}

	return nil, fmt.Errorf("no available client for schema query")
}

func (c *Client) Count(ctx context.Context, query string) (int64, error) {
	if c.mysql != nil {
		return c.mysql.Count(ctx, query)
	}

	if c.flightsql != nil {
		return c.flightsql.Count(ctx, query)
	}

	return 0, fmt.Errorf("no available client for count query")
}

func (c *Client) Close() error {
	var errs []error

	if c.mysql != nil {
		if err := c.mysql.Close(); err != nil {
			errs = append(errs, fmt.Errorf("mysql close error: %w", err))
		}
	}

	if c.flightsql != nil {
		if err := c.flightsql.Close(); err != nil {
			errs = append(errs, fmt.Errorf("flightsql close error: %w", err))
		}
	}

	if c.http != nil {
		if err := c.http.Close(); err != nil {
			errs = append(errs, fmt.Errorf("http close error: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing clients: %v", errs)
	}

	return nil
}
