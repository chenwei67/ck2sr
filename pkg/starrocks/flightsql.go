package starrocks

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/sunkaimr/ck2sr/pkg/protocol"
)

type FlightSQLClient struct {
	config *FlightSQLConfig
	db     adbc.Database
	conn   adbc.Connection
}

func NewFlightSQLClient(config *FlightSQLConfig) (*FlightSQLClient, error) {
	var driver flightsql.Driver
	db, err := driver.NewDatabase(map[string]string{
		adbc.OptionKeyURI:      fmt.Sprintf("grpc+tcp://%s:%d", config.Host, config.Port),
		adbc.OptionKeyUsername: config.Username,
		adbc.OptionKeyPassword: config.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create flightsql database: %w", err)
	}

	conn, err := db.Open(context.Background())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to open flightsql connection: %w", err)
	}

	return &FlightSQLClient{
		config: config,
		db:     db,
		conn:   conn,
	}, nil
}

func NewFlightSQLClientWithTLS(config *FlightSQLConfig) (*FlightSQLClient, error) {
	tlsConfig, err := buildTLSConfig(&config.TLS)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}

	var driver flightsql.Driver
	options := map[string]string{
		adbc.OptionKeyUsername: config.Username,
		adbc.OptionKeyPassword: config.Password,
	}

	if config.TLS.Enabled {
		options[adbc.OptionKeyURI] = fmt.Sprintf("grpc+tls://%s:%d", config.Host, config.Port)
		if tlsConfig != nil {
			options["adbc.flight.sql.client_option.tls_skip_verify"] = fmt.Sprintf("%v", tlsConfig.InsecureSkipVerify)
		}
	} else {
		options[adbc.OptionKeyURI] = fmt.Sprintf("grpc+tcp://%s:%d", config.Host, config.Port)
	}

	db, err := driver.NewDatabase(options)
	if err != nil {
		return nil, fmt.Errorf("failed to create flightsql database: %w", err)
	}

	conn, err := db.Open(context.Background())
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to open flightsql connection: %w", err)
	}

	return &FlightSQLClient{
		config: config,
		db:     db,
		conn:   conn,
	}, nil
}

func buildTLSConfig(config *TLSConfig) (*tls.Config, error) {
	if !config.Enabled {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.SkipVerify,
	}

	if config.CAFile != "" {
		caCert, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to append CA cert")
		}
		tlsConfig.RootCAs = caCertPool
	}

	if config.CertFile != "" && config.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func (c *FlightSQLClient) Query(ctx context.Context, query string) (arrow.Record, error) {
	stmt, err := c.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return nil, fmt.Errorf("failed to set query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		return nil, fmt.Errorf("no records returned")
	}

	return reader.Record(), nil
}

// QueryRecords 执行SQL查询并返回记录列表
// 返回的记录类型为[]interface{}，每个元素为map[string]interface{}
func (c *FlightSQLClient) QueryRecords(ctx context.Context, query string) ([]interface{}, error) {
	stmt, err := c.conn.NewStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to create statement: %w", err)
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(query); err != nil {
		return nil, fmt.Errorf("failed to set query: %w", err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer reader.Release()

	var records []interface{}
	for reader.Next() {
		rec := reader.Record()
		for i := 0; i < int(rec.NumRows()); i++ {
			// 构建记录map
			record := make(map[string]interface{})
			for j := 0; j < int(rec.NumCols()); j++ {
				col := rec.Column(j)
				fieldName := rec.ColumnName(j)
				record[fieldName] = getValueFromColumn(col, i)
			}
			records = append(records, record)
		}
	}

	return records, nil
}

func getValueFromColumn(col arrow.Array, rowIdx int) interface{} {
	if col.IsNull(rowIdx) {
		return nil
	}

	switch arr := col.(type) {
	case *array.Int8:
		return arr.Value(rowIdx)
	case *array.Int16:
		return arr.Value(rowIdx)
	case *array.Int32:
		return arr.Value(rowIdx)
	case *array.Int64:
		return arr.Value(rowIdx)
	case *array.Uint8:
		return arr.Value(rowIdx)
	case *array.Uint16:
		return arr.Value(rowIdx)
	case *array.Uint32:
		return arr.Value(rowIdx)
	case *array.Uint64:
		return arr.Value(rowIdx)
	case *array.Float32:
		return arr.Value(rowIdx)
	case *array.Float64:
		return arr.Value(rowIdx)
	case *array.String:
		return arr.Value(rowIdx)
	case *array.Boolean:
		return arr.Value(rowIdx)
	case *array.Date32:
		return arr.Value(rowIdx)
	case *array.Date64:
		return arr.Value(rowIdx)
	case *array.Timestamp:
		return arr.Value(rowIdx)
	default:
		return fmt.Sprintf("%v", col)
	}
}

func (c *FlightSQLClient) GetTableSchema(ctx context.Context, database, table string) ([]protocol.ColumnInfo, error) {
	query := fmt.Sprintf(`
		SELECT
			COLUMN_NAME,
			DATA_TYPE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			COLUMN_COMMENT,
			CHARACTER_MAXIMUM_LENGTH,
			NUMERIC_PRECISION,
			NUMERIC_SCALE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'
		ORDER BY ORDINAL_POSITION
	`, database, table)

	records, err := c.QueryRecords(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table schema: %w", err)
	}

	var columns []protocol.ColumnInfo
	for _, record := range records {
		// 类型断言：将interface{}转换为map[string]interface{}
		recordMap, ok := record.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("record is not a map[string]interface{}")
		}

		col := protocol.ColumnInfo{
			Name:       recordMap["COLUMN_NAME"].(string),
			DataType:   recordMap["DATA_TYPE"].(string),
			IsNullable: recordMap["IS_NULLABLE"].(string) == "YES",
		}
		if v, ok := recordMap["COLUMN_DEFAULT"]; ok && v != nil {
			s := v.(string)
			col.DefaultValue = &s
		}
		if v, ok := recordMap["COLUMN_COMMENT"]; ok && v != nil {
			col.Comment = v.(string)
		}
		if v, ok := recordMap["CHARACTER_MAXIMUM_LENGTH"]; ok && v != nil {
			i := v.(int64)
			col.CharLength = &i
		}
		if v, ok := recordMap["NUMERIC_PRECISION"]; ok && v != nil {
			i := v.(int64)
			col.NumPrecision = &i
		}
		if v, ok := recordMap["NUMERIC_SCALE"]; ok && v != nil {
			i := v.(int64)
			col.NumScale = &i
		}
		columns = append(columns, col)
	}

	return columns, nil
}

func (c *FlightSQLClient) Count(ctx context.Context, query string) (int64, error) {
	records, err := c.QueryRecords(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("failed to query count: %w", err)
	}

	if len(records) == 0 {
		return 0, nil
	}

	// 类型断言：将第一条记录转换为map[string]interface{}
	firstRecord, ok := records[0].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("first record is not a map[string]interface{}")
	}

	for _, v := range firstRecord {
		if count, ok := v.(int64); ok {
			return count, nil
		}
	}

	return 0, fmt.Errorf("failed to extract count from result")
}

func (c *FlightSQLClient) Close() error {
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return err
		}
	}
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}