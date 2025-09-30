package reader

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/pkg/starrocks"
)

type StarRocksMySQLReader struct {
	config *config.DataSourceConfig
	client *starrocks.MySQLClient
	query  string
	rows   *sql.Rows
	ctx    context.Context
}

func NewStarRocksMySQLReader(cfg *config.DataSourceConfig, cli *starrocks.MySQLClient) (*StarRocksMySQLReader, error) {
	ret := &StarRocksMySQLReader{
		config: cfg,
		ctx:    context.Background(),
	}
	ret.SetClient(cli)

	return ret, nil
}

func (r *StarRocksMySQLReader) SetClient(client *starrocks.MySQLClient) ExecutableReader {
	r.client = client
	return r
}

func (r *StarRocksMySQLReader) SetColumnFilter(excludeColumns []string, fixedValues map[string]interface{}) ExecutableReader {
	// MySQL Reader暂不支持列过滤
	return r
}

func (r *StarRocksMySQLReader) SetQuery(query string) ExecutableReader {
	r.query = query
	return r
}

func (r *StarRocksMySQLReader) Execute(ctx context.Context) error {
	r.ctx = ctx
	rows, err := r.client.Query(ctx, r.query)
	if err != nil {
		return fmt.Errorf("failed to execute query(%s): %w", r.query, err)
	}
	r.rows = rows
	return nil
}

func (r *StarRocksMySQLReader) Next() bool {
	if r.rows == nil {
		return false
	}
	return r.rows.Next()
}

// GetRecord 获取当前记录
// 返回interface{}类型的记录数据，通常为map[string]interface{}
func (r *StarRocksMySQLReader) GetRecord() (interface{}, error) {
	if r.rows == nil {
		return nil, fmt.Errorf("no active rows")
	}

	columns, err := r.rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := r.rows.Scan(valuePtrs...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}

	// 构建记录map
	record := make(map[string]interface{})
	for i, col := range columns {
		record[col] = values[i]
	}

	return record, nil
}

func (r *StarRocksMySQLReader) Close() error {
	if r.rows != nil {
		return r.rows.Close()
	}
	return nil
}

type StarRocksFlightSQLReader struct {
	config  *config.DataSourceConfig
	client  *starrocks.FlightSQLClient
	query   string
	records []interface{}
	index   int
	ctx     context.Context
}

func NewStarRocksFlightSQLReader(cfg *config.DataSourceConfig, cli *starrocks.FlightSQLClient) (*StarRocksFlightSQLReader, error) {
	ret := &StarRocksFlightSQLReader{
		config: cfg,
		ctx:    context.Background(),
	}
	ret.SetClient(cli)
	return ret, nil
}

func (r *StarRocksFlightSQLReader) SetClient(client *starrocks.FlightSQLClient) ExecutableReader {
	r.client = client
	return r
}

func (r *StarRocksFlightSQLReader) SetColumnFilter(excludeColumns []string, fixedValues map[string]interface{}) ExecutableReader {
	// FlightSQL Reader暂不支持列过滤
	return r
}

func (r *StarRocksFlightSQLReader) SetQuery(query string) ExecutableReader {
	r.query = query
	return r
}

func (r *StarRocksFlightSQLReader) Execute(ctx context.Context) error {
	r.ctx = ctx
	records, err := r.client.QueryRecords(ctx, r.query)
	if err != nil {
		return fmt.Errorf("failed to execute query(%s): %w", r.query, err)
	}
	r.records = records
	r.index = 0
	return nil
}

func (r *StarRocksFlightSQLReader) Next() bool {
	return r.index < len(r.records)
}

// GetRecord 获取当前记录（FlightSQL Reader）
func (r *StarRocksFlightSQLReader) GetRecord() (interface{}, error) {
	if r.index >= len(r.records) {
		return nil, fmt.Errorf("no more records")
	}
	record := r.records[r.index]
	r.index++
	return record, nil
}

func (r *StarRocksFlightSQLReader) Close() error {
	r.records = nil
	return nil
}
