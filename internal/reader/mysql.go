package reader

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sunkaimr/ck2sr/internal/config"
)

type MySQLReader struct {
	config *config.DataSourceConfig
	db     *sql.DB
	rows   *sql.Rows
	ctx    context.Context
}

func NewMySQLReader(cfg *config.DataSourceConfig) (*MySQLReader, error) {
	return &MySQLReader{
		config: cfg,
		ctx:    context.Background(),
	}, nil
}

func (r *MySQLReader) Connect(dsn string) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("failed to open mysql connection: %w", err)
	}
	r.db = db
	return nil
}

func (r *MySQLReader) Execute(ctx context.Context, query string) error {
	r.ctx = ctx
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	r.rows = rows
	return nil
}

func (r *MySQLReader) Next() bool {
	if r.rows == nil {
		return false
	}
	return r.rows.Next()
}

// GetRecord 获取当前记录
// 返回interface{}类型的记录数据，通常为map[string]interface{}
func (r *MySQLReader) GetRecord() (interface{}, error) {
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

func (r *MySQLReader) Close() error {
	if r.rows != nil {
		if err := r.rows.Close(); err != nil {
			return err
		}
	}
	if r.db != nil {
		return r.db.Close()
	}
	return nil
}