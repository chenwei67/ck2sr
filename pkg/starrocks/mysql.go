package starrocks

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sunkaimr/ck2sr/pkg/protocol"
)

type MySQLClient struct {
	config *MySQLConfig
	db     *sql.DB
}

func NewMySQLClient(config *MySQLConfig) (*MySQLClient, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=%s&readTimeout=%s&writeTimeout=%s",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Timeout,
		config.Timeout,
		config.Timeout,
	)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(config.Timeout)

	ctx, cancel := context.WithTimeout(context.Background(), config.Timeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping starrocks: %w", err)
	}

	return &MySQLClient{
		config: config,
		db:     db,
	}, nil
}

func (c *MySQLClient) Query(ctx context.Context, query string) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query)
}

func (c *MySQLClient) Exec(ctx context.Context, query string) (sql.Result, error) {
	return c.db.ExecContext(ctx, query)
}

func (c *MySQLClient) GetTableSchema(ctx context.Context, database, table string) ([]protocol.ColumnInfo, error) {
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

	rows, err := c.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query table schema: %w", err)
	}
	defer rows.Close()

	var columns []protocol.ColumnInfo
	for rows.Next() {
		var col protocol.ColumnInfo
		var isNullable string
		err := rows.Scan(
			&col.Name,
			&col.DataType,
			&isNullable,
			&col.DefaultValue,
			&col.Comment,
			&col.CharLength,
			&col.NumPrecision,
			&col.NumScale,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}
		col.IsNullable = (isNullable == "YES")
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return columns, nil
}

func (c *MySQLClient) Count(ctx context.Context, query string) (uint64, error) {
	row := c.db.QueryRowContext(ctx, query)
	var count uint64
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to scan count: %w", err)
	}
	return count, nil
}

func (c *MySQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
