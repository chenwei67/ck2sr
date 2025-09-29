package clickhouse

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"

	"github.com/sunkaimr/ck2sr/pkg/protocol"
)

// MySQLClient MySQL 协议客户端
type MySQLClient struct {
	config *MySQLConfig
	db     *sql.DB
}

// NewMySQLClient 创建 MySQL 客户端
func NewMySQLClient(config *MySQLConfig) (*MySQLClient, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		config.Username, config.Password,
		config.Host, config.Port)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL connection: %w", err)
	}

	// 设置连接参数
	db.SetConnMaxLifetime(config.Timeout)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)

	// 测试连接
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &MySQLClient{
		config: config,
		db:     db,
	}, nil
}

// Query 执行查询
func (c *MySQLClient) Query(ctx context.Context, query string) (*sql.Rows, error) {
	return c.db.QueryContext(ctx, query)
}

// Execute 执行SQL语句
func (c *MySQLClient) Execute(ctx context.Context, query string) error {
	_, err := c.db.ExecContext(ctx, query)
	return err
}

// Count 统计行数
func (c *MySQLClient) Count(ctx context.Context, query string) (int64, error) {
	var count int64
	err := c.db.QueryRowContext(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to execute count query: %w", err)
	}
	return count, nil
}

// GetTableSchema 获取表结构
func (c *MySQLClient) GetTableSchema(ctx context.Context, database, table string) ([]protocol.ColumnInfo, error) {
	query := `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT,
	          CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_COMMENT
	          FROM INFORMATION_SCHEMA.COLUMNS
	          WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	          ORDER BY ORDINAL_POSITION`

	rows, err := c.db.QueryContext(ctx, query, database, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query table schema: %w", err)
	}
	defer rows.Close()

	var schema []protocol.ColumnInfo
	for rows.Next() {
		var col protocol.ColumnInfo
		var nullable string
		var defaultVal sql.NullString
		var comment sql.NullString
		var charLength sql.NullInt64
		var numPrecision sql.NullInt64
		var numScale sql.NullInt64

		err := rows.Scan(&col.Name, &col.DataType, &nullable, &defaultVal,
			&charLength, &numPrecision, &numScale, &comment)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column info: %w", err)
		}

		col.IsNullable = nullable == "YES"
		if defaultVal.Valid {
			col.DefaultValue = &defaultVal.String
		}
		if comment.Valid {
			col.Comment = comment.String
		}
		if charLength.Valid {
			col.CharLength = &charLength.Int64
		}
		if numPrecision.Valid {
			col.NumPrecision = &numPrecision.Int64
		}
		if numScale.Valid {
			col.NumScale = &numScale.Int64
		}

		schema = append(schema, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading table schema: %w", err)
	}

	return schema, nil
}

// Close 关闭连接
func (c *MySQLClient) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}