package sync

import (
	"fmt"
	"strings"

	"github.com/sunkaimr/ck2sr/internal/config"
)

// DataConverter 数据转换器
type DataConverter struct{}

// NewDataConverter 创建数据转换器
func NewDataConverter() *DataConverter {
	return &DataConverter{}
}

// BuildCountQuery 构建计数查询SQL
func (dc *DataConverter) BuildCountQuery(dataSource *config.DataSourceConfig, table string, dataRange *config.DataRangeConfig) (string, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", dataSource.Database, table)

	if dataRange.TimeColumn != "" {
		var conditions []string

		if dataRange.StartTime != "" {
			conditions = append(conditions, fmt.Sprintf("`%s` >= '%s'",
				dataRange.TimeColumn, dataRange.StartTime))
		}

		if dataRange.EndTime != "" {
			conditions = append(conditions, fmt.Sprintf("`%s` <= '%s'",
				dataRange.TimeColumn, dataRange.EndTime))
		}

		if len(conditions) > 0 {
			query += " WHERE " + strings.Join(conditions, " AND ")
		}
	}

	return query, nil
}

// BuildSelectQuery 构建查询SQL
func (dc *DataConverter) BuildSelectQuery(dataSource *config.DataSourceConfig, table string, dataRange *config.DataRangeConfig) (string, error) {
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s`", dataSource.Database, table)

	if dataRange.TimeColumn != "" {
		var conditions []string

		if dataRange.StartTime != "" {
			conditions = append(conditions, fmt.Sprintf("`%s` >= '%s'",
				dataRange.TimeColumn, dataRange.StartTime))
		}

		if dataRange.EndTime != "" {
			conditions = append(conditions, fmt.Sprintf("`%s` <= '%s'",
				dataRange.TimeColumn, dataRange.EndTime))
		}

		if len(conditions) > 0 {
			query += " WHERE " + strings.Join(conditions, " AND ")
		}
	}

	return query, nil
}
