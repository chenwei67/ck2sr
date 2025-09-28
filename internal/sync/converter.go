package sync

import (
	"fmt"
	"strings"

	"github.com/ck2sr/ck2sr/internal/config"
)

// DataConverter 数据转换器
type DataConverter struct {
	config *config.SyncTaskConfig
}

// NewDataConverter 创建数据转换器
func NewDataConverter(config *config.SyncTaskConfig) *DataConverter {
	return &DataConverter{
		config: config,
	}
}

// BuildSelectQuery 构建查询SQL
func (dc *DataConverter) BuildSelectQuery(database, table string) string {
	query := fmt.Sprintf("SELECT * FROM `%s`.`%s`", database, table)

	// 添加数据范围过滤
	if dc.config.Settings.DataRange.TimeColumn != "" {
		var conditions []string

		if dc.config.Settings.DataRange.StartTime != "" {
			conditions = append(conditions, fmt.Sprintf("`%s` >= '%s'",
				dc.config.Settings.DataRange.TimeColumn,
				dc.config.Settings.DataRange.StartTime))
		}

		if dc.config.Settings.DataRange.EndTime != "" {
			conditions = append(conditions, fmt.Sprintf("`%s` <= '%s'",
				dc.config.Settings.DataRange.TimeColumn,
				dc.config.Settings.DataRange.EndTime))
		}

		if len(conditions) > 0 {
			query += " WHERE " + strings.Join(conditions, " AND ")
		}
	}

	// 添加限制（用于测试或小批量处理）
	if dc.config.Settings.BatchSize > 0 {
		query += fmt.Sprintf(" LIMIT %d", dc.config.Settings.BatchSize)
	}

	return query
}
