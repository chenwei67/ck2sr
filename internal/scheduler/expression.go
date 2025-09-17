package scheduler

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// CronExpression Cron 表达式实现
type CronExpression struct {
	expression string
	fields     [5][]int // 分、时、日、月、周
}

// ParseCronExpression 解析 Cron 表达式
func ParseCronExpression(expression string) (*CronExpression, error) {
	parts := strings.Fields(strings.TrimSpace(expression))
	if len(parts) != 5 {
		return nil, fmt.Errorf("invalid cron expression: expected 5 fields, got %d", len(parts))
	}

	cron := &CronExpression{
		expression: expression,
	}

	// 解析各个字段
	fieldNames := []string{"minute", "hour", "day", "month", "weekday"}
	fieldRanges := [][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 6}}

	for i, part := range parts {
		values, err := parseField(part, fieldRanges[i][0], fieldRanges[i][1])
		if err != nil {
			return nil, fmt.Errorf("invalid %s field '%s': %w", fieldNames[i], part, err)
		}
		cron.fields[i] = values
	}

	return cron, nil
}

// parseField 解析单个字段
func parseField(field string, min, max int) ([]int, error) {
	var values []int

	// 处理 * 通配符
	if field == "*" {
		for i := min; i <= max; i++ {
			values = append(values, i)
		}
		return values, nil
	}

	// 处理逗号分隔的值
	parts := strings.Split(field, ",")
	for _, part := range parts {
		if strings.Contains(part, "/") {
			// 处理步长 (*/2, 0-10/2)
			stepValues, err := parseStepField(part, min, max)
			if err != nil {
				return nil, err
			}
			values = append(values, stepValues...)
		} else if strings.Contains(part, "-") {
			// 处理范围 (1-5)
			rangeValues, err := parseRangeField(part, min, max)
			if err != nil {
				return nil, err
			}
			values = append(values, rangeValues...)
		} else {
			// 处理单个值
			value, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid number: %s", part)
			}
			if value < min || value > max {
				return nil, fmt.Errorf("value %d out of range [%d, %d]", value, min, max)
			}
			values = append(values, value)
		}
	}

	// 去重和排序
	values = removeDuplicates(values)
	return values, nil
}

// parseStepField 解析步长字段
func parseStepField(field string, min, max int) ([]int, error) {
	parts := strings.Split(field, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid step expression: %s", field)
	}

	step, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid step value: %s", parts[1])
	}

	if step <= 0 {
		return nil, fmt.Errorf("step must be positive: %d", step)
	}

	var start, end int
	if parts[0] == "*" {
		start, end = min, max
	} else if strings.Contains(parts[0], "-") {
		rangeValues, err := parseRangeField(parts[0], min, max)
		if err != nil {
			return nil, err
		}
		start, end = rangeValues[0], rangeValues[len(rangeValues)-1]
	} else {
		start, err = strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid start value: %s", parts[0])
		}
		end = max
	}

	var values []int
	for i := start; i <= end; i += step {
		values = append(values, i)
	}

	return values, nil
}

// parseRangeField 解析范围字段
func parseRangeField(field string, min, max int) ([]int, error) {
	parts := strings.Split(field, "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid range expression: %s", field)
	}

	start, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, fmt.Errorf("invalid start value: %s", parts[0])
	}

	end, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil, fmt.Errorf("invalid end value: %s", parts[1])
	}

	if start < min || start > max {
		return nil, fmt.Errorf("start value %d out of range [%d, %d]", start, min, max)
	}

	if end < min || end > max {
		return nil, fmt.Errorf("end value %d out of range [%d, %d]", end, min, max)
	}

	if start > end {
		return nil, fmt.Errorf("start value %d greater than end value %d", start, end)
	}

	var values []int
	for i := start; i <= end; i++ {
		values = append(values, i)
	}

	return values, nil
}

// removeDuplicates 去除重复值并排序
func removeDuplicates(values []int) []int {
	if len(values) == 0 {
		return values
	}

	// 使用 map 去重
	unique := make(map[int]bool)
	for _, v := range values {
		unique[v] = true
	}

	// 转回切片并排序
	result := make([]int, 0, len(unique))
	for v := range unique {
		result = append(result, v)
	}

	// 简单排序
	for i := 0; i < len(result)-1; i++ {
		for j := i + 1; j < len(result); j++ {
			if result[i] > result[j] {
				result[i], result[j] = result[j], result[i]
			}
		}
	}

	return result
}

// Next 计算下次执行时间
func (c *CronExpression) Next(from time.Time) *time.Time {
	// 从下一分钟开始计算
	next := from.Truncate(time.Minute).Add(time.Minute)

	// 最多尝试 4 年
	maxAttempts := 4 * 365 * 24 * 60
	attempts := 0

	for attempts < maxAttempts {
		if c.matches(next) {
			return &next
		}

		next = next.Add(time.Minute)
		attempts++
	}

	return nil // 找不到匹配的时间
}

// matches 检查时间是否匹配 Cron 表达式
func (c *CronExpression) matches(t time.Time) bool {
	minute := t.Minute()
	hour := t.Hour()
	day := t.Day()
	month := int(t.Month())
	weekday := int(t.Weekday())

	return contains(c.fields[0], minute) &&
		contains(c.fields[1], hour) &&
		contains(c.fields[2], day) &&
		contains(c.fields[3], month) &&
		contains(c.fields[4], weekday)
}

// contains 检查切片是否包含指定值
func contains(slice []int, value int) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

// IsValid 检查表达式是否有效
func (c *CronExpression) IsValid() bool {
	return c.expression != ""
}

// String 返回表达式字符串
func (c *CronExpression) String() string {
	return c.expression
}

// IntervalExpression 间隔表达式实现
type IntervalExpression struct {
	interval time.Duration
}

// ParseIntervalExpression 解析间隔表达式
func ParseIntervalExpression(expression string) (*IntervalExpression, error) {
	duration, err := time.ParseDuration(expression)
	if err != nil {
		return nil, fmt.Errorf("invalid interval expression: %w", err)
	}

	if duration <= 0 {
		return nil, fmt.Errorf("interval must be positive: %v", duration)
	}

	return &IntervalExpression{
		interval: duration,
	}, nil
}

// Next 计算下次执行时间
func (i *IntervalExpression) Next(from time.Time) *time.Time {
	next := from.Add(i.interval)
	return &next
}

// IsValid 检查表达式是否有效
func (i *IntervalExpression) IsValid() bool {
	return i.interval > 0
}

// String 返回表达式字符串
func (i *IntervalExpression) String() string {
	return i.interval.String()
}

// OneTimeExpression 一次性表达式实现
type OneTimeExpression struct {
	executeTime time.Time
	executed    bool
}

// ParseOneTimeExpression 解析一次性表达式
func ParseOneTimeExpression(expression string) (*OneTimeExpression, error) {
	executeTime, err := time.Parse("2006-01-02 15:04:05", expression)
	if err != nil {
		// 尝试其他格式
		executeTime, err = time.Parse(time.RFC3339, expression)
		if err != nil {
			return nil, fmt.Errorf("invalid datetime format: %s", expression)
		}
	}

	return &OneTimeExpression{
		executeTime: executeTime,
		executed:    false,
	}, nil
}

// Next 计算下次执行时间
func (o *OneTimeExpression) Next(from time.Time) *time.Time {
	if o.executed || from.After(o.executeTime) {
		return nil // 已执行或已过期
	}

	return &o.executeTime
}

// IsValid 检查表达式是否有效
func (o *OneTimeExpression) IsValid() bool {
	return !o.executeTime.IsZero()
}

// String 返回表达式字符串
func (o *OneTimeExpression) String() string {
	return o.executeTime.Format("2006-01-02 15:04:05")
}

// MarkExecuted 标记为已执行
func (o *OneTimeExpression) MarkExecuted() {
	o.executed = true
}

// ScheduleParserImpl 调度表达式解析器实现
type ScheduleParserImpl struct{}

// NewScheduleParser 创建调度表达式解析器
func NewScheduleParser() *ScheduleParserImpl {
	return &ScheduleParserImpl{}
}

// Parse 解析调度表达式
func (p *ScheduleParserImpl) Parse(expression string, scheduleType ScheduleType) (ScheduleExpression, error) {
	switch scheduleType {
	case ScheduleTypeCron:
		return ParseCronExpression(expression)
	case ScheduleTypeInterval:
		return ParseIntervalExpression(expression)
	case ScheduleTypeOneTime:
		return ParseOneTimeExpression(expression)
	case ScheduleTypeImmediate:
		// 立即执行，返回当前时间
		return &OneTimeExpression{
			executeTime: time.Now(),
			executed:    false,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported schedule type: %s", scheduleType)
	}
}

// Validate 验证调度表达式
func (p *ScheduleParserImpl) Validate(expression string, scheduleType ScheduleType) error {
	_, err := p.Parse(expression, scheduleType)
	return err
}

// ValidateCronExpression 验证 Cron 表达式格式
func ValidateCronExpression(expression string) error {
	// 基本格式检查
	cronPattern := `^(\*|[0-9,\-/]+)\s+(\*|[0-9,\-/]+)\s+(\*|[0-9,\-/]+)\s+(\*|[0-9,\-/]+)\s+(\*|[0-9,\-/]+)$`
	matched, err := regexp.MatchString(cronPattern, expression)
	if err != nil {
		return fmt.Errorf("regex error: %w", err)
	}

	if !matched {
		return fmt.Errorf("invalid cron expression format")
	}

	// 尝试解析
	_, err = ParseCronExpression(expression)
	return err
}

// ValidateIntervalExpression 验证间隔表达式格式
func ValidateIntervalExpression(expression string) error {
	_, err := time.ParseDuration(expression)
	return err
}

// CommonCronExpressions 常用 Cron 表达式
var CommonCronExpressions = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@hourly":   "0 * * * *",
}

// ExpandMacro 展开 Cron 宏
func ExpandMacro(expression string) string {
	if expanded, exists := CommonCronExpressions[expression]; exists {
		return expanded
	}
	return expression
}