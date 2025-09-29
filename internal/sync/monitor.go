package sync

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// MonitorTableStats 监控表统计（与同步引擎的TableStats区分）
type MonitorTableStats struct {
	TableName   string
	StartTime   time.Time
	EndTime     time.Time
	TotalRows   int64
	TotalBytes  int64
	BatchCount  int64
	Status      string
	ErrorMsg    string
}

type Monitor struct {
	taskID      string
	logger      *logrus.Logger
	startTime   time.Time
	endTime     time.Time
	tables      map[string]*MonitorTableStats
	totalRows   int64
	totalBytes  int64
	mu          sync.RWMutex
}

func NewMonitor(taskID string, logger *logrus.Logger) *Monitor {
	return &Monitor{
		taskID: taskID,
		logger: logger,
		tables: make(map[string]*MonitorTableStats),
	}
}

func (m *Monitor) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startTime = time.Now()
	m.logger.Infof("Monitor started for task: %s", m.taskID)
}

func (m *Monitor) End() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.endTime = time.Now()
	duration := m.endTime.Sub(m.startTime)
	m.logger.Infof("Monitor ended for task: %s, duration: %v", m.taskID, duration)
	m.logSummary()
}

func (m *Monitor) StartTable(tableName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tables[tableName] = &MonitorTableStats{
		TableName: tableName,
		StartTime: time.Now(),
		Status:    "running",
	}
	m.logger.Debugf("Started monitoring table: %s", tableName)
}

func (m *Monitor) EndTable(tableName string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if stats, exists := m.tables[tableName]; exists {
		stats.EndTime = time.Now()
		if stats.Status == "running" {
			stats.Status = "completed"
		}
		duration := stats.EndTime.Sub(stats.StartTime)
		m.logger.Infof("Table %s completed: %d rows, %d bytes, duration: %v",
			tableName, stats.TotalRows, stats.TotalBytes, duration)
	}
}

func (m *Monitor) AddRows(tableName string, rows int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if stats, exists := m.tables[tableName]; exists {
		stats.TotalRows += rows
		stats.BatchCount++
	}
	m.totalRows += rows
}

func (m *Monitor) AddBytes(tableName string, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if stats, exists := m.tables[tableName]; exists {
		stats.TotalBytes += bytes
	}
	m.totalBytes += bytes
}

func (m *Monitor) SetTableError(tableName string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if stats, exists := m.tables[tableName]; exists {
		stats.Status = "failed"
		stats.ErrorMsg = err.Error()
		stats.EndTime = time.Now()
	}
	m.logger.Errorf("Table %s failed: %v", tableName, err)
}

func (m *Monitor) GetTableStats(tableName string) *MonitorTableStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if stats, exists := m.tables[tableName]; exists {
		return &MonitorTableStats{
			TableName:  stats.TableName,
			StartTime:  stats.StartTime,
			EndTime:    stats.EndTime,
			TotalRows:  stats.TotalRows,
			TotalBytes: stats.TotalBytes,
			BatchCount: stats.BatchCount,
			Status:     stats.Status,
			ErrorMsg:   stats.ErrorMsg,
		}
	}
	return nil
}

func (m *Monitor) GetAllTableStats() map[string]*MonitorTableStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*MonitorTableStats)
	for name, stats := range m.tables {
		result[name] = &MonitorTableStats{
			TableName:  stats.TableName,
			StartTime:  stats.StartTime,
			EndTime:    stats.EndTime,
			TotalRows:  stats.TotalRows,
			TotalBytes: stats.TotalBytes,
			BatchCount: stats.BatchCount,
			Status:     stats.Status,
			ErrorMsg:   stats.ErrorMsg,
		}
	}
	return result
}

func (m *Monitor) GetTotalRows() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.totalRows
}

func (m *Monitor) GetTotalBytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.totalBytes
}

func (m *Monitor) GetStartTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.startTime
}

func (m *Monitor) GetEndTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.endTime
}

func (m *Monitor) GetProcessedTables() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var tables []string
	for tableName := range m.tables {
		tables = append(tables, tableName)
	}
	return tables
}

func (m *Monitor) logSummary() {
	totalDuration := m.endTime.Sub(m.startTime)
	completedTables := 0
	failedTables := 0

	for _, stats := range m.tables {
		switch stats.Status {
		case "completed":
			completedTables++
		case "failed":
			failedTables++
		}
	}

	m.logger.Infof("=== Sync Summary for task %s ===", m.taskID)
	m.logger.Infof("Total duration: %v", totalDuration)
	m.logger.Infof("Total tables: %d (completed: %d, failed: %d)",
		len(m.tables), completedTables, failedTables)
	m.logger.Infof("Total rows: %d", m.totalRows)
	m.logger.Infof("Total bytes: %d", m.totalBytes)

	if totalDuration.Seconds() > 0 {
		rowsPerSec := float64(m.totalRows) / totalDuration.Seconds()
		bytesPerSec := float64(m.totalBytes) / totalDuration.Seconds()
		m.logger.Infof("Performance: %.2f rows/sec, %.2f bytes/sec", rowsPerSec, bytesPerSec)
	}

	for tableName, stats := range m.tables {
		if stats.Status == "failed" {
			m.logger.Errorf("Failed table %s: %s", tableName, stats.ErrorMsg)
		}
	}
}