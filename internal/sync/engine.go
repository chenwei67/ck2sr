package sync

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/internal/scheduler"
	"github.com/sunkaimr/ck2sr/internal/storage"
)

// SyncEngine 同步引擎 - 高级同步协调器
// 专注于跨表和跨库的同步协调，与单表同步作业的Scheduler区分
type SyncEngine struct {
	config    *config.Config
	policy    *config.PolicyConfig
	storage   storage.Storage
	logger    *logrus.Logger
	scheduler *scheduler.Scheduler // 关联的任务调度器
	running   bool
	mutex     sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc

	// 高级协调功能
	syncSessions map[string]*SyncSession // 正在运行的同步会话
	metrics      *SyncMetrics           // 同步指标
}

// SyncSession 同步会话 - 表示一次完整的数据同步操作
type SyncSession struct {
	SessionID   string                 `json:"session_id"`
	TaskID      string                 `json:"task_id"`
	StartTime   time.Time              `json:"start_time"`
	EndTime     time.Time              `json:"end_time"`
	Status      string                 `json:"status"` // running, completed, failed, cancelled
	Tables      map[string]*TableStats `json:"tables"`
	TotalRows   int64                  `json:"total_rows"`
	TotalBytes  int64                  `json:"total_bytes"`
	ErrorCount  int                    `json:"error_count"`
	LastError   string                 `json:"last_error,omitempty"`
}

// TableStats 表同步统计
type TableStats struct {
	TableName     string        `json:"table_name"`
	StartTime     time.Time     `json:"start_time"`
	EndTime       time.Time     `json:"end_time"`
	Duration      time.Duration `json:"duration"`
	ProcessedRows int64         `json:"processed_rows"`
	ProcessedBytes int64        `json:"processed_bytes"`
	Status        string        `json:"status"`
	Progress      float64       `json:"progress"`
}

// SyncMetrics 同步指标
type SyncMetrics struct {
	TotalSessions    int64     `json:"total_sessions"`
	ActiveSessions   int       `json:"active_sessions"`
	SuccessfulSyncs  int64     `json:"successful_syncs"`
	FailedSyncs      int64     `json:"failed_syncs"`
	TotalRowsSynced  int64     `json:"total_rows_synced"`
	TotalBytesSynced int64     `json:"total_bytes_synced"`
	AverageSpeed     float64   `json:"average_speed"`    // rows per second
	LastSyncTime     time.Time `json:"last_sync_time"`
	Uptime           time.Time `json:"uptime"`
}

// EngineStatus 引擎状态
type EngineStatus struct {
	Running        bool                    `json:"running"`
	ActiveSessions map[string]*SyncSession `json:"active_sessions"`
	Metrics        *SyncMetrics            `json:"metrics"`
	SchedulerStats interface{}             `json:"scheduler_stats,omitempty"`
}

// NewSyncEngine 创建新的同步引擎
func NewSyncEngine(cfg *config.Config, policy *config.PolicyConfig, store storage.Storage, logger *logrus.Logger) *SyncEngine {
	ctx, cancel := context.WithCancel(context.Background())

	return &SyncEngine{
		config:       cfg,
		policy:       policy,
		storage:      store,
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
		syncSessions: make(map[string]*SyncSession),
		metrics: &SyncMetrics{
			Uptime: time.Now(),
		},
	}
}

// SetScheduler 设置关联的任务调度器
func (e *SyncEngine) SetScheduler(scheduler *scheduler.Scheduler) {
	e.scheduler = scheduler
}

// Start 启动同步引擎
func (e *SyncEngine) Start() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if e.running {
		return fmt.Errorf("sync engine is already running")
	}

	e.running = true
	e.logger.Info("⚙️  SyncEngine started - High-level sync coordinator active")

	// 启动指标收集器
	go e.metricsCollector()

	return nil
}

// Stop 停止同步引擎
func (e *SyncEngine) Stop() error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	if !e.running {
		return nil
	}

	e.running = false
	e.cancel()

	// 取消所有活跃的同步会话
	for sessionID := range e.syncSessions {
		e.cancelSession(sessionID)
	}

	e.logger.Info("⏹️  SyncEngine stopped")
	return nil
}

// IsRunning 检查引擎是否运行中
func (e *SyncEngine) IsRunning() bool {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return e.running
}

// StartSyncSession 启动新的同步会话
func (e *SyncEngine) StartSyncSession(taskID string) (*SyncSession, error) {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// 生成会话 ID
	sessionID := fmt.Sprintf("%s_%d", taskID, time.Now().Unix())

	session := &SyncSession{
		SessionID: sessionID,
		TaskID:    taskID,
		StartTime: time.Now(),
		Status:    "running",
		Tables:    make(map[string]*TableStats),
	}

	e.syncSessions[sessionID] = session
	e.metrics.TotalSessions++
	e.metrics.ActiveSessions++

	e.logger.Infof("🚀 Started sync session %s for task %s", sessionID, taskID)
	return session, nil
}

// EndSyncSession 结束同步会话
func (e *SyncEngine) EndSyncSession(sessionID string, success bool, errorMsg string) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	session, exists := e.syncSessions[sessionID]
	if !exists {
		return fmt.Errorf("session %s not found", sessionID)
	}

	session.EndTime = time.Now()
	if success {
		session.Status = "completed"
		e.metrics.SuccessfulSyncs++
	} else {
		session.Status = "failed"
		session.LastError = errorMsg
		e.metrics.FailedSyncs++
	}

	// 更新指标
	e.metrics.TotalRowsSynced += session.TotalRows
	e.metrics.TotalBytesSynced += session.TotalBytes
	e.metrics.LastSyncTime = time.Now()
	e.metrics.ActiveSessions--

	// 从活跃会话中移除
	delete(e.syncSessions, sessionID)

	e.logger.Infof("✅ Ended sync session %s with status: %s", sessionID, session.Status)
	return nil
}

// UpdateTableProgress 更新表同步进度
func (e *SyncEngine) UpdateTableProgress(sessionID, tableName string, stats *TableStats) error {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	session, exists := e.syncSessions[sessionID]
	if !exists {
		return fmt.Errorf("session %s not found", sessionID)
	}

	session.Tables[tableName] = stats

	// 重新计算会话统计
	session.TotalRows = 0
	session.TotalBytes = 0
	for _, tableStats := range session.Tables {
		session.TotalRows += tableStats.ProcessedRows
		session.TotalBytes += tableStats.ProcessedBytes
	}

	return nil
}

// GetActiveSessions 获取活跃会话
func (e *SyncEngine) GetActiveSessions() map[string]*SyncSession {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	sessions := make(map[string]*SyncSession)
	for id, session := range e.syncSessions {
		// 创建副本以避免并发问题
		sessionCopy := *session
		sessions[id] = &sessionCopy
	}
	return sessions
}

// GetMetrics 获取同步指标
func (e *SyncEngine) GetMetrics() *SyncMetrics {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	// 计算平均速度
	if e.metrics.TotalSessions > 0 {
		uptime := time.Since(e.metrics.Uptime).Seconds()
		if uptime > 0 {
			e.metrics.AverageSpeed = float64(e.metrics.TotalRowsSynced) / uptime
		}
	}

	// 返回副本
	metricsCopy := *e.metrics
	return &metricsCopy
}

// GetStatus 获取引擎状态
func (e *SyncEngine) GetStatus() *EngineStatus {
	e.mutex.RLock()
	defer e.mutex.RUnlock()

	status := &EngineStatus{
		Running:        e.running,
		ActiveSessions: e.GetActiveSessions(),
		Metrics:        e.GetMetrics(),
	}

	// 如果有关联的Scheduler，添加其统计信息
	// if e.scheduler != nil {
	//     status.SchedulerStats = e.scheduler.GetStatus()
	// }

	return status
}

// cancelSession 取消同步会话（内部方法）
func (e *SyncEngine) cancelSession(sessionID string) {
	if session, exists := e.syncSessions[sessionID]; exists {
		session.Status = "cancelled"
		session.EndTime = time.Now()
		e.metrics.ActiveSessions--
		delete(e.syncSessions, sessionID)
	}
}

// metricsCollector 指标收集器（后台goroutine）
func (e *SyncEngine) metricsCollector() {
	ticker := time.NewTicker(30 * time.Second) // 每30秒收集一次指标
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			// 清理过期的会话数据（可选）
			e.cleanupExpiredSessions()
		}
	}
}

// cleanupExpiredSessions 清理过期的会话数据
func (e *SyncEngine) cleanupExpiredSessions() {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	now := time.Now()
	expireThreshold := 24 * time.Hour // 24小时后过期

	for sessionID, session := range e.syncSessions {
		if session.Status != "running" && now.Sub(session.EndTime) > expireThreshold {
			delete(e.syncSessions, sessionID)
			e.logger.Debugf("Cleaned up expired session: %s", sessionID)
		}
	}
}