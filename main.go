package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"github.com/ck2sr/ck2sr/internal/config"
	"github.com/ck2sr/ck2sr/internal/scheduler"
	"github.com/ck2sr/ck2sr/internal/storage"
	"github.com/ck2sr/ck2sr/internal/worker"
	"github.com/ck2sr/ck2sr/pkg/clickhouse"
	"github.com/ck2sr/ck2sr/pkg/starrocks"
)

const (
	// 应用信息
	AppName    = "ck2sr"
	AppVersion = "1.0.0"
	AppDesc    = "ClickHouse to StarRocks Data Sync Service"
)

// 命令行参数
var (
	configFile = flag.String("config", "./config.yaml", "配置文件路径")
	logLevel   = flag.String("log-level", "", "日志级别 (debug, info, warn, error)")
	version    = flag.Bool("version", false, "显示版本信息")
	daemon     = flag.Bool("daemon", false, "以守护进程模式运行")
	validate   = flag.Bool("validate", false, "验证配置文件")
)

// Application 应用程序结构
type Application struct {
	config       *config.Config
	logger       *logrus.Logger
	chClient     *clickhouse.Client
	srClient     *starrocks.Client
	storage      storage.Storage
	taskManager  worker.TaskManager
	scheduler    scheduler.Scheduler
	metricsServer *http.Server
	healthServer  *http.Server
}

func main() {
	flag.Parse()

	// 显示版本信息
	if *version {
		fmt.Printf("%s v%s - %s\n", AppName, AppVersion, AppDesc)
		os.Exit(0)
	}

	// 创建应用程序实例
	app, err := NewApplication()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create application: %v\n", err)
		os.Exit(1)
	}

	// 验证配置
	if *validate {
		app.logger.Info("Configuration validation passed")
		fmt.Println("Configuration is valid")
		os.Exit(0)
	}

	// 启动应用程序
	if err := app.Run(); err != nil {
		app.logger.Fatalf("Application failed: %v", err)
		os.Exit(1)
	}
}

// NewApplication 创建应用程序实例
func NewApplication() (*Application, error) {
	// 初始化配置
	if err := config.Init(*configFile); err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	cfg := config.Get()

	// 初始化日志
	logger := setupLogger(cfg.Log)

	logger.Infof("Starting %s v%s", AppName, AppVersion)
	logger.Infof("Loading configuration from: %s", *configFile)

	// 创建 ClickHouse 客户端
	chClient, err := clickhouse.NewClient(&cfg.ClickHouse, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse client: %w", err)
	}

	// 创建 StarRocks 客户端
	srClient, err := starrocks.NewClient(&cfg.StarRocks, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create StarRocks client: %w", err)
	}

	// 创建存储
	storageFactory := storage.NewStorageFactory(logger)
	storageType := storage.StorageType(cfg.Kubernetes.Enabled)
	if cfg.Kubernetes.Enabled {
		storageType = storage.StorageTypeK8s
	} else {
		storageType = storage.StorageTypeFile
	}

	storageConfig := map[string]interface{}{
		"path": cfg.StoragePath,
	}

	if cfg.Kubernetes.Enabled {
		storageConfig["namespace"] = cfg.Kubernetes.Namespace
		storageConfig["group"] = "ck2sr.io"
		storageConfig["version"] = "v1"
		storageConfig["resource"] = "synctasks"
	}

	stor, err := storageFactory.CreateStorage(storageType, storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage: %w", err)
	}

	// 初始化存储
	if err := stor.Initialize(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize storage: %w", err)
	}

	// 创建任务管理器
	taskManager := worker.NewTaskManager(cfg, chClient, srClient, stor, logger)

	// 加载配置中的任务
	if err := taskManager.LoadTasksFromConfig(); err != nil {
		return nil, fmt.Errorf("failed to load tasks from config: %w", err)
	}

	// 创建调度器
	schedulerConfig := &scheduler.SchedulerConfig{
		CheckInterval:      time.Minute,
		MaxConcurrentTasks: cfg.GlobalConcurrency.MaxConcurrentTasks,
		EventBufferSize:    1000,
		RetryAttempts:      3,
		RetryInterval:      time.Minute,
	}

	schedulerPersistence := scheduler.NewFilePersistence(
		filepath.Join(cfg.StoragePath, "scheduler"),
		logger,
	)

	sched := scheduler.NewScheduler(
		schedulerConfig,
		taskManager,
		schedulerPersistence,
		logger,
	)

	app := &Application{
		config:      cfg,
		logger:      logger,
		chClient:    chClient,
		srClient:    srClient,
		storage:     stor,
		taskManager: taskManager,
		scheduler:   sched,
	}

	return app, nil
}

// Run 运行应用程序
func (app *Application) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动监控服务器
	if app.config.Monitor.Enabled {
		if err := app.startMonitoringServers(); err != nil {
			return fmt.Errorf("failed to start monitoring servers: %w", err)
		}
	}

	// 启动调度器
	if err := app.scheduler.Start(ctx); err != nil {
		return fmt.Errorf("failed to start scheduler: %w", err)
	}

	// 启动计划任务
	if err := app.taskManager.StartScheduledTasks(ctx); err != nil {
		app.logger.Warnf("Failed to start scheduled tasks: %v", err)
	}

	app.logger.Info("Application started successfully")

	// 等待信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动后台维护任务
	go app.maintenanceLoop(ctx)

	// 等待退出信号
	<-sigChan
	app.logger.Info("Received shutdown signal")

	// 优雅关闭
	return app.shutdown(ctx)
}

// startMonitoringServers 启动监控服务器
func (app *Application) startMonitoringServers() error {
	// 启动 Prometheus 指标服务器
	if app.config.Monitor.MetricsPort > 0 {
		mux := http.NewServeMux()
		mux.Handle(app.config.Monitor.MetricsPath, promhttp.Handler())
		mux.HandleFunc("/api/tasks", app.handleTasksAPI)
		mux.HandleFunc("/api/schedules", app.handleSchedulesAPI)
		mux.HandleFunc("/api/progress", app.handleProgressAPI)

		app.metricsServer = &http.Server{
			Addr:    fmt.Sprintf(":%d", app.config.Monitor.MetricsPort),
			Handler: mux,
		}

		go func() {
			app.logger.Infof("Starting metrics server on :%d", app.config.Monitor.MetricsPort)
			if err := app.metricsServer.ListenAndServe(); err != http.ErrServerClosed {
				app.logger.Errorf("Metrics server error: %v", err)
			}
		}()
	}

	// 启动健康检查服务器
	if app.config.Monitor.HealthCheckPort > 0 {
		healthMux := http.NewServeMux()
		healthMux.HandleFunc("/health", app.handleHealthCheck)
		healthMux.HandleFunc("/ready", app.handleReadinessCheck)

		app.healthServer = &http.Server{
			Addr:    fmt.Sprintf(":%d", app.config.Monitor.HealthCheckPort),
			Handler: healthMux,
		}

		go func() {
			app.logger.Infof("Starting health check server on :%d", app.config.Monitor.HealthCheckPort)
			if err := app.healthServer.ListenAndServe(); err != http.ErrServerClosed {
				app.logger.Errorf("Health check server error: %v", err)
			}
		}()
	}

	return nil
}

// maintenanceLoop 维护循环
func (app *Application) maintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(app.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			app.performMaintenance()
		}
	}
}

// performMaintenance 执行维护任务
func (app *Application) performMaintenance() {
	// 更新任务进度
	if err := app.taskManager.UpdateProgress(); err != nil {
		app.logger.Warnf("Failed to update task progress: %v", err)
	}

	// 清理过期任务状态
	if err := app.storage.CleanupExpiredTasks(context.Background(), 24*time.Hour); err != nil {
		app.logger.Warnf("Failed to cleanup expired tasks: %v", err)
	}

	// 健康检查
	if err := app.taskManager.HealthCheck(context.Background()); err != nil {
		app.logger.Warnf("Task manager health check failed: %v", err)
	}
}

// shutdown 优雅关闭
func (app *Application) shutdown(ctx context.Context) error {
	app.logger.Info("Starting graceful shutdown...")

	// 创建关闭上下文
	shutdownCtx, cancel := context.WithTimeout(ctx, app.config.GracefulShutdownTimeout)
	defer cancel()

	// 停止调度器
	if err := app.scheduler.Stop(shutdownCtx); err != nil {
		app.logger.Warnf("Failed to stop scheduler: %v", err)
	}

	// 停止任务管理器
	if err := app.taskManager.Shutdown(shutdownCtx); err != nil {
		app.logger.Warnf("Failed to shutdown task manager: %v", err)
	}

	// 关闭数据库连接
	if err := app.chClient.Close(); err != nil {
		app.logger.Warnf("Failed to close ClickHouse client: %v", err)
	}

	if err := app.srClient.Close(); err != nil {
		app.logger.Warnf("Failed to close StarRocks client: %v", err)
	}

	// 关闭存储
	if err := app.storage.Close(); err != nil {
		app.logger.Warnf("Failed to close storage: %v", err)
	}

	// 停止监控服务器
	if app.metricsServer != nil {
		if err := app.metricsServer.Shutdown(shutdownCtx); err != nil {
			app.logger.Warnf("Failed to shutdown metrics server: %v", err)
		}
	}

	if app.healthServer != nil {
		if err := app.healthServer.Shutdown(shutdownCtx); err != nil {
			app.logger.Warnf("Failed to shutdown health server: %v", err)
		}
	}

	app.logger.Info("Graceful shutdown completed")
	return nil
}

// setupLogger 设置日志
func setupLogger(logConfig config.LogConfig) *logrus.Logger {
	logger := logrus.New()

	// 设置日志级别
	if *logLevel != "" {
		logConfig.Level = *logLevel
	}

	level, err := logrus.ParseLevel(logConfig.Level)
	if err != nil {
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)

	// 设置日志格式
	if logConfig.Format == "json" {
		logger.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: time.RFC3339,
		})
	} else {
		logger.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: time.RFC3339,
		})
	}

	// 设置输出
	switch logConfig.Output {
	case "file":
		if logConfig.FilePath != "" {
			// 确保日志目录存在
			if err := os.MkdirAll(filepath.Dir(logConfig.FilePath), 0755); err != nil {
				logger.Warnf("Failed to create log directory: %v", err)
			} else {
				file, err := os.OpenFile(logConfig.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
				if err != nil {
					logger.Warnf("Failed to open log file: %v", err)
				} else {
					logger.SetOutput(file)
				}
			}
		}
	default:
		logger.SetOutput(os.Stdout)
	}

	return logger
}

// handleTasksAPI 处理任务 API
func (app *Application) handleTasksAPI(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		tasks := app.taskManager.GetAllTasks()
		app.writeJSON(w, map[string]interface{}{
			"tasks": tasks,
			"count": len(tasks),
		})
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleSchedulesAPI 处理调度 API
func (app *Application) handleSchedulesAPI(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		schedules, err := app.scheduler.GetAllSchedules()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		app.writeJSON(w, map[string]interface{}{
			"schedules": schedules,
			"count":     len(schedules),
		})
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleProgressAPI 处理进度 API
func (app *Application) handleProgressAPI(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		progress := app.taskManager.GetAllProgress()
		app.writeJSON(w, progress)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleHealthCheck 处理健康检查
func (app *Application) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := app.taskManager.HealthCheck(ctx); err != nil {
		app.writeJSON(w, map[string]interface{}{
			"status": "unhealthy",
			"error":  err.Error(),
		})
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	app.writeJSON(w, map[string]interface{}{
		"status":  "healthy",
		"version": AppVersion,
		"uptime":  time.Since(time.Now()).String(),
	})
}

// handleReadinessCheck 处理就绪检查
func (app *Application) handleReadinessCheck(w http.ResponseWriter, r *http.Request) {
	if !app.scheduler.IsRunning() {
		app.writeJSON(w, map[string]interface{}{
			"status": "not ready",
			"reason": "scheduler not running",
		})
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	app.writeJSON(w, map[string]interface{}{
		"status": "ready",
	})
}

// writeJSON 写入 JSON 响应
func (app *Application) writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		app.logger.Errorf("Failed to encode JSON response: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}