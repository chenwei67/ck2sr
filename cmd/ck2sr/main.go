package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/sunkaimr/ck2sr/internal/client"
	"github.com/sunkaimr/ck2sr/internal/config"
	"github.com/sunkaimr/ck2sr/internal/logging"
	"github.com/sunkaimr/ck2sr/internal/scheduler"
	"github.com/sunkaimr/ck2sr/internal/storage"
	"github.com/sunkaimr/ck2sr/internal/sync"
)

func main() {
	// 定义命令行参数
    configPath := flag.String("config", "", "配置文件路径")
	version := flag.Bool("version", false, "显示版本信息")
	flag.Parse()

	// 显示版本信息
	if *version {
		fmt.Println("ck2sr - ClickHouse to StarRocks Data Sync Tool v1.0.0")
		os.Exit(0)
	}

	// 1. 确定配置文件路径
	var finalConfigPath string
	if *configPath != "" {
		// 使用命令行指定的配置文件路径
		finalConfigPath = *configPath
	} else {
		// 查找默认配置文件
		foundPath, err := config.FindConfigFile()
		if err != nil {
			fmt.Printf("Config file not found, please specify with --config. Error: %v\n", err)
			os.Exit(1)
		} else {
			finalConfigPath = foundPath
		}
	}

	cfg, err := config.Load(finalConfigPath)
	if err != nil {
		fmt.Printf("Failed to load config(%s): %v\n", finalConfigPath, err)
		os.Exit(1)
	}

	// 创建日志记录器
	logger, err := logging.CreateLoggerWithFileRotation(cfg.Log)
	if err != nil {
		fmt.Printf("Failed to create logger: %v\n", err)
		os.Exit(1)
	}

	logger.Infof("ck2sr starting with config: %s", finalConfigPath)
	logger.Debugf("config: %+v", cfg)
	logger.Debugf("DEBUG: Log level configuration test - debug logging is working! (Level: %s)", cfg.Log.Level)

	// 获取策略配置（从config.yaml中的policy section）
	policyConfig := &cfg.Policy
	if cfg.Service.PProfEnable {
		// 启动 pprof HTTP 服务
		go func() {
			log.Println(http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", cfg.Service.ListenPort), nil))
		}()
	}

	// 创建存储组件
	store, err := storage.NewFileStorage(cfg.Service.StoragePath, logger)
	if err != nil {
		log.Fatalf("Failed to create storage: %v", err)
	}
	defer store.Close()

	// 客户端初始化
	ckCliMgr, err := client.NewClickHouseClientMgr(cfg.ClickHouse)
	if err != nil {
		logger.Fatalf("Failed to create ClickHouse client manager: %v", err)
	}
	defer ckCliMgr.Close()

	srCliMgr, err := client.NewStarRocksClientMgr(cfg.StarRocks, logger)
	if err != nil {
		logger.Fatalf("Failed to create StarRocks client manager: %v", err)
	}
	defer srCliMgr.Close()

	// 创建调度器
	sched := scheduler.NewScheduler(policyConfig, store, logger)

	// 创建同步任务并注册到调度器
	for _, taskConfig := range cfg.SyncTasks {
		if taskConfig.Enabled {
			// 客户端句柄初始化问题
			task, err := sync.NewSyncTask(&taskConfig, policyConfig, store, ckCliMgr, srCliMgr, logger)
			if err != nil {
				logger.Fatalf("Failed to create sync task %s: %v", taskConfig.TaskID, err)
			}
			if err := sched.RegisterTask(task); err != nil {
				logger.Fatalf("Failed to register task %s: %v", taskConfig.TaskID, err)
			}
			logger.Infof("Registered task: %s (%s)", taskConfig.Name, taskConfig.TaskID)
		}
	}

	// 3. 设置信号处理
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 启动信号处理goroutine
	go func() {
		sig := <-sigChan
		logger.Warnf("Received signal: %v", sig)
		logger.Info("Initiating graceful shutdown...")
		cancel()
	}()

	// 启动调度器（两阶段调度，阻塞直到所有任务完成）
	logger.Info("Starting scheduler with two-phase scheduling...")
	if err := sched.Start(ctx); err != nil {
		logger.Fatalf("Scheduler execution failed: %v", err)
	}

	// 调度器完成后，获取退出码并优雅退出
	exitCode := sched.GetExitCode()
	logger.Infof("All tasks completed, exiting with code: %d", exitCode)

	// 执行资源清理
	logger.Info("Cleaning up resources...")
	if err := sched.Stop(); err != nil {
		logger.Errorf("Error during shutdown: %v", err)
	}

	logger.Info("ck2sr shutdown complete. Goodbye!")
	os.Exit(exitCode)
}
