package logging

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/sunkaimr/ck2sr/internal/config"
)

// CreateLogger 根据配置创建logger实例
func CreateLogger(logConfig config.LogConfig) (*logrus.Logger, error) {
	logger := logrus.New()

	// 设置日志级别
	level, err := parseLogLevel(logConfig.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level '%s': %w", logConfig.Level, err)
	}
	logger.SetLevel(level)

	// 设置日志格式
	formatter, err := parseLogFormatter(logConfig.Format)
	if err != nil {
		return nil, fmt.Errorf("invalid log format '%s': %w", logConfig.Format, err)
	}
	logger.SetFormatter(formatter)

	// 设置输出目标
	output, err := parseLogOutput(logConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to configure log output: %w", err)
	}
	logger.SetOutput(output)

	// 设置调用者报告
	logger.SetReportCaller(true)

	return logger, nil
}

// parseLogLevel 解析日志级别
func parseLogLevel(levelStr string) (logrus.Level, error) {
	if levelStr == "" {
		return logrus.InfoLevel, nil
	}

	switch strings.ToLower(levelStr) {
	case "panic":
		return logrus.PanicLevel, nil
	case "fatal":
		return logrus.FatalLevel, nil
	case "error":
		return logrus.ErrorLevel, nil
	case "warn", "warning":
		return logrus.WarnLevel, nil
	case "info":
		return logrus.InfoLevel, nil
	case "debug":
		return logrus.DebugLevel, nil
	case "trace":
		return logrus.TraceLevel, nil
	default:
		return logrus.InfoLevel, fmt.Errorf("unknown log level: %s", levelStr)
	}
}

// parseLogFormatter 解析日志格式器
func parseLogFormatter(formatStr string) (logrus.Formatter, error) {
	switch strings.ToLower(formatStr) {
	case "", "text":
		return &logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05",
		}, nil
	case "json":
		return &logrus.JSONFormatter{
			TimestampFormat: "2006-01-02 15:04:05",
		}, nil
	default:
		return &logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05",
		}, fmt.Errorf("unknown log format: %s", formatStr)
	}
}

// parseLogOutput 解析日志输出
func parseLogOutput(logConfig config.LogConfig) (*os.File, error) {
	switch strings.ToLower(logConfig.Output) {
	case "", "stdout":
		return os.Stdout, nil
	case "stderr":
		return os.Stderr, nil
	case "file":
		if logConfig.FilePath == "" {
			return nil, fmt.Errorf("file path is required when output is 'file'")
		}

		// For the simple CreateLogger function, we just return stdout
		// The actual file handling is done in CreateLoggerWithFileRotation
		return os.Stdout, nil
	default:
		return os.Stdout, fmt.Errorf("unknown log output: %s", logConfig.Output)
	}
}

// CreateLoggerWithFileRotation 创建支持文件轮转的logger
func CreateLoggerWithFileRotation(logConfig config.LogConfig) (*logrus.Logger, error) {
	logger := logrus.New()

	// 设置日志级别
	level, err := parseLogLevel(logConfig.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level '%s': %w", logConfig.Level, err)
	}
	logger.SetLevel(level)

	// 设置日志格式
	formatter, err := parseLogFormatter(logConfig.Format)
	if err != nil {
		return nil, fmt.Errorf("invalid log format '%s': %w", logConfig.Format, err)
	}
	logger.SetFormatter(formatter)

	// 设置输出目标
	switch strings.ToLower(logConfig.Output) {
	case "", "stdout":
		logger.SetOutput(os.Stdout)
	case "stderr":
		logger.SetOutput(os.Stderr)
	case "file":
		if logConfig.FilePath == "" {
			return nil, fmt.Errorf("file path is required when output is 'file'")
		}

		// 使用 lumberjack 进行日志轮转
		logWriter := &lumberjack.Logger{
			Filename:   logConfig.FilePath,
			MaxSize:    logConfig.MaxSize, // MB
			MaxBackups: logConfig.MaxBackups,
			MaxAge:     logConfig.MaxAge, // days
			Compress:   logConfig.Compress,
		}

		logger.SetOutput(logWriter)
	default:
		return nil, fmt.Errorf("unknown log output: %s", logConfig.Output)
	}

	// 设置调用者报告
	// logger.SetReportCaller(true)

	return logger, nil
}
