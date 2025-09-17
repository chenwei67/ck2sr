package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

// 配置管理器
type Manager struct {
	config *Config
	viper  *viper.Viper
}

// 创建新的配置管理器
func NewManager() *Manager {
	return &Manager{
		viper: viper.New(),
	}
}

// 从文件加载配置
func (m *Manager) LoadFromFile(configPath string) error {
	// 设置默认配置
	m.config = DefaultConfig()

	// 配置 viper
	m.viper.SetConfigFile(configPath)
	m.viper.SetConfigType(getConfigType(configPath))

	// 设置环境变量前缀
	m.viper.SetEnvPrefix("CK2SR")
	m.viper.AutomaticEnv()
	m.viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))

	// 读取配置文件
	if err := m.viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return fmt.Errorf("failed to read config file: %w", err)
		}
		// 配置文件不存在，使用默认配置
	}

	// 解析配置到结构体
	if err := m.viper.Unmarshal(m.config); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// 验证配置
	if err := m.config.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	return nil
}

// 从环境变量和命令行参数加载配置
func (m *Manager) LoadFromEnv() error {
	m.config = DefaultConfig()

	// 绑定环境变量
	m.bindEnvVars()

	// 解析配置
	if err := m.viper.Unmarshal(m.config); err != nil {
		return fmt.Errorf("failed to unmarshal config from env: %w", err)
	}

	// 验证配置
	if err := m.config.Validate(); err != nil {
		return fmt.Errorf("config validation failed: %w", err)
	}

	return nil
}

// 获取配置
func (m *Manager) GetConfig() *Config {
	return m.config
}

// 获取任务配置
func (m *Manager) GetTaskConfig(taskID string) (*SyncTaskConfig, error) {
	for _, task := range m.config.SyncTasks {
		if task.TaskID == taskID {
			return m.config.MergeTaskConfig(&task), nil
		}
	}
	return nil, fmt.Errorf("task not found: %s", taskID)
}

// 获取启用的任务列表
func (m *Manager) GetEnabledTasks() []*SyncTaskConfig {
	var enabledTasks []*SyncTaskConfig
	for _, task := range m.config.SyncTasks {
		if task.Enabled {
			enabledTasks = append(enabledTasks, m.config.MergeTaskConfig(&task))
		}
	}
	return enabledTasks
}

// 更新任务配置
func (m *Manager) UpdateTaskConfig(taskID string, updates map[string]interface{}) error {
	for i, task := range m.config.SyncTasks {
		if task.TaskID == taskID {
			// 使用 viper 设置值
			for key, value := range updates {
				m.viper.Set(fmt.Sprintf("sync_tasks.%d.%s", i, key), value)
			}

			// 重新解析配置
			if err := m.viper.Unmarshal(m.config); err != nil {
				return fmt.Errorf("failed to unmarshal updated config: %w", err)
			}

			// 验证配置
			if err := m.config.Validate(); err != nil {
				return fmt.Errorf("updated config validation failed: %w", err)
			}

			return nil
		}
	}
	return fmt.Errorf("task not found: %s", taskID)
}

// 保存配置到文件
func (m *Manager) SaveToFile(configPath string) error {
	if err := os.MkdirAll(filepath.Dir(configPath), 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	return m.viper.WriteConfigAs(configPath)
}

// 重新加载配置
func (m *Manager) Reload() error {
	if err := m.viper.ReadInConfig(); err != nil {
		return fmt.Errorf("failed to reload config: %w", err)
	}

	if err := m.viper.Unmarshal(m.config); err != nil {
		return fmt.Errorf("failed to unmarshal reloaded config: %w", err)
	}

	if err := m.config.Validate(); err != nil {
		return fmt.Errorf("reloaded config validation failed: %w", err)
	}

	return nil
}

// 监听配置文件变化
func (m *Manager) WatchConfig(callback func()) {
	m.viper.WatchConfig()
	m.viper.OnConfigChange(func(e fsnotify.Event) {
		if err := m.Reload(); err == nil && callback != nil {
			callback()
		}
	})
}

// 绑定环境变量
func (m *Manager) bindEnvVars() {
	// 数据库配置
	m.viper.BindEnv("clickhouse.host", "CK2SR_CLICKHOUSE_HOST")
	m.viper.BindEnv("clickhouse.port", "CK2SR_CLICKHOUSE_PORT")
	m.viper.BindEnv("clickhouse.username", "CK2SR_CLICKHOUSE_USERNAME")
	m.viper.BindEnv("clickhouse.password", "CK2SR_CLICKHOUSE_PASSWORD")
	m.viper.BindEnv("clickhouse.database", "CK2SR_CLICKHOUSE_DATABASE")

	m.viper.BindEnv("starrocks.host", "CK2SR_STARROCKS_HOST")
	m.viper.BindEnv("starrocks.port", "CK2SR_STARROCKS_PORT")
	m.viper.BindEnv("starrocks.username", "CK2SR_STARROCKS_USERNAME")
	m.viper.BindEnv("starrocks.password", "CK2SR_STARROCKS_PASSWORD")
	m.viper.BindEnv("starrocks.database", "CK2SR_STARROCKS_DATABASE")
	m.viper.BindEnv("starrocks.stream_load_url", "CK2SR_STARROCKS_STREAM_LOAD_URL")

	// 全局配置
	m.viper.BindEnv("global_rate_limit.max_bytes_per_second", "CK2SR_GLOBAL_RATE_LIMIT_MAX_BYTES_PER_SECOND")
	m.viper.BindEnv("global_concurrency.max_workers", "CK2SR_GLOBAL_CONCURRENCY_MAX_WORKERS")
	m.viper.BindEnv("global_concurrency.max_concurrent_tasks", "CK2SR_GLOBAL_CONCURRENCY_MAX_CONCURRENT_TASKS")

	// 监控配置
	m.viper.BindEnv("monitor.enabled", "CK2SR_MONITOR_ENABLED")
	m.viper.BindEnv("monitor.metrics_port", "CK2SR_MONITOR_METRICS_PORT")
	m.viper.BindEnv("monitor.health_check_port", "CK2SR_MONITOR_HEALTH_CHECK_PORT")

	// Kubernetes 配置
	m.viper.BindEnv("kubernetes.enabled", "CK2SR_KUBERNETES_ENABLED")
	m.viper.BindEnv("kubernetes.namespace", "CK2SR_KUBERNETES_NAMESPACE")
	m.viper.BindEnv("kubernetes.configmap_name", "CK2SR_KUBERNETES_CONFIGMAP_NAME")

	// 日志配置
	m.viper.BindEnv("log.level", "CK2SR_LOG_LEVEL")
	m.viper.BindEnv("log.format", "CK2SR_LOG_FORMAT")
	m.viper.BindEnv("log.output", "CK2SR_LOG_OUTPUT")

	// 其他配置
	m.viper.BindEnv("storage_path", "CK2SR_STORAGE_PATH")
}

// 获取配置文件类型
func getConfigType(configPath string) string {
	ext := filepath.Ext(configPath)
	switch strings.ToLower(ext) {
	case ".yaml", ".yml":
		return "yaml"
	case ".json":
		return "json"
	case ".toml":
		return "toml"
	default:
		return "yaml"
	}
}

// 全局配置管理器实例
var globalManager *Manager

// 初始化全局配置管理器
func Init(configPath string) error {
	globalManager = NewManager()
	if configPath != "" {
		return globalManager.LoadFromFile(configPath)
	}
	return globalManager.LoadFromEnv()
}

// 获取全局配置
func Get() *Config {
	if globalManager == nil {
		panic("config manager not initialized")
	}
	return globalManager.GetConfig()
}

// 获取全局配置管理器
func GetManager() *Manager {
	if globalManager == nil {
		panic("config manager not initialized")
	}
	return globalManager
}

// 获取任务配置的便捷函数
func GetTaskConfig(taskID string) (*SyncTaskConfig, error) {
	return globalManager.GetTaskConfig(taskID)
}

// 获取启用任务的便捷函数
func GetEnabledTasks() []*SyncTaskConfig {
	return globalManager.GetEnabledTasks()
}