package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

// Load 从文件加载配置
func Load(configPath string) (*Config, error) {
	// 检查文件是否存在
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file not found: %s", configPath)
	}

	// 读取文件内容
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// 解析YAML
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// 验证配置
	if err := validate(&config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &config, nil
}

// LoadFromBytes 从字节数组加载配置（用于测试）
func LoadFromBytes(data []byte) (*Config, error) {
	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if err := validate(&config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &config, nil
}

// FindConfigFile 查找配置文件
func FindConfigFile() (string, error) {
	// 搜索路径列表
	searchPaths := []string{
		"config.yaml",
		"configs/config.yaml",
		"/etc/ck2sr/config.yaml",
		filepath.Join(os.Getenv("HOME"), ".ck2sr", "config.yaml"),
	}

	for _, path := range searchPaths {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("config file not found in any of the search paths: %v", searchPaths)
}

// GetClickHouseConfig 根据名称获取ClickHouse配置
func (c *Config) GetClickHouseConfig(name string) (*ClickHouseConfig, error) {
	for _, config := range c.ClickHouse {
		if config.Name == name {
			return &config, nil
		}
	}
	return nil, fmt.Errorf("clickhouse config not found: %s", name)
}

// GetStarRocksConfig 根据名称获取StarRocks配置
func (c *Config) GetStarRocksConfig(name string) (*StarRocksConfig, error) {
	for _, config := range c.StarRocks {
		if config.Name == name {
			return &config, nil
		}
	}
	return nil, fmt.Errorf("starrocks config not found: %s", name)
}

// GetEnabledTasks 获取启用的同步任务
func (c *Config) GetEnabledTasks() []SyncTaskConfig {
	var enabledTasks []SyncTaskConfig
	for _, task := range c.SyncTasks {
		if task.Enabled {
			enabledTasks = append(enabledTasks, task)
		}
	}
	return enabledTasks
}

// GetTasksByPriority 按优先级排序获取任务
func (c *Config) GetTasksByPriority() []SyncTaskConfig {
	tasks := c.GetEnabledTasks()

	// 简单的冒泡排序，按优先级升序排列
	for i := 0; i < len(tasks)-1; i++ {
		for j := 0; j < len(tasks)-i-1; j++ {
			if tasks[j].Priority > tasks[j+1].Priority {
				tasks[j], tasks[j+1] = tasks[j+1], tasks[j]
			}
		}
	}

	return tasks
}