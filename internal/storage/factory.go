package storage

import (
	"fmt"

	"github.com/sirupsen/logrus"
)

// StorageType 存储类型
type StorageType string

const (
	StorageTypeFile StorageType = "file"
	StorageTypeK8s  StorageType = "k8s"
)

// StorageFactory 存储工厂
type StorageFactory struct {
	logger *logrus.Logger
}

// NewStorageFactory 创建存储工厂
func NewStorageFactory(logger *logrus.Logger) *StorageFactory {
	if logger == nil {
		logger = logrus.New()
	}

	return &StorageFactory{
		logger: logger,
	}
}

// CreateStorage 创建存储实例
func (sf *StorageFactory) CreateStorage(storageType StorageType, config map[string]interface{}) (Storage, error) {
	switch storageType {
	case StorageTypeFile:
		return sf.createFileStorage(config)
	case StorageTypeK8s:
		return sf.createK8sStorage(config)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", storageType)
	}
}

// createFileStorage 创建文件存储
func (sf *StorageFactory) createFileStorage(config map[string]interface{}) (Storage, error) {
	path, ok := config["path"].(string)
	if !ok || path == "" {
		return nil, fmt.Errorf("file storage requires 'path' parameter")
	}

	return NewFileStorage(path, sf.logger)
}

// createK8sStorage 创建 Kubernetes 存储
func (sf *StorageFactory) createK8sStorage(config map[string]interface{}) (Storage, error) {
	k8sConfig := &K8sStorageConfig{
		Namespace: "default",
		Group:     "ck2sr.io",
		Version:   "v1",
		Resource:  "synctasks",
	}

	// 解析配置
	if kubeConfig, ok := config["kube_config"].(string); ok {
		k8sConfig.KubeConfig = kubeConfig
	}

	if namespace, ok := config["namespace"].(string); ok && namespace != "" {
		k8sConfig.Namespace = namespace
	}

	if group, ok := config["group"].(string); ok && group != "" {
		k8sConfig.Group = group
	}

	if version, ok := config["version"].(string); ok && version != "" {
		k8sConfig.Version = version
	}

	if resource, ok := config["resource"].(string); ok && resource != "" {
		k8sConfig.Resource = resource
	}

	return NewK8sStorage(k8sConfig, sf.logger)
}

// GetSupportedTypes 获取支持的存储类型
func (sf *StorageFactory) GetSupportedTypes() []StorageType {
	return []StorageType{
		StorageTypeFile,
		StorageTypeK8s,
	}
}

// ValidateConfig 验证存储配置
func (sf *StorageFactory) ValidateConfig(storageType StorageType, config map[string]interface{}) error {
	switch storageType {
	case StorageTypeFile:
		return sf.validateFileConfig(config)
	case StorageTypeK8s:
		return sf.validateK8sConfig(config)
	default:
		return fmt.Errorf("unsupported storage type: %s", storageType)
	}
}

// validateFileConfig 验证文件存储配置
func (sf *StorageFactory) validateFileConfig(config map[string]interface{}) error {
	path, ok := config["path"].(string)
	if !ok || path == "" {
		return fmt.Errorf("file storage requires 'path' parameter")
	}
	return nil
}

// validateK8sConfig 验证 Kubernetes 存储配置
func (sf *StorageFactory) validateK8sConfig(config map[string]interface{}) error {
	// Kubernetes 存储的所有参数都是可选的，有默认值
	return nil
}