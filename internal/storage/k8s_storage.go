package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// K8sStorage Kubernetes CRD 存储实现
type K8sStorage struct {
	client    dynamic.Interface
	namespace string
	gvr       schema.GroupVersionResource
	logger    *logrus.Logger
}

// K8sStorageConfig Kubernetes 存储配置
type K8sStorageConfig struct {
	KubeConfig string `yaml:"kube_config" json:"kube_config"`
	Namespace  string `yaml:"namespace" json:"namespace"`
	Group      string `yaml:"group" json:"group"`
	Version    string `yaml:"version" json:"version"`
	Resource   string `yaml:"resource" json:"resource"`
}

// NewK8sStorage 创建 Kubernetes 存储实例
func NewK8sStorage(config *K8sStorageConfig, logger *logrus.Logger) (*K8sStorage, error) {
	if logger == nil {
		logger = logrus.New()
	}

	// 构建 Kubernetes 配置
	var kubeConfig *rest.Config
	var err error

	if config.KubeConfig != "" {
		kubeConfig, err = clientcmd.BuildConfigFromFlags("", config.KubeConfig)
	} else {
		kubeConfig, err = rest.InClusterConfig()
	}

	if err != nil {
		return nil, fmt.Errorf("failed to build kubernetes config: %w", err)
	}

	// 创建动态客户端
	client, err := dynamic.NewForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	// 构建 GroupVersionResource
	gvr := schema.GroupVersionResource{
		Group:    config.Group,
		Version:  config.Version,
		Resource: config.Resource,
	}

	storage := &K8sStorage{
		client:    client,
		namespace: config.Namespace,
		gvr:       gvr,
		logger:    logger,
	}

	return storage, nil
}

// Initialize 初始化存储
func (ks *K8sStorage) Initialize(ctx context.Context) error {
	// 验证 CRD 是否存在
	_, err := ks.client.Resource(ks.gvr).Namespace(ks.namespace).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return fmt.Errorf("failed to verify CRD: %w", err)
	}

	ks.logger.Infof("K8sStorage initialized with namespace: %s, resource: %s", ks.namespace, ks.gvr.Resource)
	return nil
}

// Close 关闭存储
func (ks *K8sStorage) Close() error {
	// Kubernetes 客户端无需特殊关闭操作
	return nil
}

// SaveTaskState 保存任务状态
func (ks *K8sStorage) SaveTaskState(ctx context.Context, state *TaskState) error {
	state.UpdateTime = time.Now()

	// 转换为 Kubernetes 资源
	resource, err := ks.taskStateToResource(state)
	if err != nil {
		return fmt.Errorf("failed to convert task state to resource: %w", err)
	}

	// 尝试更新现有资源
	_, err = ks.client.Resource(ks.gvr).Namespace(ks.namespace).Update(ctx, resource, metav1.UpdateOptions{})
	if err != nil {
		// 如果更新失败，尝试创建新资源
		_, err = ks.client.Resource(ks.gvr).Namespace(ks.namespace).Create(ctx, resource, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("failed to create task state resource: %w", err)
		}
	}

	ks.logger.Debugf("Saved task state for task %s", state.TaskID)
	return nil
}

// GetTaskState 获取任务状态
func (ks *K8sStorage) GetTaskState(ctx context.Context, taskID string) (*TaskState, error) {
	resourceName := ks.getResourceName(taskID)
	resource, err := ks.client.Resource(ks.gvr).Namespace(ks.namespace).Get(ctx, resourceName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("task state not found: %s", taskID)
	}

	state, err := ks.resourceToTaskState(resource)
	if err != nil {
		return nil, fmt.Errorf("failed to convert resource to task state: %w", err)
	}

	return state, nil
}

// GetAllTaskStates 获取所有任务状态
func (ks *K8sStorage) GetAllTaskStates(ctx context.Context) ([]*TaskState, error) {
	list, err := ks.client.Resource(ks.gvr).Namespace(ks.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list task states: %w", err)
	}

	var states []*TaskState
	for _, item := range list.Items {
		state, err := ks.resourceToTaskState(&item)
		if err != nil {
			ks.logger.Warnf("Failed to convert resource to task state: %v", err)
			continue
		}
		states = append(states, state)
	}

	return states, nil
}

// GetTasksByStatus 获取指定状态的任务
func (ks *K8sStorage) GetTasksByStatus(ctx context.Context, status TaskStatus) ([]*TaskState, error) {
	// 使用标签选择器过滤
	labelSelector := fmt.Sprintf("status=%s", status)
	list, err := ks.client.Resource(ks.gvr).Namespace(ks.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks by status: %w", err)
	}

	var states []*TaskState
	for _, item := range list.Items {
		state, err := ks.resourceToTaskState(&item)
		if err != nil {
			ks.logger.Warnf("Failed to convert resource to task state: %v", err)
			continue
		}

		// 双重检查状态
		if state.Status == status {
			states = append(states, state)
		}
	}

	return states, nil
}

// DeleteTaskState 删除任务状态
func (ks *K8sStorage) DeleteTaskState(ctx context.Context, taskID string) error {
	resourceName := ks.getResourceName(taskID)
	err := ks.client.Resource(ks.gvr).Namespace(ks.namespace).Delete(ctx, resourceName, metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete task state: %w", err)
	}

	ks.logger.Debugf("Deleted task state for task %s", taskID)
	return nil
}

// CleanupExpiredTasks 清理过期任务状态
func (ks *K8sStorage) CleanupExpiredTasks(ctx context.Context, expireTime time.Duration) error {
	allStates, err := ks.GetAllTaskStates(ctx)
	if err != nil {
		return err
	}

	cutoffTime := time.Now().Add(-expireTime)
	var deletedCount int

	for _, state := range allStates {
		if state.IsFinished() && state.UpdateTime.Before(cutoffTime) {
			if err := ks.DeleteTaskState(ctx, state.TaskID); err != nil {
				ks.logger.Warnf("Failed to delete expired task %s: %v", state.TaskID, err)
			} else {
				deletedCount++
				ks.logger.Debugf("Deleted expired task: %s", state.TaskID)
			}
		}
	}

	if deletedCount > 0 {
		ks.logger.Infof("Cleaned up %d expired tasks", deletedCount)
	}

	return nil
}

// UpdateProgress 更新任务进度
func (ks *K8sStorage) UpdateProgress(ctx context.Context, taskID string, processedRows, processedBytes int64) error {
	state, err := ks.GetTaskState(ctx, taskID)
	if err != nil {
		return err
	}

	state.ProcessedRows = processedRows
	state.ProcessedBytes = processedBytes
	state.UpdateTime = time.Now()

	// 计算处理速度
	if state.StartTime != nil {
		elapsed := time.Since(*state.StartTime).Seconds()
		if elapsed > 0 {
			state.BytesPerSecond = float64(processedBytes) / elapsed
			state.RowsPerSecond = float64(processedRows) / elapsed
		}
	}

	return ks.SaveTaskState(ctx, state)
}

// SaveCheckpoint 保存检查点
func (ks *K8sStorage) SaveCheckpoint(ctx context.Context, taskID string, checkpoint map[string]interface{}) error {
	state, err := ks.GetTaskState(ctx, taskID)
	if err != nil {
		return err
	}

	state.Checkpoint = checkpoint
	state.UpdateTime = time.Now()

	return ks.SaveTaskState(ctx, state)
}

// GetCheckpoint 获取检查点
func (ks *K8sStorage) GetCheckpoint(ctx context.Context, taskID string) (map[string]interface{}, error) {
	state, err := ks.GetTaskState(ctx, taskID)
	if err != nil {
		return nil, err
	}

	return state.Checkpoint, nil
}

// taskStateToResource 将任务状态转换为 Kubernetes 资源
func (ks *K8sStorage) taskStateToResource(state *TaskState) (*unstructured.Unstructured, error) {
	resource := &unstructured.Unstructured{}
	resource.SetAPIVersion(fmt.Sprintf("%s/%s", ks.gvr.Group, ks.gvr.Version))
	resource.SetKind("SyncTask")
	resource.SetNamespace(ks.namespace)
	resource.SetName(ks.getResourceName(state.TaskID))

	// 设置标签
	labels := map[string]string{
		"app":    "ck2sr",
		"status": string(state.Status),
		"task-id": state.TaskID,
	}
	resource.SetLabels(labels)

	// 设置规格
	spec := map[string]interface{}{
		"taskId":         state.TaskID,
		"status":         state.Status,
		"totalRows":      state.TotalRows,
		"processedRows":  state.ProcessedRows,
		"totalBytes":     state.TotalBytes,
		"processedBytes": state.ProcessedBytes,
		"lastOffset":     state.LastOffset,
		"retryCount":     state.RetryCount,
		"bytesPerSecond": state.BytesPerSecond,
		"rowsPerSecond":  state.RowsPerSecond,
		"updateTime":     state.UpdateTime.Format(time.RFC3339),
	}

	if state.StartTime != nil {
		spec["startTime"] = state.StartTime.Format(time.RFC3339)
	}
	if state.EndTime != nil {
		spec["endTime"] = state.EndTime.Format(time.RFC3339)
	}
	if state.ErrorMessage != "" {
		spec["errorMessage"] = state.ErrorMessage
	}
	if state.Checkpoint != nil {
		spec["checkpoint"] = state.Checkpoint
	}
	if state.Metadata != nil {
		spec["metadata"] = state.Metadata
	}

	resource.Object["spec"] = spec

	return resource, nil
}

// resourceToTaskState 将 Kubernetes 资源转换为任务状态
func (ks *K8sStorage) resourceToTaskState(resource *unstructured.Unstructured) (*TaskState, error) {
	spec, found, err := unstructured.NestedMap(resource.Object, "spec")
	if err != nil || !found {
		return nil, fmt.Errorf("invalid resource spec")
	}

	state := &TaskState{}

	// 基本字段
	if taskID, found, _ := unstructured.NestedString(spec, "taskId"); found {
		state.TaskID = taskID
	}
	if status, found, _ := unstructured.NestedString(spec, "status"); found {
		state.Status = TaskStatus(status)
	}
	if totalRows, found, _ := unstructured.NestedInt64(spec, "totalRows"); found {
		state.TotalRows = totalRows
	}
	if processedRows, found, _ := unstructured.NestedInt64(spec, "processedRows"); found {
		state.ProcessedRows = processedRows
	}
	if totalBytes, found, _ := unstructured.NestedInt64(spec, "totalBytes"); found {
		state.TotalBytes = totalBytes
	}
	if processedBytes, found, _ := unstructured.NestedInt64(spec, "processedBytes"); found {
		state.ProcessedBytes = processedBytes
	}
	if lastOffset, found, _ := unstructured.NestedInt64(spec, "lastOffset"); found {
		state.LastOffset = lastOffset
	}
	if retryCount, found, _ := unstructured.NestedInt64(spec, "retryCount"); found {
		state.RetryCount = int(retryCount)
	}
	if bytesPerSecond, found, _ := unstructured.NestedFloat64(spec, "bytesPerSecond"); found {
		state.BytesPerSecond = bytesPerSecond
	}
	if rowsPerSecond, found, _ := unstructured.NestedFloat64(spec, "rowsPerSecond"); found {
		state.RowsPerSecond = rowsPerSecond
	}

	// 时间字段
	if updateTime, found, _ := unstructured.NestedString(spec, "updateTime"); found {
		if t, err := time.Parse(time.RFC3339, updateTime); err == nil {
			state.UpdateTime = t
		}
	}
	if startTime, found, _ := unstructured.NestedString(spec, "startTime"); found {
		if t, err := time.Parse(time.RFC3339, startTime); err == nil {
			state.StartTime = &t
		}
	}
	if endTime, found, _ := unstructured.NestedString(spec, "endTime"); found {
		if t, err := time.Parse(time.RFC3339, endTime); err == nil {
			state.EndTime = &t
		}
	}

	// 错误信息
	if errorMessage, found, _ := unstructured.NestedString(spec, "errorMessage"); found {
		state.ErrorMessage = errorMessage
	}

	// 检查点
	if checkpoint, found, _ := unstructured.NestedMap(spec, "checkpoint"); found {
		state.Checkpoint = checkpoint
	}

	// 元数据
	if metadata, found, _ := unstructured.NestedMap(spec, "metadata"); found {
		state.Metadata = metadata
	}

	return state, nil
}

// getResourceName 根据任务 ID 生成资源名称
func (ks *K8sStorage) getResourceName(taskID string) string {
	// Kubernetes 资源名称必须符合 DNS 规范
	// 将任务 ID 转换为合法的资源名称
	resourceName := fmt.Sprintf("ck2sr-%s", taskID)
	return resourceName
}

// HealthCheck 健康检查
func (ks *K8sStorage) HealthCheck(ctx context.Context) error {
	// 尝试列出资源以验证连接
	_, err := ks.client.Resource(ks.gvr).Namespace(ks.namespace).List(ctx, metav1.ListOptions{Limit: 1})
	if err != nil {
		return fmt.Errorf("kubernetes storage health check failed: %w", err)
	}
	return nil
}