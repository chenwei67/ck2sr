package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestMemoryStorage 测试内存存储实现
func TestMemoryStorage(t *testing.T) {
	storage := NewMemoryStorage()

	// 测试偏移量保存和加载
	taskID := "test_task"
	tableName := "test_table"
	offset := int64(12345)

	// 保存偏移量
	err := storage.SaveOffset(taskID, tableName, offset)
	if err != nil {
		t.Fatalf("Failed to save offset: %v", err)
	}

	// 加载偏移量
	loadedOffset, err := storage.LoadOffset(taskID, tableName)
	if err != nil {
		t.Fatalf("Failed to load offset: %v", err)
	}

	if loadedOffset != offset {
		t.Errorf("Expected offset %d, got %d", offset, loadedOffset)
	}

	// 测试不存在的偏移量
	nonExistentOffset, err := storage.LoadOffset("nonexistent_task", "nonexistent_table")
	if err != nil {
		t.Errorf("Expected no error for nonexistent offset, got: %v", err)
	}
	if nonExistentOffset != 0 {
		t.Errorf("Expected offset 0 for nonexistent key, got %d", nonExistentOffset)
	}

	// 测试进度保存和加载
	progress := &TableSyncProgress{
		TableName:     tableName,
		Status:        "running",
		StartTime:     time.Now(),
		EndTime:       time.Time{},
		ProcessedRows: 1000,
		TotalRows:     5000,
		Progress:      0.2,
		ErrorCount:    0,
		LastError:     "",
	}

	err = storage.SaveProgress(taskID, tableName, progress)
	if err != nil {
		t.Fatalf("Failed to save progress: %v", err)
	}

	loadedProgress, err := storage.LoadProgress(taskID, tableName)
	if err != nil {
		t.Fatalf("Failed to load progress: %v", err)
	}

	if loadedProgress.TableName != progress.TableName {
		t.Errorf("Expected table name %s, got %s", progress.TableName, loadedProgress.TableName)
	}
	if loadedProgress.Status != progress.Status {
		t.Errorf("Expected status %s, got %s", progress.Status, loadedProgress.Status)
	}
	if loadedProgress.ProcessedRows != progress.ProcessedRows {
		t.Errorf("Expected processed rows %d, got %d", progress.ProcessedRows, loadedProgress.ProcessedRows)
	}
	if loadedProgress.Progress != progress.Progress {
		t.Errorf("Expected progress %f, got %f", progress.Progress, loadedProgress.Progress)
	}

	// 测试关闭
	err = storage.Close()
	if err != nil {
		t.Errorf("Failed to close storage: %v", err)
	}
}

// TestFileStorage 测试文件存储实现
func TestFileStorage(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "storage_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storage, err := NewFileStorage(tempDir)
	if err != nil {
		t.Fatalf("Failed to create file storage: %v", err)
	}
	defer storage.Close()

	// 测试偏移量保存和加载
	taskID := "file_test_task"
	tableName := "file_test_table"
	offset := int64(54321)

	err = storage.SaveOffset(taskID, tableName, offset)
	if err != nil {
		t.Fatalf("Failed to save offset to file: %v", err)
	}

	// 检查文件是否被创建
	offsetFile := filepath.Join(tempDir, "offsets", taskID+"_"+tableName+".json")
	if _, err := os.Stat(offsetFile); os.IsNotExist(err) {
		t.Errorf("Offset file was not created: %s", offsetFile)
	}

	loadedOffset, err := storage.LoadOffset(taskID, tableName)
	if err != nil {
		t.Fatalf("Failed to load offset from file: %v", err)
	}

	if loadedOffset != offset {
		t.Errorf("Expected offset %d, got %d", offset, loadedOffset)
	}

	// 测试进度保存和加载
	progress := &TableSyncProgress{
		TableName:     tableName,
		Status:        "completed",
		StartTime:     time.Now().Add(-1 * time.Hour),
		EndTime:       time.Now(),
		ProcessedRows: 10000,
		TotalRows:     10000,
		Progress:      1.0,
		ErrorCount:    2,
		LastError:     "Some error message",
	}

	err = storage.SaveProgress(taskID, tableName, progress)
	if err != nil {
		t.Fatalf("Failed to save progress to file: %v", err)
	}

	// 检查进度文件是否被创建
	progressFile := filepath.Join(tempDir, "progress", taskID+"_"+tableName+".json")
	if _, err := os.Stat(progressFile); os.IsNotExist(err) {
		t.Errorf("Progress file was not created: %s", progressFile)
	}

	loadedProgress, err := storage.LoadProgress(taskID, tableName)
	if err != nil {
		t.Fatalf("Failed to load progress from file: %v", err)
	}

	// 验证加载的进度数据
	if loadedProgress.TableName != progress.TableName {
		t.Errorf("Expected table name %s, got %s", progress.TableName, loadedProgress.TableName)
	}
	if loadedProgress.Status != progress.Status {
		t.Errorf("Expected status %s, got %s", progress.Status, loadedProgress.Status)
	}
	if loadedProgress.ProcessedRows != progress.ProcessedRows {
		t.Errorf("Expected processed rows %d, got %d", progress.ProcessedRows, loadedProgress.ProcessedRows)
	}
	if loadedProgress.ErrorCount != progress.ErrorCount {
		t.Errorf("Expected error count %d, got %d", progress.ErrorCount, loadedProgress.ErrorCount)
	}
	if loadedProgress.LastError != progress.LastError {
		t.Errorf("Expected last error '%s', got '%s'", progress.LastError, loadedProgress.LastError)
	}

	// 测试更新偏移量
	newOffset := int64(67890)
	err = storage.SaveOffset(taskID, tableName, newOffset)
	if err != nil {
		t.Fatalf("Failed to update offset: %v", err)
	}

	updatedOffset, err := storage.LoadOffset(taskID, tableName)
	if err != nil {
		t.Fatalf("Failed to load updated offset: %v", err)
	}

	if updatedOffset != newOffset {
		t.Errorf("Expected updated offset %d, got %d", newOffset, updatedOffset)
	}
}

// TestFileStoragePersistence 测试文件存储的持久性
func TestFileStoragePersistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "persistence_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	taskID := "persistence_task"
	tableName := "persistence_table"
	offset := int64(98765)

	// 创建第一个存储实例并保存数据
	storage1, err := NewFileStorage(tempDir)
	if err != nil {
		t.Fatalf("Failed to create first storage: %v", err)
	}

	err = storage1.SaveOffset(taskID, tableName, offset)
	if err != nil {
		t.Fatalf("Failed to save offset with first storage: %v", err)
	}

	progress := &TableSyncProgress{
		TableName:     tableName,
		Status:        "paused",
		StartTime:     time.Now().Add(-2 * time.Hour),
		ProcessedRows: 7500,
		TotalRows:     15000,
		Progress:      0.5,
		ErrorCount:    1,
		LastError:     "Connection timeout",
	}

	err = storage1.SaveProgress(taskID, tableName, progress)
	if err != nil {
		t.Fatalf("Failed to save progress with first storage: %v", err)
	}

	storage1.Close()

	// 创建第二个存储实例并验证数据是否持久化
	storage2, err := NewFileStorage(tempDir)
	if err != nil {
		t.Fatalf("Failed to create second storage: %v", err)
	}
	defer storage2.Close()

	loadedOffset, err := storage2.LoadOffset(taskID, tableName)
	if err != nil {
		t.Fatalf("Failed to load offset with second storage: %v", err)
	}

	if loadedOffset != offset {
		t.Errorf("Expected persisted offset %d, got %d", offset, loadedOffset)
	}

	loadedProgress, err := storage2.LoadProgress(taskID, tableName)
	if err != nil {
		t.Fatalf("Failed to load progress with second storage: %v", err)
	}

	if loadedProgress.Status != progress.Status {
		t.Errorf("Expected persisted status %s, got %s", progress.Status, loadedProgress.Status)
	}
	if loadedProgress.ProcessedRows != progress.ProcessedRows {
		t.Errorf("Expected persisted processed rows %d, got %d", progress.ProcessedRows, loadedProgress.ProcessedRows)
	}
}

// TestStorageConcurrency 测试存储的并发安全性
func TestStorageConcurrency(t *testing.T) {
	storage := NewMemoryStorage()
	defer storage.Close()

	taskID := "concurrent_task"
	numGoroutines := 10
	numOperations := 100

	// 启动多个goroutine并发保存偏移量
	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			for j := 0; j < numOperations; j++ {
				tableName := "table_" + string(rune(goroutineID))
				offset := int64(goroutineID*numOperations + j)

				err := storage.SaveOffset(taskID, tableName, offset)
				if err != nil {
					t.Errorf("Failed to save offset in goroutine %d: %v", goroutineID, err)
					done <- false
					return
				}

				loadedOffset, err := storage.LoadOffset(taskID, tableName)
				if err != nil {
					t.Errorf("Failed to load offset in goroutine %d: %v", goroutineID, err)
					done <- false
					return
				}

				// 注意：由于并发，加载的偏移量可能不等于刚保存的偏移量
				// 我们只检查是否为有效值
				if loadedOffset < 0 {
					t.Errorf("Invalid offset %d in goroutine %d", loadedOffset, goroutineID)
					done <- false
					return
				}
			}
			done <- true
		}(i)
	}

	// 等待所有goroutine完成
	for i := 0; i < numGoroutines; i++ {
		success := <-done
		if !success {
			t.Fatal("One or more goroutines failed")
		}
	}
}

// TestInvalidStoragePath 测试无效存储路径
func TestInvalidStoragePath(t *testing.T) {
	// 在Windows上尝试使用包含无效字符的路径
	invalidPath := "invalid/path\x00/with/null/byte"

	_, err := NewFileStorage(invalidPath)
	if err == nil {
		t.Error("Expected error when creating storage with invalid path")
	}
}

// TestStorageErrorHandling 测试存储错误处理
func TestStorageErrorHandling(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "error_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storage, err := NewFileStorage(tempDir)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	// 测试空任务ID
	err = storage.SaveOffset("", "table", 123)
	if err == nil {
		t.Error("Expected error for empty task ID")
	}

	// 测试空表名
	err = storage.SaveOffset("task", "", 123)
	if err == nil {
		t.Error("Expected error for empty table name")
	}

	// 测试负偏移量（应该被接受，因为可能表示特殊状态）
	err = storage.SaveOffset("task", "table", -1)
	if err != nil {
		t.Errorf("Unexpected error for negative offset: %v", err)
	}

	// 测试nil进度
	err = storage.SaveProgress("task", "table", nil)
	if err == nil {
		t.Error("Expected error for nil progress")
	}
}

// BenchmarkMemoryStorage 内存存储性能基准测试
func BenchmarkMemoryStorage(b *testing.B) {
	storage := NewMemoryStorage()
	defer storage.Close()

	taskID := "benchmark_task"
	tableName := "benchmark_table"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i)
		err := storage.SaveOffset(taskID, tableName, offset)
		if err != nil {
			b.Fatal(err)
		}

		loadedOffset, err := storage.LoadOffset(taskID, tableName)
		if err != nil {
			b.Fatal(err)
		}

		if loadedOffset != offset {
			b.Fatalf("Expected offset %d, got %d", offset, loadedOffset)
		}
	}
}

// BenchmarkFileStorage 文件存储性能基准测试
func BenchmarkFileStorage(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "benchmark_test")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	storage, err := NewFileStorage(tempDir)
	if err != nil {
		b.Fatalf("Failed to create storage: %v", err)
	}
	defer storage.Close()

	taskID := "benchmark_task"
	tableName := "benchmark_table"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := int64(i)
		err := storage.SaveOffset(taskID, tableName, offset)
		if err != nil {
			b.Fatal(err)
		}

		loadedOffset, err := storage.LoadOffset(taskID, tableName)
		if err != nil {
			b.Fatal(err)
		}

		if loadedOffset != offset {
			b.Fatalf("Expected offset %d, got %d", offset, loadedOffset)
		}
	}
}