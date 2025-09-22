#!/bin/bash

# srtool单元测试运行脚本
# 使用mock技术测试数据生成和同步功能

set -e

echo "=== srtool Mock单元测试 ==="
echo "测试开始时间: $(date)"
echo

# 检查Go环境
if ! command -v go &> /dev/null; then
    echo "❌ Go环境未安装，请先安装Go"
    exit 1
fi

echo "✅ Go版本: $(go version)"
echo

# 进入测试目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

echo "📁 测试目录: $SCRIPT_DIR"
echo

# 运行所有单元测试
echo "🧪 运行所有单元测试..."
echo "----------------------------------------"

# 基础功能测试
echo "1️⃣ 运行数据生成功能测试..."
go test -v -run TestGenerateDataCommand ./... 2>&1 | tee generate_data_test_output.log

if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo "✅ 数据生成测试通过"
else
    echo "❌ 数据生成测试失败"
    echo "详细错误信息请查看: generate_data_test_output.log"
fi
echo

echo "2️⃣ 运行数据同步功能测试..."
go test -v -run TestSyncCommand ./... 2>&1 | tee sync_test_output.log

if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo "✅ 数据同步测试通过"
else
    echo "❌ 数据同步测试失败"
    echo "详细错误信息请查看: sync_test_output.log"
fi
echo

# 运行所有测试
echo "3️⃣ 运行完整测试套件..."
go test -v ./... 2>&1 | tee full_test_output.log

if [ ${PIPESTATUS[0]} -eq 0 ]; then
    echo "✅ 完整测试套件通过"
else
    echo "❌ 部分测试失败"
    echo "详细错误信息请查看: full_test_output.log"
fi
echo

# 运行性能基准测试
echo "4️⃣ 运行性能基准测试..."
go test -bench=. -benchmem ./... 2>&1 | tee benchmark_output.log

echo "📊 性能基准测试完成，结果已保存到: benchmark_output.log"
echo

# 生成测试覆盖率报告
echo "5️⃣ 生成测试覆盖率报告..."
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html

if [ -f coverage.html ]; then
    echo "📈 测试覆盖率报告已生成: coverage.html"
else
    echo "⚠️  测试覆盖率报告生成失败"
fi
echo

# 检查代码质量
echo "6️⃣ 运行代码质量检查..."

# go vet检查
echo "   🔍 运行go vet..."
go vet ./...
if [ $? -eq 0 ]; then
    echo "   ✅ go vet检查通过"
else
    echo "   ❌ go vet检查发现问题"
fi

# golint检查（如果安装了）
if command -v golint &> /dev/null; then
    echo "   🔍 运行golint..."
    golint ./...
    echo "   ✅ golint检查完成"
else
    echo "   ⚠️  golint未安装，跳过检查"
fi

echo

# 生成测试报告摘要
echo "📋 测试报告摘要"
echo "========================================"
echo "测试文件:"
echo "  - generate_data_test.go: 数据生成功能测试"
echo "  - sync_test.go: 数据同步功能测试"
echo "  - benchmark_test.go: 性能基准测试"
echo "  - mock_interfaces.go: Mock接口定义"
echo "  - mock_client.go: Mock客户端实现"
echo
echo "生成的文件:"
echo "  - generate_data_test_output.log: 数据生成测试日志"
echo "  - sync_test_output.log: 数据同步测试日志"
echo "  - full_test_output.log: 完整测试日志"
echo "  - benchmark_output.log: 性能基准测试结果"
echo "  - coverage.out: 测试覆盖率数据"
echo "  - coverage.html: 测试覆盖率HTML报告"
echo

echo "测试完成时间: $(date)"
echo "=== 测试完成 ==="