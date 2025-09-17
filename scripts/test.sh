#!/bin/bash

# 测试运行脚本
set -e

echo "=== ck2sr 单元测试 ==="

# 确保在项目根目录
cd "$(dirname "$0")"

# 设置 Go 环境
export GO111MODULE=on
export CGO_ENABLED=0

# 创建测试输出目录
mkdir -p test-results

echo "正在运行单元测试..."

# 运行所有测试并生成覆盖率报告
go test -v -race -coverprofile=test-results/coverage.out ./tests/... | tee test-results/test.log

# 生成覆盖率 HTML 报告
go tool cover -html=test-results/coverage.out -o test-results/coverage.html

# 显示覆盖率统计
echo ""
echo "=== 覆盖率统计 ==="
go tool cover -func=test-results/coverage.out

# 运行基准测试
echo ""
echo "=== 基准测试 ==="
go test -bench=. -benchmem ./tests/... | tee test-results/bench.log

# 运行竞态检测
echo ""
echo "=== 竞态检测 ==="
go test -race ./tests/...

echo ""
echo "测试完成! 结果保存在 test-results/ 目录中"
echo "覆盖率报告: test-results/coverage.html"