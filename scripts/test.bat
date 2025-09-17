@echo off
REM Windows 测试运行脚本

echo === ck2sr 单元测试 ===

REM 确保在项目根目录
cd /d "%~dp0\.."

REM 设置 Go 环境
set GO111MODULE=on
set CGO_ENABLED=0

REM 创建测试输出目录
if not exist test-results mkdir test-results

echo 正在运行单元测试...

REM 运行所有测试并生成覆盖率报告
go test -v -race -coverprofile=test-results\coverage.out .\tests\... > test-results\test.log 2>&1

REM 显示测试结果
type test-results\test.log

REM 生成覆盖率 HTML 报告
go tool cover -html=test-results\coverage.out -o test-results\coverage.html

REM 显示覆盖率统计
echo.
echo === 覆盖率统计 ===
go tool cover -func=test-results\coverage.out

REM 运行基准测试
echo.
echo === 基准测试 ===
go test -bench=. -benchmem .\tests\... > test-results\bench.log 2>&1
type test-results\bench.log

REM 运行竞态检测
echo.
echo === 竞态检测 ===
go test -race .\tests\...

echo.
echo 测试完成! 结果保存在 test-results\ 目录中
echo 覆盖率报告: test-results\coverage.html

pause