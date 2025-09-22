@echo off
REM srtool单元测试运行脚本 (Windows版本)
REM 使用mock技术测试数据生成和同步功能

setlocal enabledelayedexpansion

echo === srtool Mock单元测试 ===
echo 测试开始时间: %DATE% %TIME%
echo.

REM 检查Go环境
where go >nul 2>&1
if errorlevel 1 (
    echo ❌ Go环境未安装，请先安装Go
    exit /b 1
)

for /f "tokens=*" %%i in ('go version') do set GO_VERSION=%%i
echo ✅ Go版本: %GO_VERSION%
echo.

REM 进入测试目录
cd /d %~dp0
echo 📁 测试目录: %CD%
echo.

REM 运行所有单元测试
echo 🧪 运行所有单元测试...
echo ----------------------------------------

REM 基础功能测试
echo 1️⃣ 运行数据生成功能测试...
go test -v -run TestGenerateDataCommand ./... > generate_data_test_output.log 2>&1

if !errorlevel! equ 0 (
    echo ✅ 数据生成测试通过
) else (
    echo ❌ 数据生成测试失败
    echo 详细错误信息请查看: generate_data_test_output.log
)
echo.

echo 2️⃣ 运行数据同步功能测试...
go test -v -run TestSyncCommand ./... > sync_test_output.log 2>&1

if !errorlevel! equ 0 (
    echo ✅ 数据同步测试通过
) else (
    echo ❌ 数据同步测试失败
    echo 详细错误信息请查看: sync_test_output.log
)
echo.

REM 运行所有测试
echo 3️⃣ 运行完整测试套件...
go test -v ./... > full_test_output.log 2>&1

if !errorlevel! equ 0 (
    echo ✅ 完整测试套件通过
) else (
    echo ❌ 部分测试失败
    echo 详细错误信息请查看: full_test_output.log
)
echo.

REM 运行性能基准测试
echo 4️⃣ 运行性能基准测试...
go test -bench=. -benchmem ./... > benchmark_output.log 2>&1

echo 📊 性能基准测试完成，结果已保存到: benchmark_output.log
echo.

REM 生成测试覆盖率报告
echo 5️⃣ 生成测试覆盖率报告...
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out -o coverage.html

if exist coverage.html (
    echo 📈 测试覆盖率报告已生成: coverage.html
) else (
    echo ⚠️  测试覆盖率报告生成失败
)
echo.

REM 检查代码质量
echo 6️⃣ 运行代码质量检查...

REM go vet检查
echo    🔍 运行go vet...
go vet ./...
if !errorlevel! equ 0 (
    echo    ✅ go vet检查通过
) else (
    echo    ❌ go vet检查发现问题
)

echo.

REM 生成测试报告摘要
echo 📋 测试报告摘要
echo ========================================
echo 测试文件:
echo   - generate_data_test.go: 数据生成功能测试
echo   - sync_test.go: 数据同步功能测试
echo   - benchmark_test.go: 性能基准测试
echo   - mock_interfaces.go: Mock接口定义
echo   - mock_client.go: Mock客户端实现
echo.
echo 生成的文件:
echo   - generate_data_test_output.log: 数据生成测试日志
echo   - sync_test_output.log: 数据同步测试日志
echo   - full_test_output.log: 完整测试日志
echo   - benchmark_output.log: 性能基准测试结果
echo   - coverage.out: 测试覆盖率数据
echo   - coverage.html: 测试覆盖率HTML报告
echo.

echo 测试完成时间: %DATE% %TIME%
echo === 测试完成 ===

pause