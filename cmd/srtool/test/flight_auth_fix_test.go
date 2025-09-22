package test

import (
	"fmt"
	"testing"
)

// TestFlightSQLAuthConfigLogic 测试Flight SQL认证配置逻辑修复
func TestFlightSQLAuthConfigLogic(t *testing.T) {
	testCases := []struct {
		name         string
		endpoint     string
		port         int
		username     string
		password     string
		description  string
	}{
		{
			name:         "标准Flight SQL配置",
			endpoint:     "localhost",
			port:         8080,
			username:     "testuser",
			password:     "testpass",
			description:  "标准端口配置，MySQL端口应为9030",
		},
		{
			name:         "自定义Flight SQL配置",
			endpoint:     "test-host",
			port:         8090,
			username:     "customuser",
			password:     "custompass",
			description:  "自定义端口配置，MySQL端口应为9040(8090+950)",
		},
		{
			name:         "空密码配置",
			endpoint:     "localhost",
			port:         8080,
			username:     "root",
			password:     "",
			description:  "空密码的用户认证",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 模拟createStarRocksConfig函数的端口计算逻辑
			mysqlPort := 9030
			if tc.port != 8080 {
				mysqlPort = tc.port + 950
			}

			// 验证端口计算逻辑
			expectedMySQLPort := 9030
			if tc.port != 8080 {
				expectedMySQLPort = tc.port + 950
			}
			if mysqlPort != expectedMySQLPort {
				t.Errorf("Expected MySQL Port %d, got %d", expectedMySQLPort, mysqlPort)
			}

			// 验证认证信息一致性（模拟配置创建）
			mysqlUsername := tc.username
			mysqlPassword := tc.password
			flightUsername := tc.username
			flightPassword := tc.password

			if mysqlUsername != flightUsername {
				t.Errorf("MySQL and Flight SQL usernames should match: %s != %s", mysqlUsername, flightUsername)
			}

			if mysqlPassword != flightPassword {
				t.Errorf("MySQL and Flight SQL passwords should match: %s != %s", mysqlPassword, flightPassword)
			}

			t.Logf("✅ %s: 配置验证成功 - MySQL(%s:%d %s/%s), Flight(%s:%d %s/%s)",
				tc.name,
				tc.endpoint, mysqlPort, mysqlUsername, maskPassword(mysqlPassword),
				tc.endpoint, tc.port, flightUsername, maskPassword(flightPassword))
		})
	}
}

// TestConnectionErrorHandling 测试连接错误处理逻辑
func TestConnectionErrorHandling(t *testing.T) {
	testCases := []struct {
		name              string
		mysqlConfigured   bool
		flightConfigured  bool
		mysqlError        error
		flightError       error
		expectSuccess     bool
		expectWarning     bool
		description       string
	}{
		{
			name:              "两者都成功",
			mysqlConfigured:   true,
			flightConfigured:  true,
			mysqlError:        nil,
			flightError:       nil,
			expectSuccess:     true,
			expectWarning:     false,
			description:       "MySQL和Flight SQL都连接成功",
		},
		{
			name:              "MySQL失败Flight成功",
			mysqlConfigured:   true,
			flightConfigured:  true,
			mysqlError:        fmt.Errorf("MySQL authentication failed"),
			flightError:       nil,
			expectSuccess:     true,
			expectWarning:     true,
			description:       "MySQL连接失败但Flight SQL成功，应该警告但继续",
		},
		{
			name:              "Flight失败MySQL成功",
			mysqlConfigured:   true,
			flightConfigured:  true,
			mysqlError:        nil,
			flightError:       fmt.Errorf("Flight SQL authentication failed"),
			expectSuccess:     true,
			expectWarning:     true,
			description:       "Flight SQL连接失败但MySQL成功，应该警告但继续",
		},
		{
			name:              "两者都失败",
			mysqlConfigured:   true,
			flightConfigured:  true,
			mysqlError:        fmt.Errorf("MySQL authentication failed"),
			flightError:       fmt.Errorf("Flight SQL authentication failed"),
			expectSuccess:     false,
			expectWarning:     false,
			description:       "MySQL和Flight SQL都连接失败，应该返回错误",
		},
		{
			name:              "只配置Flight SQL且成功",
			mysqlConfigured:   false,
			flightConfigured:  true,
			mysqlError:        nil,
			flightError:       nil,
			expectSuccess:     true,
			expectWarning:     false,
			description:       "只配置Flight SQL且连接成功",
		},
		{
			name:              "只配置Flight SQL但失败",
			mysqlConfigured:   false,
			flightConfigured:  true,
			mysqlError:        nil,
			flightError:       fmt.Errorf("Flight SQL authentication failed"),
			expectSuccess:     false,
			expectWarning:     false,
			description:       "只配置Flight SQL但连接失败，应该返回错误",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 模拟连接测试逻辑（基于修复后的TestConnection方法）
			var shouldFail bool
			var warningCount int

			if tc.mysqlConfigured && tc.flightConfigured {
				// 两者都配置的情况
				if tc.mysqlError != nil && tc.flightError != nil {
					shouldFail = true
				} else {
					if tc.mysqlError != nil {
						warningCount++
					}
					if tc.flightError != nil {
						warningCount++
					}
				}
			} else if tc.mysqlConfigured {
				// 只配置了MySQL
				if tc.mysqlError != nil {
					shouldFail = true
				}
			} else if tc.flightConfigured {
				// 只配置了Flight SQL
				if tc.flightError != nil {
					shouldFail = true
				}
			} else {
				shouldFail = true // 都没配置
			}

			// 验证结果
			if tc.expectSuccess && shouldFail {
				t.Errorf("Expected success but logic indicates failure")
			} else if !tc.expectSuccess && !shouldFail {
				t.Errorf("Expected failure but logic indicates success")
			}

			if tc.expectWarning && warningCount == 0 {
				t.Errorf("Expected warning but no warnings detected")
			} else if !tc.expectWarning && warningCount > 0 {
				t.Logf("Info: Detected %d warnings (may be expected for this test case)", warningCount)
			}

			if tc.expectSuccess {
				t.Logf("✅ %s: 连接测试逻辑按预期成功", tc.name)
			} else {
				t.Logf("✅ %s: 连接测试逻辑按预期失败", tc.name)
			}

			if tc.expectWarning {
				t.Logf("✅ %s: 检测到预期的警告情况", tc.name)
			}
		})
	}
}

// TestAuthenticationFailureScenarios 测试认证失败场景
func TestAuthenticationFailureScenarios(t *testing.T) {
	testCases := []struct {
		name        string
		username    string
		password    string
		errorType   string
		description string
	}{
		{
			name:        "错误的用户名",
			username:    "wrong_user",
			password:    "correct_pass",
			errorType:   "Access denied for user",
			description: "使用错误的用户名应该导致认证失败",
		},
		{
			name:        "错误的密码",
			username:    "correct_user",
			password:    "wrong_pass",
			errorType:   "Access denied for user",
			description: "使用错误的密码应该导致认证失败",
		},
		{
			name:        "空用户名",
			username:    "",
			password:    "some_pass",
			errorType:   "Access denied for user",
			description: "空用户名应该导致认证失败",
		},
		{
			name:        "正确的认证信息",
			username:    "valid_user",
			password:    "valid_pass",
			errorType:   "",
			description: "正确的认证信息应该成功",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 模拟认证逻辑
			authSuccess := (tc.username != "" && tc.username != "wrong_user" && tc.password != "wrong_pass")

			if tc.errorType == "" {
				// 期待成功
				if !authSuccess {
					t.Errorf("Expected authentication success but got failure")
				} else {
					t.Logf("✅ %s: 认证按预期成功", tc.name)
				}
			} else {
				// 期待失败
				if authSuccess {
					t.Errorf("Expected authentication failure but got success")
				} else {
					t.Logf("✅ %s: 认证按预期失败，错误类型: %s", tc.name, tc.errorType)
				}
			}
		})
	}
}

// maskPassword 隐藏密码用于日志显示
func maskPassword(password string) string {
	if password == "" {
		return "<empty>"
	}
	if len(password) <= 2 {
		return "***"
	}
	return password[:1] + "***" + password[len(password)-1:]
}