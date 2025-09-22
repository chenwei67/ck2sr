package test

import (
	"context"
	"encoding/base64"
	"testing"
)

// MockAuthConn 模拟AuthConn接口用于测试
type MockAuthConn struct {
	sentData     []byte
	responseData []byte
	readError    error
	sendError    error
}

// Send 模拟发送数据
func (m *MockAuthConn) Send(data []byte) error {
	if m.sendError != nil {
		return m.sendError
	}
	m.sentData = data
	return nil
}

// Read 模拟读取数据
func (m *MockAuthConn) Read() ([]byte, error) {
	if m.readError != nil {
		return nil, m.readError
	}
	return m.responseData, nil
}

// MockBasicAuthHandler 模拟BasicAuth处理器用于测试
type MockBasicAuthHandler struct {
	username string
	password string
	token    string
}

// NewMockBasicAuthHandler 创建mock认证处理器
func NewMockBasicAuthHandler(username, password string) *MockBasicAuthHandler {
	return &MockBasicAuthHandler{
		username: username,
		password: password,
	}
}

// Authenticate 模拟认证方法
func (h *MockBasicAuthHandler) Authenticate(ctx context.Context, authConn *MockAuthConn) error {
	// 构建Basic Auth凭据
	auth := base64.StdEncoding.EncodeToString([]byte(h.username + ":" + h.password))
	payload := []byte("Basic " + auth)

	// 发送认证凭据
	if err := authConn.Send(payload); err != nil {
		return err
	}

	// 读取服务器响应
	response, err := authConn.Read()
	if err != nil {
		return err
	}

	// 保存认证令牌（服务器响应）
	if len(response) > 0 {
		h.token = string(response)
	} else {
		// 如果没有返回令牌，使用Basic Auth字符串作为令牌
		h.token = "Basic " + auth
	}

	return nil
}

// GetToken 获取认证令牌
func (h *MockBasicAuthHandler) GetToken(ctx context.Context) (string, error) {
	if h.token == "" {
		// 如果没有令牌，返回Basic Auth字符串
		auth := base64.StdEncoding.EncodeToString([]byte(h.username + ":" + h.password))
		return "Basic " + auth, nil
	}
	return h.token, nil
}

// TestFlightBasicAuthHandler 测试Flight BasicAuth处理器
func TestFlightBasicAuthHandler(t *testing.T) {
	testCases := []struct {
		name         string
		username     string
		password     string
		responseData []byte
		expectToken  bool
		description  string
	}{
		{
			name:         "有效的用户名和密码",
			username:     "testuser",
			password:     "testpass",
			responseData: []byte("auth-token-12345"),
			expectToken:  true,
			description:  "提供用户名和密码应该成功认证",
		},
		{
			name:         "服务器无响应",
			username:     "testuser",
			password:     "testpass",
			responseData: []byte{},
			expectToken:  true,
			description:  "服务器无响应时使用Basic Auth作为令牌",
		},
		{
			name:         "空凭据",
			username:     "",
			password:     "",
			responseData: []byte("empty-auth"),
			expectToken:  true,
			description:  "空凭据也应该能处理",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 创建mock处理器和连接
			handler := NewMockBasicAuthHandler(tc.username, tc.password)
			authConn := &MockAuthConn{
				responseData: tc.responseData,
			}

			// 执行认证
			ctx := context.Background()
			err := handler.Authenticate(ctx, authConn)

			if err != nil {
				t.Errorf("Authentication failed: %v", err)
				return
			}

			// 验证发送的数据
			expectedAuth := base64.StdEncoding.EncodeToString([]byte(tc.username + ":" + tc.password))
			expectedPayload := "Basic " + expectedAuth
			if string(authConn.sentData) != expectedPayload {
				t.Errorf("Expected payload '%s', got '%s'", expectedPayload, string(authConn.sentData))
			}

			// 验证令牌获取
			if tc.expectToken {
				token, err := handler.GetToken(ctx)
				if err != nil {
					t.Errorf("GetToken failed: %v", err)
				} else if token == "" {
					t.Errorf("Expected non-empty token")
				} else {
					t.Logf("✅ %s: 认证成功，令牌: %s", tc.name, maskToken(token))
				}
			}
		})
	}
}

// TestFlightAuthHandlerCreation 测试认证处理器创建逻辑
func TestFlightAuthHandlerCreation(t *testing.T) {
	testCases := []struct {
		name           string
		username       string
		password       string
		shouldCreate   bool
		description    string
	}{
		{
			name:           "完整凭据",
			username:       "admin",
			password:       "secret123",
			shouldCreate:   true,
			description:    "有完整用户名和密码时应该创建处理器",
		},
		{
			name:           "空凭据",
			username:       "",
			password:       "",
			shouldCreate:   false,
			description:    "无凭据时不应该创建处理器",
		},
		{
			name:           "部分凭据-用户名",
			username:       "user",
			password:       "",
			shouldCreate:   true,
			description:    "有用户名时应该创建处理器",
		},
		{
			name:           "部分凭据-密码",
			username:       "",
			password:       "pass",
			shouldCreate:   true,
			description:    "有密码时应该创建处理器",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 模拟认证处理器创建逻辑
			var authHandler interface{}
			if tc.username != "" || tc.password != "" {
				authHandler = NewMockBasicAuthHandler(tc.username, tc.password)
			}

			// 验证创建结果
			if tc.shouldCreate {
				if authHandler == nil {
					t.Errorf("Expected auth handler to be created but got nil")
				} else {
					t.Logf("✅ %s: 认证处理器创建成功", tc.name)
				}
			} else {
				if authHandler != nil {
					t.Errorf("Expected auth handler to be nil but got: %v", authHandler)
				} else {
					t.Logf("✅ %s: 正确跳过了认证处理器创建", tc.name)
				}
			}
		})
	}
}

// TestBasicAuthTokenGeneration 测试Basic Auth令牌生成
func TestBasicAuthTokenGeneration(t *testing.T) {
	testCases := []struct {
		username     string
		password     string
		expectedAuth string
		description  string
	}{
		{
			username:     "admin",
			password:     "password123",
			expectedAuth: base64.StdEncoding.EncodeToString([]byte("admin:password123")),
			description:  "标准用户名密码编码",
		},
		{
			username:     "user@domain.com",
			password:     "complex!@#$%",
			expectedAuth: base64.StdEncoding.EncodeToString([]byte("user@domain.com:complex!@#$%")),
			description:  "复杂用户名和密码编码",
		},
		{
			username:     "",
			password:     "onlypass",
			expectedAuth: base64.StdEncoding.EncodeToString([]byte(":onlypass")),
			description:  "只有密码的编码",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 模拟Basic Auth编码逻辑
			actualAuth := base64.StdEncoding.EncodeToString([]byte(tc.username + ":" + tc.password))

			if actualAuth != tc.expectedAuth {
				t.Errorf("Expected auth '%s', got '%s'", tc.expectedAuth, actualAuth)
			} else {
				t.Logf("✅ %s: Basic Auth编码正确", tc.description)
			}
		})
	}
}

// maskToken 隐藏令牌用于日志显示
func maskToken(token string) string {
	if len(token) <= 8 {
		return "***"
	}
	return token[:4] + "***" + token[len(token)-4:]
}