package clickhouse

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPClient HTTP 协议客户端
type HTTPClient struct {
	config     *HTTPConfig
	httpClient *http.Client
	baseURL    string
}

// NewHTTPClient 创建 HTTP 客户端
func NewHTTPClient(config *HTTPConfig) (*HTTPClient, error) {
	baseURL := fmt.Sprintf("http://%s:%d", config.Host, config.Port)

	httpClient := &http.Client{
		Timeout: config.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			IdleConnTimeout:     60 * time.Second,
			DisableCompression:  false,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}

	return &HTTPClient{
		config:     config,
		httpClient: httpClient,
		baseURL:    baseURL,
	}, nil
}

// Query 执行 HTTP 查询
func (c *HTTPClient) Query(ctx context.Context, query string, format string) ([]byte, error) {
	if format == "" {
		format = "JSON"
	}

	queryWithFormat := fmt.Sprintf("%s FORMAT %s", query, format)

	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL, bytes.NewBufferString(queryWithFormat))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// 设置认证
	if c.config.Username != "" {
		req.SetBasicAuth(c.config.Username, c.config.Password)
	}

	req.Header.Set("Content-Type", "text/plain")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

// Execute 执行 HTTP 命令
func (c *HTTPClient) Execute(ctx context.Context, query string) error {
	req, err := http.NewRequestWithContext(ctx, "POST", c.baseURL, bytes.NewBufferString(query))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	if c.config.Username != "" {
		req.SetBasicAuth(c.config.Username, c.config.Password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// Close 关闭客户端
func (c *HTTPClient) Close() error {
	if c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
	return nil
}