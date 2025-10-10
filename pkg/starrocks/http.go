package starrocks

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sunkaimr/ck2sr/pkg/protocol"
)

type HTTPClient struct {
	config     *HTTPConfig
	httpClient *http.Client
	logger     *logrus.Logger
	baseURL    string
}

func NewHTTPClient(config *HTTPConfig, logger *logrus.Logger) (*HTTPClient, error) {
	baseURL := fmt.Sprintf("http://%s:%d", config.Host, config.Port)

	// 优化连接池配置：
	// 1. MaxIdleConnsPerHost：控制空闲连接数，避免资源浪费
	// 2. MaxConnsPerHost：限制单主机总连接数，防止连接数爆炸
	// 3. IdleConnTimeout：空闲连接超时，释放无用连接
	// 4. DisableKeepAlives：设为false，启用连接复用提高性能
	httpClient := &http.Client{
		Timeout: config.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,              // 全局最大空闲连接（所有主机）
			MaxIdleConnsPerHost: 50,               // 单主机最大空闲连接（根据WriterConcurrency调整）
			MaxConnsPerHost:     100,              // 单主机最大总连接（防止连接数爆炸）
			IdleConnTimeout:     90 * time.Second, // 空闲连接超时
			DisableKeepAlives:   false,            // 启用连接复用
			// TLS 和超时配置
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 30 * time.Second, // 响应头超时
			ExpectContinueTimeout: 1 * time.Second,  // Expect: 100-continue 超时
			TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
		},
		// 解决StarRocks的FE重定向到BE时认证丢失问题
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			SetStarRocksStreamLoadHeaders(req, config.Username, config.Password)
			return nil // 返回 nil 表示信任所有重定向
		},
	}

	return &HTTPClient{
		config:     config,
		httpClient: httpClient,
		logger:     logger,
		baseURL:    baseURL,
	}, nil
}

// getNewClient 已废弃：每次创建新Client导致连接池隔离，引发连接数爆炸和死锁问题
// 现在直接使用共享的 c.httpClient 实例，确保连接池配置生效
// func (c *HTTPClient) getNewClient() *http.Client { ... }

type StreamLoadResponse struct {
	TxnID                  int64  `json:"TxnId"`
	Label                  string `json:"Label"`
	Status                 string `json:"Status"`
	Message                string `json:"Message"`
	NumberTotalRows        int64  `json:"NumberTotalRows"`
	NumberLoadedRows       int64  `json:"NumberLoadedRows"`
	NumberFilteredRows     int64  `json:"NumberFilteredRows"`
	NumberUnselectedRows   int64  `json:"NumberUnselectedRows"`
	LoadBytes              int64  `json:"LoadBytes"`
	LoadTimeMs             int64  `json:"LoadTimeMs"`
	BeginTxnTimeMs         int64  `json:"BeginTxnTimeMs"`
	StreamLoadPutTimeMs    int64  `json:"StreamLoadPutTimeMs"`
	ReadDataTimeMs         int64  `json:"ReadDataTimeMs"`
	WriteDataTimeMs        int64  `json:"WriteDataTimeMs"`
	CommitAndPublishTimeMs int64  `json:"CommitAndPublishTimeMs"`
}

func SetStarRocksStreamLoadHeaders(req *http.Request, username, password string) {
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("format", "json")
	req.Header.Set("strip_outer_array", "true") // 剥离JSON数组的外层数组包装
	req.Header.Set("ignore_json_size", "true")
	req.Header.Set("fuzzy_parse", "true") // 启用模糊解析以提高容错性
	// 移除可能导致冲突的头部设置
	req.Header.Set("compress_type", "lz4_frame") // 暂时禁用压缩避免问题
	req.Header.Set("num_as_string", "true")      // 移除强制数字为字符串，让StarRocks自动推断类型
	// 设置认证
	req.SetBasicAuth(username, password)
}

func (c *HTTPClient) StreamLoad(ctx context.Context, options *StreamLoadOptions, data *bytes.Buffer) (*StreamLoadResponse, error) {
	url := fmt.Sprintf("%s/api/%s/%s/_stream_load", c.baseURL, options.Database, options.Table)

	// 为请求添加超时保护，防止永久阻塞
	// 使用父context的超时，如果未设置则默认使用60秒
	requestCtx := ctx
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		requestCtx, cancel = context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(requestCtx, "PUT", url, data)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// 设置必要的头部
	SetStarRocksStreamLoadHeaders(req, c.config.Username, c.config.Password)

	if options.Label != "" {
		req.Header.Set("label", options.Label)
	}

	if options.MaxBytes > 0 {
		req.Header.Set("max_filter_ratio", "0.0")
	}

	for k, v := range options.Properties {
		req.Header.Set(k, v)
	}

	c.logger.Debugf("StreamLoad request: URL=%s, Headers=%+v", url, req.Header)

	// 使用共享的 httpClient 实例，确保连接池配置生效
	// 避免每次创建新Client导致连接池隔离和连接数爆炸
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute stream load: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("stream load failed with status %d: %s", resp.StatusCode, string(body))
	}

	var loadResp StreamLoadResponse
	if err := json.Unmarshal(body, &loadResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	c.logger.Infof("Stream load response: %+v", loadResp)
	if loadResp.Status != "Success" && loadResp.Status != "Publish Timeout" {
		return &loadResp, fmt.Errorf("stream load failed: %s - %s", loadResp.Status, loadResp.Message)
	}

	return &loadResp, nil
}

// StreamLoadRecords 使用Stream Load写入记录列表
// records: 记录列表，每个记录为interface{}类型（通常为map[string]interface{}）
func (c *HTTPClient) StreamLoadRecords(ctx context.Context, options *StreamLoadOptions, records []interface{}) (*StreamLoadResponse, error) {
	if options.Format == "" {
		options.Format = "json"
	}

	var data []byte
	var err error

	switch options.Format {
	case "json":
		data, err = json.Marshal(records)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal records to json: %w", err)
		}
	case "csv":
		return nil, fmt.Errorf("CSV format not yet implemented")
	default:
		return nil, fmt.Errorf("unsupported format: %s", options.Format)
	}

	return c.StreamLoad(ctx, options, bytes.NewBuffer(data))
}

// StreamLoadBatch 批量写入记录
// records: 记录列表，每个记录为interface{}类型（通常为map[string]interface{}）
func (c *HTTPClient) StreamLoadBatch(ctx context.Context, options *StreamLoadOptions, records []interface{}, batchSize int) (*protocol.Stats, error) {
	stats := &protocol.Stats{
		TotalRows: int64(len(records)),
	}

	startTime := time.Now()

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		resp, err := c.StreamLoadRecords(ctx, options, batch)
		if err != nil {
			return stats, fmt.Errorf("failed to load batch %d-%d: %w", i, end, err)
		}

		stats.TotalBytes += resp.LoadBytes
	}

	stats.Duration = time.Since(startTime)
	if stats.Duration.Seconds() > 0 {
		stats.RowsPerSecond = float64(stats.TotalRows) / stats.Duration.Seconds()
		stats.BytesPerSecond = float64(stats.TotalBytes) / stats.Duration.Seconds()
	}

	return stats, nil
}

func (c *HTTPClient) Close() error {
	if c.httpClient != nil {
		c.httpClient.CloseIdleConnections()
	}
	return nil
}
