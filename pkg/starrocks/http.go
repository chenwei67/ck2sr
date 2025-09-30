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

	"github.com/sunkaimr/ck2sr/pkg/protocol"
)

type HTTPClient struct {
	config     *HTTPConfig
	httpClient *http.Client
	baseURL    string
}

func NewHTTPClient(config *HTTPConfig) (*HTTPClient, error) {
	baseURL := fmt.Sprintf("http://%s:%d", config.Host, config.Port)

	httpClient := &http.Client{
		Timeout: config.Timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	return &HTTPClient{
		config:     config,
		httpClient: httpClient,
		baseURL:    baseURL,
	}, nil
}

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

func (c *HTTPClient) StreamLoad(ctx context.Context, options *StreamLoadOptions, data *bytes.Buffer) (*StreamLoadResponse, error) {
	url := fmt.Sprintf("%s/api/%s/%s/_stream_load", c.baseURL, options.Database, options.Table)

	req, err := http.NewRequestWithContext(ctx, "PUT", url, data)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.SetBasicAuth(c.config.Username, c.config.Password)
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("format", options.Format)

	if options.Label != "" {
		req.Header.Set("label", options.Label)
	}

	if options.MaxBytes > 0 {
		req.Header.Set("max_filter_ratio", "0.0")
	}

	for k, v := range options.Properties {
		req.Header.Set(k, v)
	}

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
