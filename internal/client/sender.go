package client

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/loglugger/internal/models"
)

// Sender sends batches to the server via HTTP.
type Sender interface {
	Send(ctx context.Context, req *models.BatchRequest) (*models.BatchResponse, error)
	CurrentPosition(ctx context.Context) (*models.PositionResponse, error)
}

type sender struct {
	endpoints  []senderEndpoint
	clientID   string
	retryDelay time.Duration
	nextIndex  uint64
}

const maxRetryBackoff = time.Minute

type senderEndpoint struct {
	client      *http.Client
	baseURL     string
	batchesURL  string
	positionURL string
}

// SenderConfig configures the sender.
type SenderConfig struct {
	ServerURLs  []string
	ClientID    string
	HTTPTimeout time.Duration
	RetryDelay  time.Duration
	TLSConfig   *tls.Config
}

// NewSender creates a sender.
func NewSender(cfg SenderConfig) Sender {
	endpoints := make([]senderEndpoint, 0, len(cfg.ServerURLs))
	for _, raw := range cfg.ServerURLs {
		baseURL := strings.TrimSuffix(raw, "/")
		transport := &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
			ForceAttemptHTTP2:   true,
		}
		if cfg.TLSConfig != nil {
			tlsCfg := cfg.TLSConfig.Clone()
			if parsed, err := url.Parse(baseURL); err == nil {
				tlsCfg.ServerName = parsed.Hostname()
			}
			transport.TLSClientConfig = tlsCfg
		}
		httpClient := &http.Client{
			Timeout:   cfg.HTTPTimeout,
			Transport: transport,
		}
		endpoints = append(endpoints, senderEndpoint{
			client:      httpClient,
			baseURL:     baseURL,
			batchesURL:  baseURL + "/v1/batches",
			positionURL: baseURL + "/v1/positions",
		})
	}
	return &sender{
		endpoints:  endpoints,
		clientID:   cfg.ClientID,
		retryDelay: cfg.RetryDelay,
	}
}

func (s *sender) Send(ctx context.Context, req *models.BatchRequest) (*models.BatchResponse, error) {
	if len(s.endpoints) == 0 {
		return nil, fmt.Errorf("no server endpoints configured")
	}
	req.ClientID = s.clientID
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	compressedBody, err := compressJSON(body)
	if err != nil {
		return nil, fmt.Errorf("compress request: %w", err)
	}

	for attempt := 0; ; attempt++ {
		if err := sleepForRetry(ctx, s.retryDelay, attempt); err != nil {
			return nil, err
		}

		endpoint, endpointIdx := s.currentEndpoint()
		slog.Debug("send attempt", "attempt", attempt+1, "endpoint", endpoint.baseURL)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.batchesURL, bytes.NewReader(compressedBody))
		if err != nil {
			return nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Content-Encoding", "gzip")

		resp, err := endpoint.client.Do(httpReq)
		if err != nil {
			slog.Debug("send failed", "attempt", attempt+1, "endpoint", endpoint.baseURL, "error", err)
			s.advanceStartIndexOnFailure(endpointIdx)
			continue
		}

		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var batchResp models.BatchResponse
		if err := json.Unmarshal(respBody, &batchResp); err != nil {
			batchResp = models.BatchResponse{Status: "error", Message: string(respBody)}
		}

		switch resp.StatusCode {
		case http.StatusOK:
			s.markSuccess(endpointIdx)
			return &batchResp, nil
		case http.StatusConflict:
			s.markSuccess(endpointIdx)
			return &batchResp, nil
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
			return &batchResp, ErrClientError{Message: batchResp.Message}
		default:
			if resp.StatusCode >= 500 {
				slog.Debug("send got retriable HTTP status", "attempt", attempt+1, "endpoint", endpoint.baseURL, "status_code", resp.StatusCode, "message", batchResp.Message)
				s.advanceStartIndexOnFailure(endpointIdx)
				continue
			}
			return &batchResp, fmt.Errorf("HTTP %d: %s", resp.StatusCode, batchResp.Message)
		}
	}
}

func compressJSON(body []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(body); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *sender) CurrentPosition(ctx context.Context) (*models.PositionResponse, error) {
	if len(s.endpoints) == 0 {
		return nil, fmt.Errorf("no server endpoints configured")
	}
	for attempt := 0; ; attempt++ {
		if err := sleepForRetry(ctx, s.retryDelay, attempt); err != nil {
			return nil, err
		}

		endpoint, endpointIdx := s.currentEndpoint()
		slog.Debug("fetch position attempt", "attempt", attempt+1, "endpoint", endpoint.baseURL)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.positionURL+"?client_id="+url.QueryEscape(s.clientID), nil)
		if err != nil {
			return nil, err
		}

		resp, err := endpoint.client.Do(req)
		if err != nil {
			slog.Debug("fetch position failed", "attempt", attempt+1, "endpoint", endpoint.baseURL, "error", err)
			s.advanceStartIndexOnFailure(endpointIdx)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var positionResp models.PositionResponse
		if err := json.Unmarshal(body, &positionResp); err != nil {
			slog.Debug("decode position response failed", "attempt", attempt+1, "endpoint", endpoint.baseURL, "error", err)
			s.advanceStartIndexOnFailure(endpointIdx)
			continue
		}

		switch resp.StatusCode {
		case http.StatusOK:
			s.markSuccess(endpointIdx)
			return &positionResp, nil
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
			return &positionResp, ErrClientError{Message: positionResp.Message}
		default:
			if resp.StatusCode >= 500 {
				slog.Debug("fetch position got retriable HTTP status", "attempt", attempt+1, "endpoint", endpoint.baseURL, "status_code", resp.StatusCode, "message", positionResp.Message)
				s.advanceStartIndexOnFailure(endpointIdx)
				continue
			}
			return &positionResp, fmt.Errorf("HTTP %d: %s", resp.StatusCode, positionResp.Message)
		}
	}
}

func sleepForRetry(ctx context.Context, baseDelay time.Duration, attempt int) error {
	if attempt == 0 {
		return nil
	}
	delay := retryDelayForAttempt(baseDelay, attempt)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}

func retryDelayForAttempt(baseDelay time.Duration, attempt int) time.Duration {
	if baseDelay <= 0 {
		return 0
	}
	delay := baseDelay
	for i := 1; i < attempt; i++ {
		if delay >= maxRetryBackoff/2 {
			return maxRetryBackoff
		}
		delay *= 2
	}
	if delay > maxRetryBackoff {
		return maxRetryBackoff
	}
	return delay
}

func (s *sender) currentEndpoint() (senderEndpoint, int) {
	idx := int(atomic.LoadUint64(&s.nextIndex)) % len(s.endpoints)
	return s.endpoints[idx], idx
}

func (s *sender) advanceStartIndexOnFailure(failedIndex int) {
	if len(s.endpoints) == 0 {
		return
	}
	next := uint64((failedIndex + 1) % len(s.endpoints))
	atomic.StoreUint64(&s.nextIndex, next)
}

func (s *sender) markSuccess(successIndex int) {
	// Re-prefer the first configured endpoint after any successful call.
	// This allows automatic failback once the primary endpoint recovers.
	if successIndex == 0 {
		return
	}
	atomic.StoreUint64(&s.nextIndex, 0)
}

// ErrClientError indicates a client error (4xx) that should not be retried.
type ErrClientError struct{ Message string }

func (e ErrClientError) Error() string { return "client error: " + e.Message }
