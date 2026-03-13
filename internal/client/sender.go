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
	retryMax   int
	retryDelay time.Duration
	nextIndex  uint64
}

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
	RetryMax    int
	RetryDelay  time.Duration
	TLSConfig   *tls.Config
}

// NewSender creates a sender.
func NewSender(cfg SenderConfig) Sender {
	endpoints := make([]senderEndpoint, 0, len(cfg.ServerURLs))
	for _, raw := range cfg.ServerURLs {
		baseURL := strings.TrimSuffix(raw, "/")
		transport := &http.Transport{}
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
		retryMax:   cfg.RetryMax,
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

	var lastErr error
	for attempt := 0; attempt <= s.retryMax; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(s.retryDelay * time.Duration(1<<uint(attempt-1))):
			}
		}

		endpoint := s.endpointForAttempt(attempt)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.batchesURL, bytes.NewReader(compressedBody))
		if err != nil {
			return nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Content-Encoding", "gzip")

		resp, err := endpoint.client.Do(httpReq)
		if err != nil {
			lastErr = err
			slog.Debug("send failed", "attempt", attempt+1, "endpoint", endpoint.baseURL, "error", err)
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
			s.advanceStartIndex(attempt)
			return &batchResp, nil
		case http.StatusConflict:
			s.advanceStartIndex(attempt)
			return &batchResp, nil
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
			s.advanceStartIndex(attempt)
			return &batchResp, ErrClientError{Message: batchResp.Message}
		default:
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, batchResp.Message)
			if resp.StatusCode >= 500 {
				continue
			}
			return &batchResp, lastErr
		}
	}
	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
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
	var lastErr error
	for attempt := 0; attempt <= s.retryMax; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(s.retryDelay * time.Duration(1<<uint(attempt-1))):
			}
		}

		endpoint := s.endpointForAttempt(attempt)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.positionURL+"?client_id="+url.QueryEscape(s.clientID), nil)
		if err != nil {
			return nil, err
		}

		resp, err := endpoint.client.Do(req)
		if err != nil {
			lastErr = err
			slog.Debug("fetch position failed", "attempt", attempt+1, "endpoint", endpoint.baseURL, "error", err)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var positionResp models.PositionResponse
		if err := json.Unmarshal(body, &positionResp); err != nil {
			lastErr = fmt.Errorf("decode position response: %w", err)
			continue
		}

		switch resp.StatusCode {
		case http.StatusOK:
			s.advanceStartIndex(attempt)
			return &positionResp, nil
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
			s.advanceStartIndex(attempt)
			return &positionResp, ErrClientError{Message: positionResp.Message}
		default:
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, positionResp.Message)
			if resp.StatusCode >= 500 {
				continue
			}
			return &positionResp, lastErr
		}
	}
	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

func (s *sender) endpointForAttempt(attempt int) senderEndpoint {
	start := int(atomic.LoadUint64(&s.nextIndex))
	idx := (start + attempt) % len(s.endpoints)
	return s.endpoints[idx]
}

func (s *sender) advanceStartIndex(attempt int) {
	if len(s.endpoints) == 0 {
		return
	}
	start := atomic.LoadUint64(&s.nextIndex)
	next := uint64((int(start) + attempt + 1) % len(s.endpoints))
	atomic.StoreUint64(&s.nextIndex, next)
}

// ErrClientError indicates a client error (4xx) that should not be retried.
type ErrClientError struct{ Message string }

func (e ErrClientError) Error() string { return "client error: " + e.Message }
