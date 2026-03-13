package client

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/mzinal/loglugger/internal/models"
)

// Sender sends batches to the server via HTTP.
type Sender interface {
	Send(ctx context.Context, req *models.BatchRequest) (*models.BatchResponse, error)
}

// PositionStore stores and retrieves the expected position.
type PositionStore interface {
	Get() (string, error)
	Set(position string) error
}

type sender struct {
	client       *http.Client
	url         string
	clientID    string
	positionStore PositionStore
	retryMax    int
	retryDelay  time.Duration
}

// SenderConfig configures the sender.
type SenderConfig struct {
	ServerURL     string
	ClientID      string
	PositionStore PositionStore
	HTTPTimeout   time.Duration
	RetryMax      int
	RetryDelay    time.Duration
	TLSConfig     *tls.Config
}

// NewSender creates a sender.
func NewSender(cfg SenderConfig) Sender {
	transport := &http.Transport{}
	if cfg.TLSConfig != nil {
		transport.TLSClientConfig = cfg.TLSConfig
	}
	client := &http.Client{
		Timeout:   cfg.HTTPTimeout,
		Transport: transport,
	}
	return &sender{
		client:        client,
		url:           strings.TrimSuffix(cfg.ServerURL, "/") + "/v1/batches",
		clientID:      cfg.ClientID,
		positionStore: cfg.PositionStore,
		retryMax:      cfg.RetryMax,
		retryDelay:   cfg.RetryDelay,
	}
}

func (s *sender) Send(ctx context.Context, req *models.BatchRequest) (*models.BatchResponse, error) {
	req.ClientID = s.clientID
	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
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

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		httpReq.Header.Set("Content-Type", "application/json")

		resp, err := s.client.Do(httpReq)
		if err != nil {
			lastErr = err
			slog.Debug("send failed", "attempt", attempt+1, "error", err)
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
			if batchResp.NextPosition != "" && s.positionStore != nil {
				_ = s.positionStore.Set(batchResp.NextPosition)
			}
			return &batchResp, nil
		case http.StatusConflict:
			if batchResp.ExpectedPosition != "" && s.positionStore != nil {
				_ = s.positionStore.Set(batchResp.ExpectedPosition)
			}
			return &batchResp, nil
		case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
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

// ErrClientError indicates a client error (4xx) that should not be retried.
type ErrClientError struct{ Message string }

func (e ErrClientError) Error() string { return "client error: " + e.Message }
