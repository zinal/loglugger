package client

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ydb-platform/loglugger/internal/models"
)

func TestSenderCurrentPositionRetriesOn5xx(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"error","message":"temporary"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","current_position":"cursor-3"}`))
	}))
	defer srv.Close()

	s := NewSender(SenderConfig{
		ServerURL:   srv.URL,
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
		RetryMax:    3,
		RetryDelay:  time.Millisecond,
	})

	resp, err := s.CurrentPosition(context.Background())
	if err != nil {
		t.Fatalf("CurrentPosition() error = %v", err)
	}
	if resp == nil || resp.Status != "ok" || resp.CurrentPosition != "cursor-3" {
		t.Fatalf("CurrentPosition() = %+v, want ok/cursor-3", resp)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestSenderSendUsesGzipCompression(t *testing.T) {
	sawGzip := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/batches" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Content-Encoding") != "gzip" {
			t.Fatalf("Content-Encoding = %q, want gzip", r.Header.Get("Content-Encoding"))
		}
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			t.Fatalf("gzip.NewReader() error = %v", err)
		}
		defer gz.Close()
		raw, err := io.ReadAll(gz)
		if err != nil {
			t.Fatalf("read gzip body error = %v", err)
		}
		var req models.BatchRequest
		if err := json.Unmarshal(raw, &req); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		if req.ClientID != "client-1" {
			t.Fatalf("client_id = %q, want client-1", req.ClientID)
		}
		sawGzip = true
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","next_position":"p2"}`))
	}))
	defer srv.Close()

	s := NewSender(SenderConfig{
		ServerURL:   srv.URL,
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
		RetryMax:    0,
		RetryDelay:  time.Millisecond,
	})

	resp, err := s.Send(context.Background(), &models.BatchRequest{
		Reset:           true,
		CurrentPosition: "",
		NextPosition:    "p2",
		Records:         []models.Record{{Message: "hello"}},
	})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if !sawGzip {
		t.Fatal("expected server to receive gzip-compressed request")
	}
	if resp == nil || resp.Status != "ok" {
		t.Fatalf("Send() response = %+v, want status ok", resp)
	}
}

