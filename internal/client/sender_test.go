package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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

