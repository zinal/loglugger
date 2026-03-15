package client

import (
	"compress/gzip"
	"context"
	"crypto/tls"
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
		ServerURLs:  []string{srv.URL},
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
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

func TestSenderCurrentPositionRetriesEndlessly(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 5 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"error","message":"temporary"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","current_position":"cursor-5"}`))
	}))
	defer srv.Close()

	s := NewSender(SenderConfig{
		ServerURLs:  []string{srv.URL},
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
		RetryDelay:  time.Millisecond,
	})

	resp, err := s.CurrentPosition(context.Background())
	if err != nil {
		t.Fatalf("CurrentPosition() error = %v", err)
	}
	if resp == nil || resp.Status != "ok" || resp.CurrentPosition != "cursor-5" {
		t.Fatalf("CurrentPosition() = %+v, want ok/cursor-5", resp)
	}
	if attempts != 5 {
		t.Fatalf("attempts = %d, want 5", attempts)
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
		ServerURLs:  []string{srv.URL},
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
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

func TestSenderCurrentPositionSwitchesEndpointOnFailure(t *testing.T) {
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"error","message":"temporary"}`))
	}))
	defer primary.Close()

	secondaryHits := 0
	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondaryHits++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","current_position":"cursor-secondary"}`))
	}))
	defer secondary.Close()

	s := NewSender(SenderConfig{
		ServerURLs:  []string{primary.URL, secondary.URL},
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
		RetryDelay:  time.Millisecond,
	})

	resp, err := s.CurrentPosition(context.Background())
	if err != nil {
		t.Fatalf("CurrentPosition() error = %v", err)
	}
	if resp == nil || resp.CurrentPosition != "cursor-secondary" {
		t.Fatalf("CurrentPosition() = %+v, want cursor-secondary", resp)
	}
	if secondaryHits != 1 {
		t.Fatalf("secondary hits = %d, want 1", secondaryHits)
	}
}

func TestSenderCurrentPositionKeepsEndpointOnSuccess(t *testing.T) {
	primaryHits := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryHits++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","current_position":"cursor-primary"}`))
	}))
	defer primary.Close()

	secondaryHits := 0
	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondaryHits++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","current_position":"cursor-secondary"}`))
	}))
	defer secondary.Close()

	s := NewSender(SenderConfig{
		ServerURLs:  []string{primary.URL, secondary.URL},
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
		RetryDelay:  time.Millisecond,
	})

	first, err := s.CurrentPosition(context.Background())
	if err != nil {
		t.Fatalf("first CurrentPosition() error = %v", err)
	}
	second, err := s.CurrentPosition(context.Background())
	if err != nil {
		t.Fatalf("second CurrentPosition() error = %v", err)
	}

	if first == nil || first.CurrentPosition != "cursor-primary" {
		t.Fatalf("first CurrentPosition() = %+v, want cursor-primary", first)
	}
	if second == nil || second.CurrentPosition != "cursor-primary" {
		t.Fatalf("second CurrentPosition() = %+v, want cursor-primary", second)
	}
	if primaryHits != 2 {
		t.Fatalf("primary hits = %d, want 2", primaryHits)
	}
	if secondaryHits != 0 {
		t.Fatalf("secondary hits = %d, want 0", secondaryHits)
	}
}

func TestSenderCurrentPositionFailsBackToPrimaryAfterRecovery(t *testing.T) {
	primaryHits := 0
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		primaryHits++
		if primaryHits == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"status":"error","message":"temporary"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","current_position":"cursor-primary"}`))
	}))
	defer primary.Close()

	secondaryHits := 0
	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondaryHits++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","current_position":"cursor-secondary"}`))
	}))
	defer secondary.Close()

	s := NewSender(SenderConfig{
		ServerURLs:  []string{primary.URL, secondary.URL},
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
		RetryDelay:  time.Millisecond,
	})

	first, err := s.CurrentPosition(context.Background())
	if err != nil {
		t.Fatalf("first CurrentPosition() error = %v", err)
	}
	second, err := s.CurrentPosition(context.Background())
	if err != nil {
		t.Fatalf("second CurrentPosition() error = %v", err)
	}

	if first == nil || first.CurrentPosition != "cursor-secondary" {
		t.Fatalf("first CurrentPosition() = %+v, want cursor-secondary", first)
	}
	if second == nil || second.CurrentPosition != "cursor-primary" {
		t.Fatalf("second CurrentPosition() = %+v, want cursor-primary", second)
	}
	if primaryHits != 2 {
		t.Fatalf("primary hits = %d, want 2", primaryHits)
	}
	if secondaryHits != 1 {
		t.Fatalf("secondary hits = %d, want 1", secondaryHits)
	}
}

func TestSenderSetsTLSHostPerEndpoint(t *testing.T) {
	s := NewSender(SenderConfig{
		ServerURLs:  []string{"https://host-a.example:27312", "https://host-b.example:9443"},
		ClientID:    "client-1",
		HTTPTimeout: time.Second,
		RetryDelay:  time.Millisecond,
		TLSConfig:   &tlsConfigWithMinVersion12,
	}).(*sender)

	gotA := s.endpoints[0].client.Transport.(*http.Transport).TLSClientConfig.ServerName
	gotB := s.endpoints[1].client.Transport.(*http.Transport).TLSClientConfig.ServerName
	if gotA != "host-a.example" || gotB != "host-b.example" {
		t.Fatalf("server names = [%q %q], want [host-a.example host-b.example]", gotA, gotB)
	}
}

var tlsConfigWithMinVersion12 = tls.Config{MinVersion: tls.VersionTLS12}
