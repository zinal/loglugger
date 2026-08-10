package client

import (
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestSenderSendBatchUsesPreencodedRecords(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		if req.ClientID != "client-1" || !req.Reset || req.NextPosition != "p2" {
			t.Fatalf("unexpected request: %+v", req)
		}
		if len(req.Records) != 1 || req.Records[0].Message != "hello" {
			t.Fatalf("records = %+v", req.Records)
		}
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
	recordJSON, err := json.Marshal(models.Record{Message: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := s.SendBatch(context.Background(), &Batch{
		Records:      []models.Record{{Message: "hello"}},
		RecordJSONs:  []json.RawMessage{recordJSON},
		NextPosition: "p2",
	}, true)
	if err != nil {
		t.Fatalf("SendBatch() error = %v", err)
	}
	if resp == nil || resp.Status != "ok" {
		t.Fatalf("SendBatch() response = %+v, want status ok", resp)
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

func TestSenderCurrentPositionSticksToRecoveredEndpointUntilReshuffle(t *testing.T) {
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
	if second == nil || second.CurrentPosition != "cursor-secondary" {
		t.Fatalf("second CurrentPosition() = %+v, want cursor-secondary", second)
	}
	if primaryHits != 1 {
		t.Fatalf("primary hits = %d, want 1", primaryHits)
	}
	if secondaryHits != 2 {
		t.Fatalf("secondary hits = %d, want 2", secondaryHits)
	}
}

func TestSenderReshufflesEndpointsPeriodically(t *testing.T) {
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

	raw := NewSender(SenderConfig{
		ServerURLs:  []string{primary.URL, secondary.URL},
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
		RetryDelay:  time.Millisecond,
	})
	s := raw.(*sender)
	baseNow := time.Now()
	s.now = func() time.Time { return baseNow }
	s.nextShuffleAt = baseNow.Add(time.Hour)

	first, err := s.CurrentPosition(context.Background())
	if err != nil {
		t.Fatalf("first CurrentPosition() error = %v", err)
	}
	if first == nil || first.CurrentPosition != "cursor-primary" {
		t.Fatalf("first CurrentPosition() = %+v, want cursor-primary", first)
	}

	s.shuffle = func(n int, swap func(i, j int)) {
		// Deterministic reshuffle for test: swap first two endpoints.
		swap(0, 1)
	}
	s.nextShuffleAt = baseNow

	second, err := s.CurrentPosition(context.Background())
	if err != nil {
		t.Fatalf("second CurrentPosition() error = %v", err)
	}
	if second == nil || second.CurrentPosition != "cursor-secondary" {
		t.Fatalf("second CurrentPosition() = %+v, want cursor-secondary", second)
	}
	if primaryHits != 1 {
		t.Fatalf("primary hits = %d, want 1", primaryHits)
	}
	if secondaryHits != 1 {
		t.Fatalf("secondary hits = %d, want 1", secondaryHits)
	}
}

func TestSenderSendTriesNextEndpointOnEachRetryAttempt(t *testing.T) {
	hitOrder := make([]string, 0, 3)
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitOrder = append(hitOrder, "primary")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"error","message":"temporary-primary"}`))
	}))
	defer primary.Close()

	secondary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitOrder = append(hitOrder, "secondary")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"status":"error","message":"temporary-secondary"}`))
	}))
	defer secondary.Close()

	tertiary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitOrder = append(hitOrder, "tertiary")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","next_position":"cursor-3"}`))
	}))
	defer tertiary.Close()

	s := NewSender(SenderConfig{
		ServerURLs:  []string{primary.URL, secondary.URL, tertiary.URL},
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
		RetryDelay:  time.Millisecond,
	})

	resp, err := s.Send(context.Background(), &models.BatchRequest{
		CurrentPosition: "cursor-1",
		NextPosition:    "cursor-3",
		Records:         []models.Record{{Message: "hello"}},
	})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if resp == nil || resp.Status != "ok" || resp.NextPosition != "cursor-3" {
		t.Fatalf("Send() response = %+v, want ok/cursor-3", resp)
	}
	wantOrder := []string{"primary", "secondary", "tertiary"}
	if len(hitOrder) != len(wantOrder) {
		t.Fatalf("hit order length = %d, want %d (%v)", len(hitOrder), len(wantOrder), hitOrder)
	}
	for i := range wantOrder {
		if hitOrder[i] != wantOrder[i] {
			t.Fatalf("hit order = %v, want %v", hitOrder, wantOrder)
		}
	}
}

func TestSenderSendConflictRequiresPositionMismatch(t *testing.T) {
	t.Parallel()

	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"status":"error","message":"not a mismatch"}`))
	}))
	defer server.Close()

	s := NewSender(SenderConfig{
		ServerURLs:  []string{server.URL},
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
		RetryDelay:  time.Millisecond,
	})

	resp, err := s.Send(context.Background(), &models.BatchRequest{
		CurrentPosition: "cursor-1",
		NextPosition:    "cursor-2",
		Records:         []models.Record{{Message: "hello"}},
	})
	if err == nil {
		t.Fatal("Send() error = nil, want ErrClientError for non-mismatch 409")
	}
	var clientErr ErrClientError
	if !errors.As(err, &clientErr) {
		t.Fatalf("Send() error = %v, want ErrClientError", err)
	}
	if clientErr.Message != "not a mismatch" {
		t.Fatalf("ErrClientError.Message = %q, want %q", clientErr.Message, "not a mismatch")
	}
	if resp == nil || resp.Status != "error" {
		t.Fatalf("Send() response = %+v, want status error", resp)
	}
	if hits != 1 {
		t.Fatalf("hits = %d, want 1 (no retry on malformed 409 semantics)", hits)
	}
}

func TestSenderSendRetriesInvalidJSONOnConflict(t *testing.T) {
	t.Parallel()

	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if hits == 1 {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte("proxy error page"))
			return
		}
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"status":"position_mismatch","expected_position":"cursor-9"}`))
	}))
	defer server.Close()

	s := NewSender(SenderConfig{
		ServerURLs:  []string{server.URL},
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
		RetryDelay:  time.Millisecond,
	})

	resp, err := s.Send(context.Background(), &models.BatchRequest{
		CurrentPosition: "cursor-1",
		NextPosition:    "cursor-2",
		Records:         []models.Record{{Message: "hello"}},
	})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if resp == nil || resp.Status != "position_mismatch" || resp.ExpectedPosition != "cursor-9" {
		t.Fatalf("Send() response = %+v, want position_mismatch/cursor-9", resp)
	}
	if hits != 2 {
		t.Fatalf("hits = %d, want 2 (retry after invalid JSON 409)", hits)
	}
}

func TestSenderSendConflictPositionMismatch(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"status":"position_mismatch","expected_position":"cursor-42"}`))
	}))
	defer server.Close()

	s := NewSender(SenderConfig{
		ServerURLs:  []string{server.URL},
		ClientID:    "client-1",
		HTTPTimeout: 2 * time.Second,
		RetryDelay:  time.Millisecond,
	})

	resp, err := s.Send(context.Background(), &models.BatchRequest{
		CurrentPosition: "cursor-1",
		NextPosition:    "cursor-2",
		Records:         []models.Record{{Message: "hello"}},
	})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if resp == nil || resp.Status != "position_mismatch" || resp.ExpectedPosition != "cursor-42" {
		t.Fatalf("Send() response = %+v, want position_mismatch/cursor-42", resp)
	}
}

func TestSenderSendDoesNotRetryOn4xx(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		statusCode int
		body       string
		wantMsg    string
	}{
		{
			name:       "bad request",
			statusCode: http.StatusBadRequest,
			body:       `{"status":"error","message":"invalid records[0]"}`,
			wantMsg:    "invalid records[0]",
		},
		{
			name:       "not found",
			statusCode: http.StatusNotFound,
			body:       `{"status":"error","message":"no route"}`,
			wantMsg:    "no route",
		},
		{
			name:       "unprocessable",
			statusCode: http.StatusUnprocessableEntity,
			body:       `{"status":"error","message":"schema mismatch"}`,
			wantMsg:    "schema mismatch",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hits := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				hits++
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			s := NewSender(SenderConfig{
				ServerURLs:  []string{server.URL},
				ClientID:    "client-1",
				HTTPTimeout: 2 * time.Second,
				RetryDelay:  time.Millisecond,
			})

			_, err := s.Send(context.Background(), &models.BatchRequest{
				CurrentPosition: "cursor-1",
				NextPosition:    "cursor-2",
				Records:         []models.Record{{Message: "hello"}},
			})
			if err == nil {
				t.Fatal("Send() error = nil, want ErrClientError")
			}
			var clientErr ErrClientError
			if !errors.As(err, &clientErr) {
				t.Fatalf("Send() error = %v, want ErrClientError", err)
			}
			if clientErr.Message != tc.wantMsg {
				t.Fatalf("ErrClientError.Message = %q, want %q", clientErr.Message, tc.wantMsg)
			}
			if hits != 1 {
				t.Fatalf("hits = %d, want 1 (no retry on 4xx)", hits)
			}
		})
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

func TestRetryDelayForAttempt(t *testing.T) {
	t.Parallel()

	if got := retryDelayForAttempt(0, 3); got != 0 {
		t.Fatalf("zero base delay = %v, want 0", got)
	}
	if got := retryDelayForAttempt(time.Millisecond, 1); got != time.Millisecond {
		t.Fatalf("attempt 1 = %v, want 1ms", got)
	}
	if got := retryDelayForAttempt(time.Millisecond, 2); got != 2*time.Millisecond {
		t.Fatalf("attempt 2 = %v, want 2ms", got)
	}
	if got := retryDelayForAttempt(time.Millisecond, 3); got != 4*time.Millisecond {
		t.Fatalf("attempt 3 = %v, want 4ms", got)
	}
	if got := retryDelayForAttempt(maxRetryBackoff, 5); got != maxRetryBackoff {
		t.Fatalf("capped delay = %v, want %v", got, maxRetryBackoff)
	}
}

func TestErrClientErrorMessage(t *testing.T) {
	t.Parallel()

	err := ErrClientError{Message: "bad request"}
	if got := err.Error(); got != "client error: bad request" {
		t.Fatalf("Error() = %q, want %q", got, "client error: bad request")
	}
}

func TestReadHTTPResponseBodyLimitsSize(t *testing.T) {
	t.Parallel()

	body := io.NopCloser(strings.NewReader(strings.Repeat("a", int(maxResponseBodyBytes)+1)))
	got, err := readHTTPResponseBody(body)
	if err == nil {
		t.Fatalf("expected error for oversized body, got %d bytes", len(got))
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("error = %v, want exceeds limit", err)
	}
}

func TestReadHTTPResponseBodyReturnsReadError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	got, err := readHTTPResponseBody(io.NopCloser(errReader{err: wantErr}))
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if got != nil {
		t.Fatalf("got = %q, want nil", got)
	}
}

func TestReadHTTPResponseBodyReturnsCloseError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("close failed")
	_, err := readHTTPResponseBody(&closeErrReader{
		Reader: strings.NewReader(`{"status":"ok"}`),
		err:    wantErr,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
}

func TestSenderCurrentPositionRetriesOnOversizedResponse(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(strings.Repeat("x", int(maxResponseBodyBytes)+1)))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","current_position":"cursor-ok"}`))
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
	if resp == nil || resp.CurrentPosition != "cursor-ok" {
		t.Fatalf("CurrentPosition() = %+v, want cursor-ok", resp)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestSenderSendRetriesOnOversizedResponse(t *testing.T) {
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(strings.Repeat("x", int(maxResponseBodyBytes)+1)))
			return
		}
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
		NextPosition: "p2",
		Records:      []models.Record{{Message: "hello"}},
	})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if resp == nil || resp.Status != "ok" || resp.NextPosition != "p2" {
		t.Fatalf("Send() = %+v, want ok/p2", resp)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

type closeErrReader struct {
	io.Reader
	err error
}

func (r *closeErrReader) Close() error { return r.err }
