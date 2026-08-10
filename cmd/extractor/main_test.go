package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestFilterListSetAndString(t *testing.T) {
	t.Parallel()

	var filters filterList
	if err := filters.Set("level=INFO,WARN"); err != nil {
		t.Fatalf("Set() error = %v", err)
	}
	if err := filters.Set("level=ERROR,INFO"); err != nil {
		t.Fatalf("merge Set() error = %v", err)
	}
	if err := filters.Set("host=node-1"); err != nil {
		t.Fatalf("second field Set() error = %v", err)
	}
	if got := filters.String(); got != "level=INFO,WARN,ERROR;host=node-1" {
		t.Fatalf("String() = %q, want level=INFO,WARN,ERROR;host=node-1", got)
	}

	cases := []string{"", "novalue", "bad field=1", "level="}
	for _, raw := range cases {
		var f filterList
		if err := f.Set(raw); err == nil {
			t.Fatalf("Set(%q) error = nil, want error", raw)
		}
	}
}

func TestParseTimestamp(t *testing.T) {
	t.Parallel()

	got, err := parseTimestamp("2025-03-13T10:00:00Z")
	if err != nil {
		t.Fatalf("parseTimestamp() error = %v", err)
	}
	want := time.Date(2025, 3, 13, 10, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	got, err = parseTimestamp("2025-03-13T10:00:00.123456789Z")
	if err != nil {
		t.Fatalf("parseTimestamp nano error = %v", err)
	}
	if got.Nanosecond() != 123456789 {
		t.Fatalf("nanoseconds = %d, want 123456789", got.Nanosecond())
	}

	if _, err := parseTimestamp(""); err == nil {
		t.Fatal("expected error for empty timestamp")
	}
	if _, err := parseTimestamp("13/03/2025"); err == nil {
		t.Fatal("expected error for unsupported timestamp")
	}
}

func TestParseByteSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw     string
		want    int64
		wantErr bool
	}{
		{raw: "1048576", want: 1048576},
		{raw: "10MiB", want: 10 << 20},
		{raw: "2GiB", want: 2 << 30},
		{raw: "3KB", want: 3000},
		{raw: "4MB", want: 4_000_000},
		{raw: "5GB", want: 5_000_000_000},
		{raw: "7B", want: 7},
		{raw: "1KiB", want: 1 << 10},
		{raw: "", wantErr: true},
		{raw: "MiB", wantErr: true},
		{raw: "abc", wantErr: true},
	}
	for _, tt := range tests {
		got, err := parseByteSize(tt.raw)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("parseByteSize(%q) error = nil, want error", tt.raw)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseByteSize(%q) error = %v", tt.raw, err)
		}
		if got != tt.want {
			t.Fatalf("parseByteSize(%q) = %d, want %d", tt.raw, got, tt.want)
		}
	}
}

func TestStringifyCellAndEscapeTSVText(t *testing.T) {
	t.Parallel()

	ts := time.Date(2025, 3, 13, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		in   any
		want string
	}{
		{nil, ""},
		{"text", "text"},
		{true, "true"},
		{int(1), "1"},
		{int8(2), "2"},
		{int16(3), "3"},
		{int32(4), "4"},
		{int64(42), "42"},
		{uint(5), "5"},
		{uint8(6), "6"},
		{uint16(7), "7"},
		{uint32(8), "8"},
		{uint64(9), "9"},
		{float32(1.25), "1.25"},
		{float64(1.5), "1.5"},
		{ts, "2025-03-13T10:00:00Z"},
		{[]byte("raw"), "raw"},
		{stringerValue("x"), "x"},
	}
	for _, tc := range cases {
		if got := stringifyCell(tc.in); got != tc.want {
			t.Fatalf("stringifyCell(%T)=%q, want %q", tc.in, got, tc.want)
		}
	}

	if got := escapeTSVText("plain"); got != "plain" {
		t.Fatalf("plain escape = %q", got)
	}
	if got := escapeTSVText("a\tb\nc\rd"); got != "a\\tb\\nc\\rd" {
		t.Fatalf("control-char escape = %q", got)
	}
}

type stringerValue string

func (s stringerValue) String() string { return string(s) }

func TestContainsAndQuoteHelpers(t *testing.T) {
	t.Parallel()

	if !contains([]string{"a", "b"}, "b") || contains([]string{"a"}, "z") {
		t.Fatal("contains() mismatch")
	}
	if got := quoteYDBPath("/local/path`x"); got != "`/local/path_x`" {
		t.Fatalf("quoteYDBPath = %q", got)
	}
	if got := quoteYDBIdentifier("col`name"); got != "`col_name`" {
		t.Fatalf("quoteYDBIdentifier = %q", got)
	}
}

func TestFullTablePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		database string
		table    string
		want     string
	}{
		{
			name:     "relative table under absolute database",
			database: "/local",
			table:    "ydblogs",
			want:     "/local/ydblogs",
		},
		{
			name:     "nested database path",
			database: "/Root/db",
			table:    "logs",
			want:     "/Root/db/logs",
		},
		{
			name:     "absolute table path unchanged",
			database: "/local",
			table:    "/Root/db/logs",
			want:     "/Root/db/logs",
		},
		{
			name:     "empty database keeps table as-is",
			database: "",
			table:    "logs",
			want:     "logs",
		},
		{
			name:     "trims surrounding whitespace",
			database: "  /local  ",
			table:    "  ydblogs  ",
			want:     "/local/ydblogs",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := fullTablePath(tc.database, tc.table); got != tc.want {
				t.Fatalf("fullTablePath(%q, %q) = %q, want %q", tc.database, tc.table, got, tc.want)
			}
		})
	}
}

func TestYDBDSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		endpoint string
		database string
		want     string
	}{
		{
			name:     "typical absolute database path",
			endpoint: "grpcs://host:2135",
			database: "/local",
			want:     "grpcs://host:2135/local",
		},
		{
			name:     "nested database path",
			endpoint: "grpcs://host:2135",
			database: "/Root/db",
			want:     "grpcs://host:2135/Root/db",
		},
		{
			name:     "database without leading slash",
			endpoint: "grpcs://host:2135",
			database: "local",
			want:     "grpcs://host:2135/local",
		},
		{
			name:     "endpoint with trailing slash",
			endpoint: "grpcs://host:2135/",
			database: "/local",
			want:     "grpcs://host:2135/local",
		},
		{
			name:     "trims surrounding whitespace",
			endpoint: "  grpcs://host:2135  ",
			database: "  /local  ",
			want:     "grpcs://host:2135/local",
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ydbDSN(tc.endpoint, tc.database); got != tc.want {
				t.Fatalf("ydbDSN(%q, %q) = %q, want %q", tc.endpoint, tc.database, got, tc.want)
			}
		})
	}
}

func TestLoadAndMergeServerConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "server.yaml")
	if err := os.WriteFile(yamlPath, []byte(`
ydb_endpoint: grpcs://ydb.example:2135
ydb_database: /Root/db
ydb_table: logs
ydb_auth_mode: static
ydb_auth_login: user
ydb_auth_password: secret
ydb_open_timeout: 5s
`), 0o600); err != nil {
		t.Fatal(err)
	}

	serverCfg, err := loadServerConfig(yamlPath)
	if err != nil {
		t.Fatalf("loadServerConfig() error = %v", err)
	}
	cfg := extractConfig{YDBAuthMode: defaultYDBAuthMode}
	mergeServerConfig(&cfg, serverCfg)
	if cfg.YDBEndpoint != "grpcs://ydb.example:2135" {
		t.Fatalf("endpoint = %q", cfg.YDBEndpoint)
	}
	if cfg.YDBDatabase != "/Root/db" || cfg.YDBTable != "logs" {
		t.Fatalf("database/table = %q/%q", cfg.YDBDatabase, cfg.YDBTable)
	}
	if cfg.YDBAuthMode != "static" || cfg.YDBAuthLogin != "user" || cfg.YDBAuthPassword != "secret" {
		t.Fatalf("auth = %#v", cfg)
	}
	if cfg.YDBOpenTimeout != 5*time.Second {
		t.Fatalf("open timeout = %v, want 5s", cfg.YDBOpenTimeout)
	}

	jsonPath := filepath.Join(dir, "server.json")
	if err := os.WriteFile(jsonPath, []byte(`{"ydb_endpoint":"grpcs://json.example:2135","ydb_database":"/Root/json","ydb_table":"events"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	jsonCfg, err := loadServerConfig(jsonPath)
	if err != nil {
		t.Fatalf("loadServerConfig(json) error = %v", err)
	}
	if jsonCfg.YDBEndpoint != "grpcs://json.example:2135" {
		t.Fatalf("json endpoint = %q", jsonCfg.YDBEndpoint)
	}
}

func TestBuildQueryAndParams(t *testing.T) {
	t.Parallel()

	cfg := extractConfig{
		YDBTable:   "/Root/db/logs",
		TimeColumn: "ts_orig",
		From:       time.Date(2025, 3, 13, 10, 0, 0, 0, time.UTC),
		To:         time.Date(2025, 3, 13, 11, 0, 0, 0, time.UTC),
		Filters: filterList{
			{Field: "level", Values: []string{"INFO", "WARN"}},
			{Field: "host", Values: []string{"node-1"}},
		},
	}
	query, params, err := buildQueryAndParams(cfg, []string{"ts_orig", "level", "host", "message"})
	if err != nil {
		t.Fatalf("buildQueryAndParams() error = %v", err)
	}
	if !strings.Contains(query, "DECLARE $from_ts AS Timestamp64;") {
		t.Fatalf("query missing from declare:\n%s", query)
	}
	if !strings.Contains(query, "DECLARE $filter_0 AS List<Utf8>;") {
		t.Fatalf("query missing filter declare:\n%s", query)
	}
	if !strings.Contains(query, "FROM `/Root/db/logs`") {
		t.Fatalf("query missing FROM clause:\n%s", query)
	}
	if !strings.Contains(query, "`ts_orig` >= $from_ts AND `ts_orig` < $to_ts") {
		t.Fatalf("query missing time predicate:\n%s", query)
	}
	if !strings.Contains(query, "`level` IN $filter_0") || !strings.Contains(query, "`host` IN $filter_1") {
		t.Fatalf("query missing filter predicates:\n%s", query)
	}
	if params == nil {
		t.Fatal("params is nil")
	}
}

func TestYDBAuthOptionValidation(t *testing.T) {
	t.Parallel()

	if _, err := ydbAuthOption(ydbAuthOptions{Mode: "anonymous"}); err != nil {
		t.Fatalf("anonymous error = %v", err)
	}
	if _, err := ydbAuthOption(ydbAuthOptions{Mode: "static", Login: "u"}); err == nil {
		t.Fatal("expected static auth password error")
	}
	if _, err := ydbAuthOption(ydbAuthOptions{Mode: "static", Password: "p"}); err == nil {
		t.Fatal("expected static auth login error")
	}
	if _, err := ydbAuthOption(ydbAuthOptions{Mode: "static", Login: "u", Password: "p"}); err != nil {
		t.Fatalf("static auth error = %v", err)
	}
	if _, err := ydbAuthOption(ydbAuthOptions{Mode: "sa-key"}); err == nil {
		t.Fatal("expected sa-key file error")
	}
	if _, err := ydbAuthOption(ydbAuthOptions{Mode: "metadata"}); err != nil {
		t.Fatalf("metadata auth error = %v", err)
	}
	if _, err := ydbAuthOption(ydbAuthOptions{Mode: "metadata", MetadataURL: "http://169.254.169.254/"}); err != nil {
		t.Fatalf("metadata URL auth error = %v", err)
	}
	if _, err := ydbAuthOption(ydbAuthOptions{Mode: "unknown"}); err == nil {
		t.Fatal("expected unknown mode error")
	}
}

func TestNormalizeOutputPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		raw     string
		want    string
		wantErr bool
	}{
		{raw: "extract", want: "extract"},
		{raw: "  extract  ", want: "extract"},
		{raw: "../extract", want: "extract"},
		{raw: "foo/../evil", want: "evil"},
		{raw: "dir/prefix", want: "prefix"},
		{raw: "", wantErr: true},
		{raw: ".", wantErr: true},
		{raw: "..", wantErr: true},
		{raw: "   ", wantErr: true},
	}
	for _, tt := range tests {
		got, err := normalizeOutputPrefix(tt.raw)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("normalizeOutputPrefix(%q) error = nil, want error", tt.raw)
			}
			continue
		}
		if err != nil {
			t.Fatalf("normalizeOutputPrefix(%q) error = %v", tt.raw, err)
		}
		if got != tt.want {
			t.Fatalf("normalizeOutputPrefix(%q) = %q, want %q", tt.raw, got, tt.want)
		}
	}
}

func TestRemoveOutputPrefixFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	keep := filepath.Join(dir, "keep.txt")
	otherPrefix := filepath.Join(dir, "other_000001.tsv")
	plain := filepath.Join(dir, "extract_000001.tsv")
	compressed := filepath.Join(dir, "extract_000002.tsv.zst")
	notOurs := filepath.Join(dir, "extract_notes.tsv")
	symlinkTarget := filepath.Join(dir, "secret")
	symlinkPath := filepath.Join(dir, "extract_000003.tsv")

	for _, path := range []string{keep, otherPrefix, plain, compressed, notOurs, symlinkTarget} {
		if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(symlinkTarget, symlinkPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	if err := removeOutputPrefixFiles(dir, "extract"); err != nil {
		t.Fatalf("removeOutputPrefixFiles() error = %v", err)
	}

	mustExist := []string{keep, otherPrefix, notOurs, symlinkTarget}
	for _, path := range mustExist {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("expected to keep %s: %v", path, err)
		}
	}
	mustGone := []string{plain, compressed, symlinkPath}
	for _, path := range mustGone {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			t.Fatalf("expected %s to be removed, err=%v", path, err)
		}
	}
	got, err := os.ReadFile(symlinkTarget)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "data" {
		t.Fatalf("symlink target modified: %q", got)
	}
}

func TestRemoveOutputPrefixFilesAllowsRerun(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	existing := filepath.Join(dir, "extract_000001.tsv")
	if err := os.WriteFile(existing, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := removeOutputPrefixFiles(dir, "extract"); err != nil {
		t.Fatalf("removeOutputPrefixFiles() error = %v", err)
	}
	writer, err := newExtractionWriter(extractConfig{
		OutputDir:    dir,
		OutputPrefix: "extract",
		MaxFileSize:  1 << 20,
	})
	if err != nil {
		t.Fatalf("newExtractionWriter() after cleanup error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestIsExtractRetryable(t *testing.T) {
	t.Parallel()

	if isExtractRetryable(nil) {
		t.Fatal("nil should not be retryable")
	}
	if isExtractRetryable(errors.New("plain local error")) {
		t.Fatal("plain local error should not be retryable")
	}
	if isExtractRetryable(context.Canceled) {
		t.Fatal("context.Canceled should not be retryable")
	}
	if isExtractRetryable(context.DeadlineExceeded) {
		t.Fatal("context.DeadlineExceeded should not be retryable")
	}

	retryable := retry.RetryableError(errors.New("transient ydb failure"), retry.WithBackoff(retry.TypeFastBackoff))
	if !isExtractRetryable(retryable) {
		t.Fatal("retry.RetryableError should be retryable")
	}
	if !isExtractRetryable(errors.Join(errors.New("wrapper"), retryable)) {
		t.Fatal("joined retryable error should remain retryable")
	}
}

func TestExtractRetryDelay(t *testing.T) {
	t.Parallel()

	if got := extractRetryDelay(1); got != extractRetryBaseDelay {
		t.Fatalf("attempt 1 delay = %s, want %s", got, extractRetryBaseDelay)
	}
	if got := extractRetryDelay(2); got != 2*extractRetryBaseDelay {
		t.Fatalf("attempt 2 delay = %s, want %s", got, 2*extractRetryBaseDelay)
	}
	if got := extractRetryDelay(100); got != extractRetryMaxDelay {
		t.Fatalf("capped delay = %s, want %s", got, extractRetryMaxDelay)
	}
}

func TestExtractionWriterWritesAndRotates(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writer, err := newExtractionWriter(extractConfig{
		OutputDir:    dir,
		OutputPrefix: "extract",
		MaxFileSize:  8,
	})
	if err != nil {
		t.Fatalf("newExtractionWriter() error = %v", err)
	}
	defer func() { _ = writer.Close() }()

	if err := writer.WriteRow([]string{"aaaa", "bbbb"}); err != nil {
		t.Fatalf("WriteRow() error = %v", err)
	}
	if err := writer.WriteRow([]string{"cccc", "dddd"}); err != nil {
		t.Fatalf("second WriteRow() error = %v", err)
	}
	if writer.rowsWritten != 2 {
		t.Fatalf("rowsWritten = %d, want 2", writer.rowsWritten)
	}
	if writer.currentIndex < 2 {
		t.Fatalf("currentIndex = %d, want >= 2 after rotation", writer.currentIndex)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) < 2 {
		t.Fatalf("expected rotated files, got %d entries", len(entries))
	}

	info, err := os.Stat(filepath.Join(dir, "extract_000001.tsv"))
	if err != nil {
		t.Fatalf("stat first output file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("output file mode = %04o, want 0600", perm)
	}
}

func TestExtractionWriterUsesBasenamePrefix(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writer, err := newExtractionWriter(extractConfig{
		OutputDir:    dir,
		OutputPrefix: "../escape",
		MaxFileSize:  1 << 20,
	})
	if err != nil {
		t.Fatalf("newExtractionWriter() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	path := filepath.Join(dir, "escape_000001.tsv")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected basename-local output file: %v", err)
	}
	escaped := filepath.Join(filepath.Dir(dir), "escape_000001.tsv")
	if _, err := os.Stat(escaped); err == nil {
		t.Fatalf("output escaped parent dir via prefix: found %s", escaped)
	}
}

func TestExtractionWriterRejectsExistingFileAndSymlink(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	existing := filepath.Join(dir, "extract_000001.tsv")
	if err := os.WriteFile(existing, []byte("keep-me"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newExtractionWriter(extractConfig{
		OutputDir:    dir,
		OutputPrefix: "extract",
		MaxFileSize:  1 << 20,
	}); err == nil {
		t.Fatal("expected error when output file already exists")
	}
	got, err := os.ReadFile(existing)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "keep-me" {
		t.Fatalf("existing file overwritten: got %q", got)
	}

	symlinkDir := t.TempDir()
	target := filepath.Join(symlinkDir, "secret")
	if err := os.WriteFile(target, []byte("secret-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(symlinkDir, "extract_000001.tsv")
	if err := os.Symlink(target, link); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if _, err := newExtractionWriter(extractConfig{
		OutputDir:    symlinkDir,
		OutputPrefix: "extract",
		MaxFileSize:  1 << 20,
	}); err == nil {
		t.Fatal("expected error when output path is a symlink")
	}
	got, err = os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "secret-data" {
		t.Fatalf("symlink target overwritten: got %q", got)
	}
}

func TestExtractionWriterZstdCreatesCompressedFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writer, err := newExtractionWriter(extractConfig{
		OutputDir:    dir,
		OutputPrefix: "extract",
		ZstdEnabled:  true,
		MaxFileSize:  1 << 20,
	})
	if err != nil {
		t.Fatalf("newExtractionWriter() error = %v", err)
	}
	if err := writer.WriteRow([]string{"hello", "world"}); err != nil {
		t.Fatalf("WriteRow() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	path := filepath.Join(dir, "extract_000001.tsv.zst")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected zstd output file: %v", err)
	}
}
