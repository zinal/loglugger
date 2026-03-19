package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/ydb-platform/loglugger/internal/buildinfo"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	yc "github.com/ydb-platform/ydb-go-yc"
	"gopkg.in/yaml.v3"
)

const (
	defaultPlainMaxFileSize = 200 << 20 // 200 MiB
	defaultZstdMaxFileSize  = 10 << 20  // 10 MiB
	defaultYDBOpenTimeout   = 10 * time.Second
	defaultYDBAuthMode      = "anonymous"
	defaultTimeColumn       = "ts_orig"
	defaultOutputPrefix     = "extract"
	// For zstd output we periodically flush to refresh compressed byte counter
	// used by rotation, without paying per-row flush overhead.
	zstdSizeCheckInterval = 1 << 20 // 1 MiB of plain TSV data
)

var (
	identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	tsvTextReplacer   = strings.NewReplacer("\t", "\\t", "\n", "\\n", "\r", "\\r")
)

type serverConfig struct {
	YDBEndpoint          string `json:"ydb_endpoint" yaml:"ydb_endpoint"`
	YDBDatabase          string `json:"ydb_database" yaml:"ydb_database"`
	YDBTable             string `json:"ydb_table" yaml:"ydb_table"`
	YDBAuthMode          string `json:"ydb_auth_mode" yaml:"ydb_auth_mode"`
	YDBAuthLogin         string `json:"ydb_auth_login" yaml:"ydb_auth_login"`
	YDBAuthPassword      string `json:"ydb_auth_password" yaml:"ydb_auth_password"`
	YDBAuthSACredentials string `json:"ydb_auth_sa_key_file" yaml:"ydb_auth_sa_key_file"`
	YDBAuthMetadataURL   string `json:"ydb_auth_metadata_url" yaml:"ydb_auth_metadata_url"`
	YDBCAPath            string `json:"ydb_ca_path" yaml:"ydb_ca_path"`
	YDBOpenTimeout       string `json:"ydb_open_timeout" yaml:"ydb_open_timeout"`
}

type extractConfig struct {
	ServerConfigPath string

	YDBEndpoint          string
	YDBDatabase          string
	YDBTable             string
	YDBAuthMode          string
	YDBAuthLogin         string
	YDBAuthPassword      string
	YDBAuthSACredentials string
	YDBAuthMetadataURL   string
	YDBCAPath            string
	YDBOpenTimeout       time.Duration

	TimeColumn string
	From       time.Time
	To         time.Time

	Filters filterList

	OutputDir    string
	OutputPrefix string
	ZstdEnabled  bool
	MaxFileSize  int64
}

type ydbAuthOptions struct {
	Mode                  string
	Login                 string
	Password              string
	ServiceAccountKeyFile string
	MetadataURL           string
	CACertPath            string
}

type listFilter struct {
	Field  string
	Values []string
}

type filterList []listFilter

func (f *filterList) String() string {
	if len(*f) == 0 {
		return ""
	}
	parts := make([]string, 0, len(*f))
	for _, item := range *f {
		parts = append(parts, item.Field+"="+strings.Join(item.Values, ","))
	}
	return strings.Join(parts, ";")
}

func (f *filterList) Set(raw string) error {
	spec := strings.TrimSpace(raw)
	if spec == "" {
		return fmt.Errorf("empty filter")
	}
	parts := strings.SplitN(spec, "=", 2)
	if len(parts) != 2 {
		return fmt.Errorf("filter %q must have format field=v1,v2", spec)
	}
	field := strings.TrimSpace(parts[0])
	if !identifierPattern.MatchString(field) {
		return fmt.Errorf("filter field %q must match %s", field, identifierPattern)
	}
	valueParts := strings.Split(parts[1], ",")
	values := make([]string, 0, len(valueParts))
	for _, v := range valueParts {
		val := strings.TrimSpace(v)
		if val == "" {
			continue
		}
		values = append(values, val)
	}
	if len(values) == 0 {
		return fmt.Errorf("filter %q has no values", spec)
	}

	for i := range *f {
		if (*f)[i].Field == field {
			for _, v := range values {
				if !slices.Contains((*f)[i].Values, v) {
					(*f)[i].Values = append((*f)[i].Values, v)
				}
			}
			return nil
		}
	}

	*f = append(*f, listFilter{
		Field:  field,
		Values: values,
	})
	return nil
}

type extractionWriter struct {
	outputDir    string
	outputPrefix string
	zstdEnabled  bool
	maxFileSize  int64

	currentIndex int
	currentPath  string
	currentSize  int64
	rowsWritten  int64

	file   *os.File
	base   *countingWriter
	writer io.Writer
	zstd   *zstd.Encoder

	lineBuf           bytes.Buffer
	pendingPlainBytes int64
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (w *countingWriter) Write(p []byte) (int, error) {
	written, err := w.w.Write(p)
	w.n += int64(written)
	return written, err
}

type rawCellValue struct {
	value any
}

func (c *rawCellValue) UnmarshalYDB(raw types.RawValue) error {
	if raw.IsNull() {
		c.value = nil
		return raw.Err()
	}
	c.value = raw.Any()
	return raw.Err()
}

func main() {
	cfg, err := parseConfig()
	if err != nil {
		slog.Error("parse extractor config", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()
	driver, err := openYDBDriver(ctx, cfg)
	if err != nil {
		slog.Error("open ydb driver", "error", err)
		os.Exit(1)
	}
	defer func() {
		if closeErr := driver.Close(ctx); closeErr != nil {
			slog.Warn("close ydb driver", "error", closeErr)
		}
	}()

	writer, err := newExtractionWriter(cfg)
	if err != nil {
		slog.Error("prepare output writer", "error", err)
		os.Exit(1)
	}
	defer func() {
		if closeErr := writer.Close(); closeErr != nil {
			slog.Warn("close output files", "error", closeErr)
		}
	}()

	columns, err := describeColumns(ctx, driver, cfg.YDBTable)
	if err != nil {
		slog.Error("describe source table", "error", err)
		os.Exit(1)
	}
	if len(columns) == 0 {
		slog.Error("source table has no columns", "table", cfg.YDBTable)
		os.Exit(1)
	}
	if !contains(columns, cfg.TimeColumn) {
		slog.Error("time column not found in source table", "time_column", cfg.TimeColumn, "table", cfg.YDBTable)
		os.Exit(1)
	}
	for _, f := range cfg.Filters {
		if !contains(columns, f.Field) {
			slog.Error("filter field not found in source table", "field", f.Field, "table", cfg.YDBTable)
			os.Exit(1)
		}
	}

	query, params, err := buildQueryAndParams(cfg, columns)
	if err != nil {
		slog.Error("build extraction query", "error", err)
		os.Exit(1)
	}

	if err := runExtraction(ctx, driver, query, params, columns, writer); err != nil {
		slog.Error("extract rows", "error", err)
		os.Exit(1)
	}

	slog.Info(
		"extraction completed",
		"version", buildinfo.Version,
		"rows", writer.rowsWritten,
		"files", writer.currentIndex,
		"bytes_last_file", writer.currentSize,
		"output_dir", writer.outputDir,
	)
}

func parseConfig() (extractConfig, error) {
	var (
		cfg     extractConfig
		fromRaw string
		toRaw   string
		sizeRaw string
	)

	cfg.YDBAuthMode = defaultYDBAuthMode
	cfg.TimeColumn = defaultTimeColumn
	cfg.OutputPrefix = defaultOutputPrefix

	flag.StringVar(&cfg.ServerConfigPath, "server-config", "", "Path to server YAML/JSON config file (required)")
	flag.StringVar(&cfg.YDBEndpoint, "ydb-endpoint", "", "Override YDB endpoint from server config")
	flag.StringVar(&cfg.YDBDatabase, "ydb-database", "", "Override YDB database from server config")
	flag.StringVar(&cfg.YDBTable, "ydb-table", "", "Override YDB table from server config")
	flag.StringVar(&cfg.TimeColumn, "time-column", defaultTimeColumn, "Event-time column used by mandatory [from,to) filter")
	flag.StringVar(&fromRaw, "from", "", "Interval start (inclusive), RFC3339 or RFC3339Nano (required)")
	flag.StringVar(&toRaw, "to", "", "Interval end (exclusive), RFC3339 or RFC3339Nano (required)")
	flag.Var(&cfg.Filters, "filter", "Optional field-list filter: field=v1,v2 (repeatable)")
	flag.StringVar(&cfg.OutputDir, "output-dir", ".", "Directory for extracted files")
	flag.StringVar(&cfg.OutputPrefix, "output-prefix", defaultOutputPrefix, "Output file prefix")
	flag.BoolVar(&cfg.ZstdEnabled, "zstd", false, "Enable zstd compression for output files")
	flag.StringVar(&sizeRaw, "max-file-size", "", "Max output file size (e.g. 200MiB, 10MiB, 1048576)")
	flag.Parse()

	if strings.TrimSpace(cfg.ServerConfigPath) == "" {
		return extractConfig{}, fmt.Errorf("server-config is required")
	}
	serverCfg, err := loadServerConfig(cfg.ServerConfigPath)
	if err != nil {
		return extractConfig{}, err
	}
	mergeServerConfig(&cfg, serverCfg)

	if strings.TrimSpace(cfg.YDBEndpoint) == "" {
		return extractConfig{}, fmt.Errorf("ydb endpoint is required")
	}
	if strings.TrimSpace(cfg.YDBDatabase) == "" {
		return extractConfig{}, fmt.Errorf("ydb database is required")
	}
	if strings.TrimSpace(cfg.YDBTable) == "" {
		return extractConfig{}, fmt.Errorf("ydb table is required")
	}
	if !identifierPattern.MatchString(cfg.TimeColumn) {
		return extractConfig{}, fmt.Errorf("time-column %q must match %s", cfg.TimeColumn, identifierPattern)
	}

	cfg.From, err = parseTimestamp(strings.TrimSpace(fromRaw))
	if err != nil {
		return extractConfig{}, fmt.Errorf("parse from: %w", err)
	}
	cfg.To, err = parseTimestamp(strings.TrimSpace(toRaw))
	if err != nil {
		return extractConfig{}, fmt.Errorf("parse to: %w", err)
	}
	if !cfg.From.Before(cfg.To) {
		return extractConfig{}, fmt.Errorf("invalid interval: from must be less than to")
	}

	cfg.OutputDir = strings.TrimSpace(cfg.OutputDir)
	if cfg.OutputDir == "" {
		return extractConfig{}, fmt.Errorf("output-dir must not be empty")
	}
	cfg.OutputPrefix = strings.TrimSpace(cfg.OutputPrefix)
	if cfg.OutputPrefix == "" {
		return extractConfig{}, fmt.Errorf("output-prefix must not be empty")
	}

	if strings.TrimSpace(sizeRaw) == "" {
		if cfg.ZstdEnabled {
			cfg.MaxFileSize = defaultZstdMaxFileSize
		} else {
			cfg.MaxFileSize = defaultPlainMaxFileSize
		}
	} else {
		cfg.MaxFileSize, err = parseByteSize(sizeRaw)
		if err != nil {
			return extractConfig{}, fmt.Errorf("parse max-file-size: %w", err)
		}
	}
	if cfg.MaxFileSize <= 0 {
		return extractConfig{}, fmt.Errorf("max-file-size must be greater than zero")
	}

	cfg.From = cfg.From.UTC()
	cfg.To = cfg.To.UTC()

	return cfg, nil
}

func parseTimestamp(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, fmt.Errorf("value is required")
	}
	if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return ts, nil
	}
	if ts, err := time.Parse(time.RFC3339, raw); err == nil {
		return ts, nil
	}
	return time.Time{}, fmt.Errorf("unsupported format %q (expected RFC3339)", raw)
}

func loadServerConfig(path string) (serverConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return serverConfig{}, fmt.Errorf("read server config: %w", err)
	}
	cfg := serverConfig{}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		if err := json.Unmarshal(data, &cfg); err != nil {
			return serverConfig{}, fmt.Errorf("decode server JSON config: %w", err)
		}
	default:
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return serverConfig{}, fmt.Errorf("decode server YAML config: %w", err)
		}
	}
	return cfg, nil
}

func mergeServerConfig(cfg *extractConfig, serverCfg serverConfig) {
	if cfg.YDBEndpoint == "" {
		cfg.YDBEndpoint = strings.TrimSpace(serverCfg.YDBEndpoint)
	}
	if cfg.YDBDatabase == "" {
		cfg.YDBDatabase = strings.TrimSpace(serverCfg.YDBDatabase)
	}
	if cfg.YDBTable == "" {
		cfg.YDBTable = strings.TrimSpace(serverCfg.YDBTable)
	}
	if cfg.YDBAuthMode == defaultYDBAuthMode || cfg.YDBAuthMode == "" {
		if mode := strings.TrimSpace(serverCfg.YDBAuthMode); mode != "" {
			cfg.YDBAuthMode = mode
		}
	}
	cfg.YDBAuthLogin = strings.TrimSpace(serverCfg.YDBAuthLogin)
	cfg.YDBAuthPassword = strings.TrimSpace(serverCfg.YDBAuthPassword)
	cfg.YDBAuthSACredentials = strings.TrimSpace(serverCfg.YDBAuthSACredentials)
	cfg.YDBAuthMetadataURL = strings.TrimSpace(serverCfg.YDBAuthMetadataURL)
	cfg.YDBCAPath = strings.TrimSpace(serverCfg.YDBCAPath)

	cfg.YDBOpenTimeout = defaultYDBOpenTimeout
	if strings.TrimSpace(serverCfg.YDBOpenTimeout) != "" {
		if d, err := time.ParseDuration(strings.TrimSpace(serverCfg.YDBOpenTimeout)); err == nil && d > 0 {
			cfg.YDBOpenTimeout = d
		}
	}
}

func openYDBDriver(ctx context.Context, cfg extractConfig) (*ydb.Driver, error) {
	dsn := fmt.Sprintf("%s/%s", strings.TrimSpace(cfg.YDBEndpoint), strings.TrimSpace(cfg.YDBDatabase))
	authOpt, err := ydbAuthOption(ydbAuthOptions{
		Mode:                  cfg.YDBAuthMode,
		Login:                 cfg.YDBAuthLogin,
		Password:              cfg.YDBAuthPassword,
		ServiceAccountKeyFile: cfg.YDBAuthSACredentials,
		MetadataURL:           cfg.YDBAuthMetadataURL,
		CACertPath:            cfg.YDBCAPath,
	})
	if err != nil {
		return nil, err
	}

	openCtx := ctx
	cancel := func() {}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		openCtx, cancel = context.WithTimeout(ctx, cfg.YDBOpenTimeout)
	}
	defer cancel()

	opts := []ydb.Option{authOpt}
	if cfg.YDBCAPath != "" {
		opts = append(opts, ydb.WithCertificatesFromFile(cfg.YDBCAPath))
	}
	driver, err := ydb.Open(openCtx, dsn, opts...)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("open ydb connection timed out after %s: %w", cfg.YDBOpenTimeout, err)
		}
		return nil, fmt.Errorf("open ydb connection: %w", err)
	}
	return driver, nil
}

func ydbAuthOption(auth ydbAuthOptions) (ydb.Option, error) {
	mode := strings.ToLower(strings.TrimSpace(auth.Mode))
	switch mode {
	case "", "anonymous":
		return ydb.WithAnonymousCredentials(), nil
	case "static":
		if strings.TrimSpace(auth.Login) == "" {
			return nil, fmt.Errorf("ydb static auth login is required")
		}
		if strings.TrimSpace(auth.Password) == "" {
			return nil, fmt.Errorf("ydb static auth password is required")
		}
		return ydb.WithStaticCredentials(auth.Login, auth.Password), nil
	case "service-account-key", "service_account_key", "sa-key":
		if strings.TrimSpace(auth.ServiceAccountKeyFile) == "" {
			return nil, fmt.Errorf("ydb service account key file is required")
		}
		return yc.WithServiceAccountKeyFileCredentials(auth.ServiceAccountKeyFile), nil
	case "metadata":
		if strings.TrimSpace(auth.MetadataURL) != "" {
			return yc.WithMetadataCredentialsURL(auth.MetadataURL), nil
		}
		return yc.WithMetadataCredentials(), nil
	default:
		return nil, fmt.Errorf("unsupported ydb auth mode %q", auth.Mode)
	}
}

func describeColumns(ctx context.Context, driver *ydb.Driver, tablePath string) ([]string, error) {
	var desc options.Description
	err := driver.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		var err error
		desc, err = session.DescribeTable(ctx, tablePath)
		return err
	}, table.WithIdempotent())
	if err != nil {
		return nil, fmt.Errorf("describe table %s: %w", tablePath, err)
	}
	columns := make([]string, 0, len(desc.Columns))
	for _, col := range desc.Columns {
		columns = append(columns, col.Name)
	}
	return columns, nil
}

func buildQueryAndParams(cfg extractConfig, columns []string) (string, *table.QueryParameters, error) {
	var sql strings.Builder
	sql.WriteString("DECLARE $from_ts AS Timestamp64;\n")
	sql.WriteString("DECLARE $to_ts AS Timestamp64;\n")
	for i := range cfg.Filters {
		sql.WriteString(fmt.Sprintf("DECLARE $filter_%d AS List<Utf8>;\n", i))
	}
	sql.WriteString("SELECT ")
	for i, col := range columns {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(quoteYDBIdentifier(col))
	}
	sql.WriteString("\nFROM ")
	sql.WriteString(quoteYDBPath(cfg.YDBTable))
	sql.WriteString("\nWHERE ")
	sql.WriteString(quoteYDBIdentifier(cfg.TimeColumn))
	sql.WriteString(" >= $from_ts AND ")
	sql.WriteString(quoteYDBIdentifier(cfg.TimeColumn))
	sql.WriteString(" < $to_ts")
	for i, f := range cfg.Filters {
		sql.WriteString("\n  AND ")
		sql.WriteString(quoteYDBIdentifier(f.Field))
		sql.WriteString(fmt.Sprintf(" IN $filter_%d", i))
	}
	sql.WriteString("\nORDER BY ")
	sql.WriteString(quoteYDBIdentifier(cfg.TimeColumn))
	sql.WriteString(";\n")

	paramOpts := make([]table.ParameterOption, 0, 2+len(cfg.Filters))
	paramOpts = append(paramOpts,
		table.ValueParam("$from_ts", types.Timestamp64Value(cfg.From.UnixMicro())),
		table.ValueParam("$to_ts", types.Timestamp64Value(cfg.To.UnixMicro())),
	)
	for i, f := range cfg.Filters {
		values := make([]types.Value, 0, len(f.Values))
		for _, v := range f.Values {
			values = append(values, types.UTF8Value(v))
		}
		paramOpts = append(paramOpts, table.ValueParam(fmt.Sprintf("$filter_%d", i), types.ListValue(values...)))
	}
	return sql.String(), table.NewQueryParameters(paramOpts...), nil
}

func runExtraction(
	ctx context.Context,
	driver *ydb.Driver,
	query string,
	params *table.QueryParameters,
	columns []string,
	writer *extractionWriter,
) error {
	return driver.Table().Do(ctx, func(ctx context.Context, session table.Session) error {
		res, err := session.StreamExecuteScanQuery(ctx, query, params)
		if err != nil {
			return fmt.Errorf("execute scan query: %w", err)
		}
		defer func() {
			_ = res.Close()
		}()

		if !res.NextResultSet(ctx) {
			if err := res.Err(); err != nil {
				return err
			}
			return nil
		}

		scanners := make([]rawCellValue, len(columns))
		dest := make([]indexed.RequiredOrOptional, len(columns))
		row := make([]string, len(columns))
		for i := range scanners {
			dest[i] = &scanners[i]
		}

		for res.NextRow() {
			if err := res.Scan(dest...); err != nil {
				return fmt.Errorf("scan row: %w", err)
			}

			for i := range scanners {
				row[i] = normalizeTSVText(stringifyCell(scanners[i].value))
			}
			if err := writer.WriteRow(row); err != nil {
				return err
			}
		}
		return res.Err()
	}, table.WithIdempotent())
}

func newExtractionWriter(cfg extractConfig) (*extractionWriter, error) {
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create output-dir %s: %w", cfg.OutputDir, err)
	}
	w := &extractionWriter{
		outputDir:    cfg.OutputDir,
		outputPrefix: cfg.OutputPrefix,
		zstdEnabled:  cfg.ZstdEnabled,
		maxFileSize:  cfg.MaxFileSize,
	}
	if err := w.rotate(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *extractionWriter) WriteRow(cells []string) error {
	w.lineBuf.Reset()
	for i, cell := range cells {
		if i > 0 {
			w.lineBuf.WriteByte('\t')
		}
		w.lineBuf.WriteString(cell)
	}
	w.lineBuf.WriteByte('\n')

	if _, err := w.writer.Write(w.lineBuf.Bytes()); err != nil {
		return fmt.Errorf("write row into %s: %w", w.currentPath, err)
	}
	if w.zstd != nil {
		w.pendingPlainBytes += int64(w.lineBuf.Len())
		if w.pendingPlainBytes >= zstdSizeCheckInterval {
			if err := w.zstd.Flush(); err != nil {
				return fmt.Errorf("flush zstd writer for %s: %w", w.currentPath, err)
			}
			w.pendingPlainBytes = 0
		}
	}
	w.currentSize = w.base.n
	w.rowsWritten++
	if w.currentSize >= w.maxFileSize {
		if err := w.rotate(); err != nil {
			return err
		}
	}
	return nil
}

func (w *extractionWriter) rotate() error {
	if err := w.closeCurrent(); err != nil {
		return err
	}
	w.currentIndex++

	fileName := fmt.Sprintf("%s_%06d.tsv", w.outputPrefix, w.currentIndex)
	if w.zstdEnabled {
		fileName += ".zst"
	}
	path := filepath.Join(w.outputDir, fileName)
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create output file %s: %w", path, err)
	}

	base := &countingWriter{w: file}
	w.file = file
	w.base = base
	w.currentPath = path
	w.currentSize = 0
	w.writer = base
	w.zstd = nil
	w.pendingPlainBytes = 0

	if w.zstdEnabled {
		encoder, err := zstd.NewWriter(base)
		if err != nil {
			_ = file.Close()
			return fmt.Errorf("create zstd writer for %s: %w", path, err)
		}
		w.zstd = encoder
		w.writer = encoder
	}
	return nil
}

func (w *extractionWriter) closeCurrent() error {
	var closeErr error
	if w.zstd != nil {
		if err := w.zstd.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
		w.zstd = nil
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			closeErr = errors.Join(closeErr, err)
		}
		w.file = nil
	}
	if closeErr != nil {
		return fmt.Errorf("close output file: %w", closeErr)
	}
	return nil
}

func (w *extractionWriter) Close() error {
	return w.closeCurrent()
}

func stringifyCell(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case bool:
		return strconv.FormatBool(x)
	case int:
		return strconv.FormatInt(int64(x), 10)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case []byte:
		return string(x)
	case fmt.Stringer:
		return x.String()
	default:
		return fmt.Sprint(x)
	}
}

func normalizeTSVText(s string) string {
	if !strings.ContainsAny(s, "\t\n\r") {
		return s
	}
	return tsvTextReplacer.Replace(s)
}

func contains(items []string, target string) bool {
	for _, item := range items {
		if item == target {
			return true
		}
	}
	return false
}

func quoteYDBPath(path string) string {
	return "`" + strings.ReplaceAll(path, "`", "_") + "`"
}

func quoteYDBIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "_") + "`"
}

func parseByteSize(raw string) (int64, error) {
	s := strings.TrimSpace(strings.ToUpper(raw))
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	units := []struct {
		suffix string
		value  int64
	}{
		{"KIB", 1 << 10},
		{"MIB", 1 << 20},
		{"GIB", 1 << 30},
		{"KB", 1000},
		{"MB", 1000 * 1000},
		{"GB", 1000 * 1000 * 1000},
		{"B", 1},
	}

	for _, unit := range units {
		if strings.HasSuffix(s, unit.suffix) {
			base := strings.TrimSpace(strings.TrimSuffix(s, unit.suffix))
			if base == "" {
				return 0, fmt.Errorf("missing size value")
			}
			n, err := strconv.ParseInt(base, 10, 64)
			if err != nil {
				return 0, err
			}
			return n * unit.value, nil
		}
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	return n, nil
}
