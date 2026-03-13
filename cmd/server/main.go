package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/mzinal/loglugger/internal/server"
)

type serverConfig struct {
	ListenAddr           string
	WriterBackend        string
	YDBEndpoint          string
	YDBDatabase          string
	YDBTable             string
	PositionStoreBackend string
	PositionTable        string
	FieldMappingFile     string
	TLSCertFile          string
	TLSKeyFile           string
	TLSCAFile            string
	TLSCAPath            string
	TLSClientSubjectCN   string
	TLSClientSubjectO    string
	TLSClientSubjectOU   string
}

func main() {
	cfg := parseServerConfig()

	mappings, err := loadMappings(cfg)
	if err != nil {
		slog.Error("load field mappings", "error", err)
		os.Exit(1)
	}
	positions, err := newPositionStore(cfg)
	if err != nil {
		slog.Error("create position store", "error", err)
		os.Exit(1)
	}
	if closer, ok := positions.(interface{ Close(context.Context) error }); ok {
		defer func() {
			if err := closer.Close(context.Background()); err != nil {
				slog.Warn("close position store", "error", err)
			}
		}()
	}
	writer, err := newWriter(cfg)
	if err != nil {
		slog.Error("create writer", "error", err)
		os.Exit(1)
	}
	if closer, ok := writer.(interface{ Close(context.Context) error }); ok {
		defer func() {
			if err := closer.Close(context.Background()); err != nil {
				slog.Warn("close writer", "error", err)
			}
		}()
	}

	mapper := server.NewMapper(mappings)
	handler := server.NewHandler(positions, mapper, writer, fullTablePath(cfg.YDBDatabase, cfg.YDBTable))
	mux := http.NewServeMux()
	mux.Handle("/v1/batches", handler)

	tlsConfig, err := server.LoadServerTLSConfig(server.ServerTLSOptions{
		CertFile:  cfg.TLSCertFile,
		KeyFile:   cfg.TLSKeyFile,
		CAFile:    cfg.TLSCAFile,
		CAPath:    cfg.TLSCAPath,
		AllowedCN: parseCSVList(cfg.TLSClientSubjectCN),
		AllowedO:  parseCSVList(cfg.TLSClientSubjectO),
		AllowedOU: parseCSVList(cfg.TLSClientSubjectOU),
	})
	if err != nil {
		slog.Error("load TLS config", "error", err)
		os.Exit(1)
	}

	srv := &http.Server{
		Addr:      cfg.ListenAddr,
		Handler:   mux,
		TLSConfig: tlsConfig,
	}

	slog.Info("starting server", "addr", cfg.ListenAddr, "writer", cfg.WriterBackend, "position_store", cfg.PositionStoreBackend)
	if err := srv.ListenAndServeTLS("", ""); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func parseServerConfig() serverConfig {
	cfg := serverConfig{}
	flag.StringVar(&cfg.ListenAddr, "listen", ":8443", "Listen address")
	flag.StringVar(&cfg.WriterBackend, "writer", "mock", "Writer backend: mock or ydb")
	flag.StringVar(&cfg.YDBEndpoint, "ydb-endpoint", "", "YDB endpoint")
	flag.StringVar(&cfg.YDBDatabase, "ydb-database", "", "YDB database path")
	flag.StringVar(&cfg.YDBTable, "ydb-table", "logs", "YDB table name")
	flag.StringVar(&cfg.PositionStoreBackend, "position-store", "memory", "Position store backend: memory or ydb")
	flag.StringVar(&cfg.PositionTable, "position-table", "loglugger_positions", "YDB table name for stored client positions")
	flag.StringVar(&cfg.FieldMappingFile, "field-mapping-file", "", "Path to YAML or JSON field mapping file")
	flag.StringVar(&cfg.TLSCertFile, "tls-cert-file", "", "Server certificate file")
	flag.StringVar(&cfg.TLSKeyFile, "tls-key-file", "", "Server private key file")
	flag.StringVar(&cfg.TLSCAFile, "tls-ca-file", "", "Client CA certificate file")
	flag.StringVar(&cfg.TLSCAPath, "tls-ca-path", "", "Client CA certificate directory")
	flag.StringVar(&cfg.TLSClientSubjectCN, "tls-client-subject-cn", "", "Allowed client certificate common names (comma-separated)")
	flag.StringVar(&cfg.TLSClientSubjectO, "tls-client-subject-o", "", "Allowed client certificate organizations (comma-separated)")
	flag.StringVar(&cfg.TLSClientSubjectOU, "tls-client-subject-ou", "", "Allowed client certificate organizational units (comma-separated)")
	flag.Parse()
	return cfg
}

func loadMappings(cfg serverConfig) ([]server.FieldMapping, error) {
	if cfg.FieldMappingFile != "" {
		return server.LoadFieldMappings(cfg.FieldMappingFile)
	}
	return []server.FieldMapping{
		{Source: "message", Destination: "message"},
		{Source: "parsed.P_DTTM", Destination: "log_dttm"},
		{Source: "parsed.P_SERVICE", Destination: "service_name"},
		{Source: "parsed.P_LEVEL", Destination: "log_level"},
		{Source: "parsed.P_MESSAGE", Destination: "log_message"},
		{Source: "syslog_identifier", Destination: "syslog_id"},
		{Source: "realtime_timestamp", Destination: "ts_epoch_us", Transform: "int64"},
		{Source: "monotonic_timestamp", Destination: "monotonic_ts", Transform: "uint64"},
		{Source: "priority", Destination: "priority", Transform: "int"},
		{Source: "client_id", Destination: "client_id"},
	}, nil
}

func newPositionStore(cfg serverConfig) (server.PositionStore, error) {
	switch cfg.PositionStoreBackend {
	case "memory":
		return server.NewMemoryPositionStore(), nil
	case "ydb":
		return server.NewYDBPositionStore(context.Background(), cfg.YDBEndpoint, cfg.YDBDatabase, fullTablePath(cfg.YDBDatabase, cfg.PositionTable))
	default:
		return nil, fmt.Errorf("unsupported position store backend %q", cfg.PositionStoreBackend)
	}
}

func newWriter(cfg serverConfig) (server.Writer, error) {
	switch cfg.WriterBackend {
	case "mock":
		return server.NewMockWriter(), nil
	case "ydb":
		return server.NewYDBWriter(context.Background(), cfg.YDBEndpoint, cfg.YDBDatabase)
	default:
		return nil, fmt.Errorf("unsupported writer backend %q", cfg.WriterBackend)
	}
}

func parseCSVList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func fullTablePath(database, table string) string {
	if strings.HasPrefix(table, "/") || database == "" {
		return table
	}
	return strings.TrimRight(database, "/") + "/" + strings.TrimLeft(table, "/")
}
