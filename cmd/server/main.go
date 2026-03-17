package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ydb-platform/loglugger/internal/buildinfo"
	"github.com/ydb-platform/loglugger/internal/server"
	"gopkg.in/yaml.v3"
)

type serverConfig struct {
	ListenAddr               string   `json:"listen_addr" yaml:"listen_addr"`
	WriterBackend            string   `json:"writer_backend" yaml:"writer_backend"`
	MaxCompressedBodyBytes   int64    `json:"max_compressed_body_bytes" yaml:"max_compressed_body_bytes"`
	MaxDecompressedBodyBytes int64    `json:"max_decompressed_body_bytes" yaml:"max_decompressed_body_bytes"`
	YDBEndpoint              string   `json:"ydb_endpoint" yaml:"ydb_endpoint"`
	YDBDatabase              string   `json:"ydb_database" yaml:"ydb_database"`
	YDBTable                 string   `json:"ydb_table" yaml:"ydb_table"`
	YDBAuthMode              string   `json:"ydb_auth_mode" yaml:"ydb_auth_mode"`
	YDBAuthLogin             string   `json:"ydb_auth_login" yaml:"ydb_auth_login"`
	YDBAuthPassword          string   `json:"ydb_auth_password" yaml:"ydb_auth_password"`
	YDBAuthSACredentials     string   `json:"ydb_auth_sa_key_file" yaml:"ydb_auth_sa_key_file"`
	YDBAuthMetadataURL       string   `json:"ydb_auth_metadata_url" yaml:"ydb_auth_metadata_url"`
	YDBCAPath                string   `json:"ydb_ca_path" yaml:"ydb_ca_path"`
	YDBOpenTimeout           string   `json:"ydb_open_timeout" yaml:"ydb_open_timeout"`
	PositionTable            string   `json:"position_table" yaml:"position_table"`
	FieldMappingFile         string   `json:"field_mapping_file" yaml:"field_mapping_file"`
	ConvertTimeToLocalTZ     bool     `json:"convert_time_to_local_tz" yaml:"convert_time_to_local_tz"`
	TLSCertFile              string   `json:"tls_cert_file" yaml:"tls_cert_file"`
	TLSKeyFile               string   `json:"tls_key_file" yaml:"tls_key_file"`
	TLSCAFile                string   `json:"tls_ca_file" yaml:"tls_ca_file"`
	TLSClientSubjectCN       []string `json:"tls_client_subject_cn" yaml:"tls_client_subject_cn"`
	TLSClientSubjectO        []string `json:"tls_client_subject_o" yaml:"tls_client_subject_o"`
	TLSClientSubjectOU       []string `json:"tls_client_subject_ou" yaml:"tls_client_subject_ou"`
}

func main() {
	cfg, err := parseServerConfig()
	if err != nil {
		slog.Error("parse server config", "error", err)
		os.Exit(1)
	}

	mappings, err := loadMappings(cfg)
	if err != nil {
		slog.Error("load field mappings", "error", err)
		os.Exit(1)
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

	mapper := server.NewMapperWithOptions(mappings, server.MapperOptions{
		ConvertTimeToLocalTZ: cfg.ConvertTimeToLocalTZ,
	})
	handler := server.NewHandlerWithOptions(
		mapper,
		writer,
		fullTablePath(cfg.YDBDatabase, cfg.YDBTable),
		server.HandlerOptions{
			MaxCompressedBodyBytes:   cfg.MaxCompressedBodyBytes,
			MaxDecompressedBodyBytes: cfg.MaxDecompressedBodyBytes,
		},
	)
	mux := http.NewServeMux()
	mux.Handle("/v1/positions", handler)
	mux.Handle("/v1/batches", handler)

	tlsConfig, err := server.LoadServerTLSConfig(server.ServerTLSOptions{
		CertFile:  cfg.TLSCertFile,
		KeyFile:   cfg.TLSKeyFile,
		CAFile:    cfg.TLSCAFile,
		AllowedCN: cfg.TLSClientSubjectCN,
		AllowedO:  cfg.TLSClientSubjectO,
		AllowedOU: cfg.TLSClientSubjectOU,
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

	slog.Info("starting server", "version", buildinfo.Version, "addr", cfg.ListenAddr, "writer", cfg.WriterBackend)
	if err := srv.ListenAndServeTLS("", ""); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func parseServerConfig() (serverConfig, error) {
	configPath := flag.String("config", "", "Path to server YAML/JSON config file")
	listenOverride := flag.String("listen", "", "Optional listen address override")
	flag.Parse()

	if strings.TrimSpace(*configPath) == "" {
		return serverConfig{}, fmt.Errorf("config file is required (-config)")
	}

	cfg := defaultServerConfig()
	if err := loadServerConfigFile(*configPath, &cfg); err != nil {
		return serverConfig{}, err
	}
	if strings.TrimSpace(*listenOverride) != "" {
		cfg.ListenAddr = strings.TrimSpace(*listenOverride)
	}
	return cfg, nil
}

func defaultServerConfig() serverConfig {
	return serverConfig{
		ListenAddr:               ":27312",
		WriterBackend:            "mock",
		MaxCompressedBodyBytes:   8 << 20,
		MaxDecompressedBodyBytes: 32 << 20,
		YDBTable:                 "logs",
		YDBAuthMode:              "anonymous",
		YDBOpenTimeout:           "10s",
		PositionTable:            "loglugger_positions",
	}
}

func loadServerConfigFile(path string, cfg *serverConfig) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}

	switch strings.ToLower(filepath.Ext(path)) {
	case ".json":
		if err := json.Unmarshal(data, cfg); err != nil {
			return fmt.Errorf("decode JSON config file: %w", err)
		}
	default:
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return fmt.Errorf("decode YAML config file: %w", err)
		}
	}
	return nil
}

func loadMappings(cfg serverConfig) ([]server.FieldMapping, error) {
	if cfg.FieldMappingFile != "" {
		return server.LoadFieldMappings(cfg.FieldMappingFile)
	}
	return []server.FieldMapping{
		{Source: "message", Destination: "message"},
		{Source: "seqno", Destination: "seqno", Transform: "int64"},
		{Source: "parsed.P_DTTM", Destination: "log_dttm", Transform: "timestamp64"},
		{Source: "parsed.P_SERVICE", Destination: "service_name"},
		{Source: "parsed.P_LEVEL", Destination: "log_level"},
		{Source: "parsed.P_MESSAGE", Destination: "log_message"},
		{Source: "syslog_identifier", Destination: "syslog_id"},
		{Source: "log_timestamp_us", Destination: "log_timestamp_us", Transform: "timestamp64_us"},
		{Source: "message_cityhash64", Destination: "message_hash", Transform: "uint64"},
		{Source: "priority", Destination: "priority", Transform: "int"},
		{Source: "client_id", Destination: "client_id"},
	}, nil
}

func newWriter(cfg serverConfig) (server.Writer, error) {
	switch cfg.WriterBackend {
	case "mock":
		return server.NewMockWriter(), nil
	case "ydb":
		openTimeout, err := time.ParseDuration(strings.TrimSpace(cfg.YDBOpenTimeout))
		if err != nil {
			return nil, fmt.Errorf("parse ydb_open_timeout: %w", err)
		}
		if openTimeout <= 0 {
			return nil, fmt.Errorf("ydb_open_timeout must be greater than zero")
		}
		return server.NewYDBWriter(
			context.Background(),
			cfg.YDBEndpoint,
			cfg.YDBDatabase,
			cfg.PositionTable,
			ydbAuthConfig(cfg),
			openTimeout,
		)
	default:
		return nil, fmt.Errorf("unsupported writer backend %q", cfg.WriterBackend)
	}
}

func ydbAuthConfig(cfg serverConfig) server.YDBAuthOptions {
	return server.YDBAuthOptions{
		Mode:                  cfg.YDBAuthMode,
		Login:                 cfg.YDBAuthLogin,
		Password:              cfg.YDBAuthPassword,
		ServiceAccountKeyFile: cfg.YDBAuthSACredentials,
		MetadataURL:           cfg.YDBAuthMetadataURL,
		CACertPath:            strings.TrimSpace(cfg.YDBCAPath),
	}
}

func fullTablePath(database, table string) string {
	if strings.HasPrefix(table, "/") || database == "" {
		return table
	}
	return strings.TrimRight(database, "/") + "/" + strings.TrimLeft(table, "/")
}
