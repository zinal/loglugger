package server

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ydb-platform/loglugger/internal/models"
)

func TestLoadFieldMappingsYAMLAndTransforms(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mapping.yaml")
	if err := os.WriteFile(path, []byte(`
field_mapping:
  - source: parsed.P_LEVEL
    destination: level
  - source: parsed.P_DTTM
    destination: ts_orig
    transform: timestamp64
  - source: priority
    destination: priority
    transform: int
  - source: fields.CODE_LINE
    destination: code_line
    transform: int64
  - source: parsed.MISSING
    destination: fallback
    default: unknown
  - source: log_timestamp_us
    destination: log_timestamp_us
    transform: timestamp64_us
  - source: message_cityhash64
    destination: message_hash
    transform: uint64
`), 0o600); err != nil {
		t.Fatal(err)
	}

	mappings, err := LoadFieldMappings(path)
	if err != nil {
		t.Fatal(err)
	}
	mapper := NewMapper(mappings)
	recordTs := int64(1710345600000000)
	priority := 6
	row, err := mapper.MapRecord("client-1", models.Record{
		Parsed:            map[string]string{"P_LEVEL": "INFO", "P_DTTM": "2025-03-13T10:00:00"},
		Priority:          &priority,
		RealtimeTimestamp: &recordTs,
		Fields:            map[string]string{"CODE_LINE": "42"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if row["level"] != "INFO" {
		t.Fatalf("level = %v, want INFO", row["level"])
	}
	if row["priority"] != 6 {
		t.Fatalf("priority = %#v, want int 6", row["priority"])
	}
	if row["code_line"] != int64(42) {
		t.Fatalf("code_line = %#v, want int64(42)", row["code_line"])
	}
	if row["fallback"] != "unknown" {
		t.Fatalf("fallback = %#v, want unknown", row["fallback"])
	}
	logTS, ok := row["log_timestamp_us"].(time.Time)
	if !ok {
		t.Fatalf("log_timestamp_us = %#v, want time.Time", row["log_timestamp_us"])
	}
	if got := logTS.UTC().UnixMicro(); got != recordTs {
		t.Fatalf("log_timestamp_us UTC unix micro = %d, want %d", got, recordTs)
	}
	tsOrig, ok := row["ts_orig"].(time.Time)
	if !ok {
		t.Fatalf("ts_orig = %#v, want time.Time", row["ts_orig"])
	}
	if tsOrig.UTC().Format("2006-01-02T15:04:05") != "2025-03-13T10:00:00" {
		t.Fatalf("ts_orig UTC = %s, want 2025-03-13T10:00:00", tsOrig.UTC().Format("2006-01-02T15:04:05"))
	}
	if _, ok := row["message_hash"].(uint64); !ok {
		t.Fatalf("message_hash = %#v, want uint64", row["message_hash"])
	}
}

func TestMapper_MessageHashStableForSameRecord(t *testing.T) {
	mapper := NewMapper([]FieldMapping{
		{Source: "message_cityhash64", Destination: "message_hash", Transform: "uint64"},
	})
	rec := models.Record{
		Parsed: map[string]string{
			"P_LEVEL": "INFO",
			"P_DTTM":  "2025-03-13T10:00:00",
		},
		Fields: map[string]string{
			"_HOSTNAME": "node-1",
			"CODE_FILE": "main.go",
		},
	}

	first, err := mapper.MapRecord("client-1", rec)
	if err != nil {
		t.Fatal(err)
	}
	second, err := mapper.MapRecord("client-1", rec)
	if err != nil {
		t.Fatal(err)
	}

	if first["message_hash"] != second["message_hash"] {
		t.Fatalf("hash mismatch for identical records: %v != %v", first["message_hash"], second["message_hash"])
	}
}

func TestMapper_TimestampWithoutZoneUsesLocalWhenEnabled(t *testing.T) {
	original := time.Local
	local := time.FixedZone("LOCAL+03", 3*60*60)
	time.Local = local
	t.Cleanup(func() { time.Local = original })

	mapper := NewMapperWithOptions([]FieldMapping{
		{Source: "parsed.P_DTTM", Destination: "ts_orig", Transform: "timestamp64"},
	}, MapperOptions{ConvertTimeToLocalTZ: true})

	row, err := mapper.MapRecord("client-1", models.Record{
		Parsed: map[string]string{"P_DTTM": "2025-03-13T10:00:00"},
	})
	if err != nil {
		t.Fatal(err)
	}

	ts, ok := row["ts_orig"].(time.Time)
	if !ok {
		t.Fatalf("ts_orig = %#v, want time.Time", row["ts_orig"])
	}
	if ts.Location().String() != local.String() {
		t.Fatalf("ts_orig location = %q, want %q", ts.Location(), local)
	}
	if ts.UTC().Format("2006-01-02T15:04:05") != "2025-03-13T07:00:00" {
		t.Fatalf("ts_orig UTC = %s, want 2025-03-13T07:00:00", ts.UTC().Format("2006-01-02T15:04:05"))
	}
}

func TestMapper_TimestampWithZoneConvertedToLocalWhenEnabled(t *testing.T) {
	original := time.Local
	local := time.FixedZone("LOCAL+03", 3*60*60)
	time.Local = local
	t.Cleanup(func() { time.Local = original })

	mapper := NewMapperWithOptions([]FieldMapping{
		{Source: "parsed.P_DTTM", Destination: "ts_orig", Transform: "timestamp64"},
	}, MapperOptions{ConvertTimeToLocalTZ: true})

	row, err := mapper.MapRecord("client-1", models.Record{
		Parsed: map[string]string{"P_DTTM": "2025-03-13T10:00:00Z"},
	})
	if err != nil {
		t.Fatal(err)
	}

	ts, ok := row["ts_orig"].(time.Time)
	if !ok {
		t.Fatalf("ts_orig = %#v, want time.Time", row["ts_orig"])
	}
	if ts.Location().String() != local.String() {
		t.Fatalf("ts_orig location = %q, want %q", ts.Location(), local)
	}
	if ts.UTC().Format("2006-01-02T15:04:05") != "2025-03-13T10:00:00" {
		t.Fatalf("ts_orig UTC = %s, want 2025-03-13T10:00:00", ts.UTC().Format("2006-01-02T15:04:05"))
	}
}

func TestQuoteYDBPath(t *testing.T) {
	got := quoteYDBPath("/local/path`withtick")
	want := "`/local/path``withtick`"
	if got != want {
		t.Fatalf("quoteYDBPath() = %q, want %q", got, want)
	}
}

func TestYDBAuthOptionValidation(t *testing.T) {
	tests := []struct {
		name    string
		auth    YDBAuthOptions
		wantErr bool
	}{
		{
			name:    "anonymous default",
			auth:    YDBAuthOptions{},
			wantErr: false,
		},
		{
			name: "static ok",
			auth: YDBAuthOptions{
				Mode:     "static",
				Login:    "user",
				Password: "pass",
			},
			wantErr: false,
		},
		{
			name: "static without login",
			auth: YDBAuthOptions{
				Mode:     "static",
				Password: "pass",
			},
			wantErr: true,
		},
		{
			name: "static without password",
			auth: YDBAuthOptions{
				Mode:  "static",
				Login: "user",
			},
			wantErr: true,
		},
		{
			name: "service account key ok",
			auth: YDBAuthOptions{
				Mode:                  "service-account-key",
				ServiceAccountKeyFile: "sa-key.json",
			},
			wantErr: false,
		},
		{
			name: "service account key missing file",
			auth: YDBAuthOptions{
				Mode: "service-account-key",
			},
			wantErr: true,
		},
		{
			name: "metadata default",
			auth: YDBAuthOptions{
				Mode: "metadata",
			},
			wantErr: false,
		},
		{
			name: "metadata with custom url",
			auth: YDBAuthOptions{
				Mode:        "metadata",
				MetadataURL: "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token",
			},
			wantErr: false,
		},
		{
			name: "unsupported mode",
			auth: YDBAuthOptions{
				Mode: "unknown",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ydbAuthOption(tt.auth)
			if tt.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestLoadServerTLSConfigSubjectValidation(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile, err := writeTLSFixture(dir)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadServerTLSConfig(ServerTLSOptions{
		CertFile:  certFile,
		KeyFile:   keyFile,
		CAFile:    caFile,
		AllowedCN: []string{"allowed-client"},
		AllowedO:  []string{"team-a"},
		AllowedOU: []string{"prod"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName:         "allowed-client",
			Organization:       []string{"team-a"},
			OrganizationalUnit: []string{"prod"},
		},
	}
	if err := cfg.VerifyConnection(tlsState(cert)); err != nil {
		t.Fatalf("VerifyConnection() error = %v", err)
	}

	badCert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName:         "blocked",
			Organization:       []string{"team-a"},
			OrganizationalUnit: []string{"prod"},
		},
	}
	if err := cfg.VerifyConnection(tlsState(badCert)); err == nil {
		t.Fatal("expected VerifyConnection to reject mismatched CN")
	}
}

func TestLoadServerTLSConfigSubjectRegexValidation(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile, caFile, err := writeTLSFixture(dir)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadServerTLSConfig(ServerTLSOptions{
		CertFile:  certFile,
		KeyFile:   keyFile,
		CAFile:    caFile,
		AllowedCN: []string{"regex:^client-[0-9]+$"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: "client-42",
		},
	}
	if err := cfg.VerifyConnection(tlsState(cert)); err != nil {
		t.Fatalf("VerifyConnection() error = %v", err)
	}

	blocked := &x509.Certificate{
		Subject: pkix.Name{
			CommonName: "worker-1",
		},
	}
	if err := cfg.VerifyConnection(tlsState(blocked)); err == nil {
		t.Fatal("expected VerifyConnection to reject non-matching CN regex")
	}
}

func writeTLSFixture(dir string) (certFile, keyFile, caFile string, err error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", "", err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "server.local",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return "", "", "", err
	}

	certFile = filepath.Join(dir, "server.crt")
	keyFile = filepath.Join(dir, "server.key")
	caFile = filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		return "", "", "", err
	}
	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return "", "", "", err
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return "", "", "", err
	}
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		return "", "", "", err
	}
	return certFile, keyFile, caFile, nil
}

func tlsState(cert *x509.Certificate) tls.ConnectionState {
	return tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
}
