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
  - source: priority
    destination: priority
    transform: int
  - source: fields.CODE_LINE
    destination: code_line
    transform: int64
  - source: parsed.MISSING
    destination: fallback
    default: unknown
`), 0o600); err != nil {
		t.Fatal(err)
	}

	mappings, err := LoadFieldMappings(path)
	if err != nil {
		t.Fatal(err)
	}
	mapper := NewMapper(mappings)
	priority := 6
	row, err := mapper.MapRecord("client-1", models.Record{
		Parsed:   map[string]string{"P_LEVEL": "INFO"},
		Priority: &priority,
		Fields:   map[string]string{"CODE_LINE": "42"},
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
}

func TestQuoteYDBPath(t *testing.T) {
	got := quoteYDBPath("/local/path`withtick")
	want := "`/local/path``withtick`"
	if got != want {
		t.Fatalf("quoteYDBPath() = %q, want %q", got, want)
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
