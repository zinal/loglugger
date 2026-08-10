package client

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadClientTLSConfigRequiresCertAndKeyTogether(t *testing.T) {
	t.Parallel()

	if _, err := LoadClientTLSConfig("", "cert.pem", "", true); err == nil {
		t.Fatal("expected error when only cert is set")
	}
	if _, err := LoadClientTLSConfig("", "", "key.pem", true); err == nil {
		t.Fatal("expected error when only key is set")
	}
}

func TestLoadClientTLSConfigRequiresCAWithoutSystemPool(t *testing.T) {
	t.Parallel()

	if _, err := LoadClientTLSConfig("", "", "", false); err == nil {
		t.Fatal("expected error when neither CA file nor system pool is configured")
	}
}

func TestLoadClientTLSConfigLoadsCAAndClientCert(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	caFile, certFile, keyFile, err := writeClientTLSFixture(dir)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadClientTLSConfig(caFile, certFile, keyFile, false)
	if err != nil {
		t.Fatalf("LoadClientTLSConfig() error = %v", err)
	}
	if cfg.RootCAs == nil {
		t.Fatal("RootCAs is nil")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("len(Certificates) = %d, want 1", len(cfg.Certificates))
	}
}

func TestLoadClientTLSConfigRejectsInvalidCA(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	caFile := filepath.Join(dir, "bad-ca.pem")
	if err := os.WriteFile(caFile, []byte("not-a-cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadClientTLSConfig(caFile, "", "", false); err == nil {
		t.Fatal("expected error for invalid CA PEM")
	}
}

func writeClientTLSFixture(dir string) (caFile, certFile, keyFile string, err error) {
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return "", "", "", err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "client.local",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return "", "", "", err
	}
	caFile = filepath.Join(dir, "ca.pem")
	certFile = filepath.Join(dir, "client.crt")
	keyFile = filepath.Join(dir, "client.key")
	pemCert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(caFile, pemCert, 0o600); err != nil {
		return "", "", "", err
	}
	if err := os.WriteFile(certFile, pemCert, 0o600); err != nil {
		return "", "", "", err
	}
	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		return "", "", "", err
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return "", "", "", err
	}
	return caFile, certFile, keyFile, nil
}
