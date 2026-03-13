package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"path/filepath"
)

// LoadClientTLSConfig loads TLS config for the client.
func LoadClientTLSConfig(caFile, caPath, certFile, keyFile string, useSystemPool bool) (*tls.Config, error) {
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("tls client certificate and key must be configured together")
	}
	pool, err := loadCertPool(caFile, caPath, useSystemPool)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func loadCertPool(caFile, caPath string, useSystemPool bool) (*x509.CertPool, error) {
	var pool *x509.CertPool
	if useSystemPool {
		pool, _ = x509.SystemCertPool()
	}
	if pool == nil {
		pool = x509.NewCertPool()
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("no certificates found in %s", caFile)
		}
	}
	if caPath != "" {
		entries, err := os.ReadDir(caPath)
		if err != nil {
			return nil, fmt.Errorf("read CA path: %w", err)
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			fp := filepath.Join(caPath, e.Name())
			pem, err := os.ReadFile(fp)
			if err != nil {
				continue
			}
			pool.AppendCertsFromPEM(pem)
		}
	}
	if !useSystemPool && caFile == "" && caPath == "" {
		return nil, fmt.Errorf("no CA certificates configured (set tls_ca_file, tls_ca_path, or tls_use_system_pool)")
	}
	return pool, nil
}
