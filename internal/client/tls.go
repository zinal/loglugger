package client

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// LoadClientTLSConfig loads TLS config for the client.
func LoadClientTLSConfig(caFile, certFile, keyFile string, useSystemPool bool) (*tls.Config, error) {
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("tls client certificate and key must be configured together")
	}
	pool, err := loadCertPool(caFile, useSystemPool)
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

func loadCertPool(caFile string, useSystemPool bool) (*x509.CertPool, error) {
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
	if !useSystemPool && caFile == "" {
		return nil, fmt.Errorf("no CA certificates configured (set tls_ca_file or tls_use_system_pool)")
	}
	return pool, nil
}
