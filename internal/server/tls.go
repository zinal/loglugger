package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"regexp"
	"strings"
)

type ServerTLSOptions struct {
	CertFile  string
	KeyFile   string
	CAFile    string
	AllowedCN []string
	AllowedO  []string
	AllowedOU []string
}

// LoadServerTLSConfig builds the server TLS config with optional subject checks.
func LoadServerTLSConfig(opts ServerTLSOptions) (*tls.Config, error) {
	if opts.CertFile == "" || opts.KeyFile == "" {
		return nil, fmt.Errorf("tls server certificate and key are required")
	}
	if opts.CAFile == "" {
		return nil, fmt.Errorf("client CA file is required for mTLS")
	}

	cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server certificate: %w", err)
	}
	clientPool, err := loadServerCertPool(opts.CAFile)
	if err != nil {
		return nil, err
	}

	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientPool,
	}
	if len(opts.AllowedCN) > 0 || len(opts.AllowedO) > 0 || len(opts.AllowedOU) > 0 {
		cfg.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return fmt.Errorf("missing client certificate")
			}
			subject := state.PeerCertificates[0].Subject
			if len(opts.AllowedCN) > 0 && !matchesAllowed(subject.CommonName, opts.AllowedCN) {
				return fmt.Errorf("client certificate common name %q is not allowed", subject.CommonName)
			}
			if len(opts.AllowedO) > 0 && !containsAnyMatch(subject.Organization, opts.AllowedO) {
				return fmt.Errorf("client certificate organization is not allowed")
			}
			if len(opts.AllowedOU) > 0 && !containsAnyMatch(subject.OrganizationalUnit, opts.AllowedOU) {
				return fmt.Errorf("client certificate organizational unit is not allowed")
			}
			return nil
		}
	}
	return cfg, nil
}

func loadServerCertPool(caFile string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read client CA file: %w", err)
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no certificates found in %s", caFile)
	}
	return pool, nil
}

func containsAnyMatch(values []string, allowed []string) bool {
	for _, value := range values {
		if matchesAllowed(value, allowed) {
			return true
		}
	}
	return false
}

func matchesAllowed(value string, allowed []string) bool {
	for _, candidate := range allowed {
		if strings.HasPrefix(candidate, "regex:") {
			pattern := strings.TrimPrefix(candidate, "regex:")
			re, err := regexp.Compile(pattern)
			if err != nil {
				continue
			}
			if re.MatchString(value) {
				return true
			}
			continue
		}
		if candidate == value {
			return true
		}
	}
	return false
}
