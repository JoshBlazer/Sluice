package leader

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
	"strings"
	"testing"
	"time"
)

func writeTestCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestTLSConfig(t *testing.T) {
	if c, err := (ClientConfig{}).tlsConfig(); c != nil || err != nil {
		t.Fatalf("no TLS settings: got %v, %v; want plaintext", c, err)
	}

	c, err := (ClientConfig{CAFile: writeTestCA(t)}).tlsConfig()
	if err != nil || c == nil || c.RootCAs == nil {
		t.Fatalf("CA file: got %v, %v; want TLS with a root pool", c, err)
	}

	notPEM := filepath.Join(t.TempDir(), "junk.pem")
	os.WriteFile(notPEM, []byte("not a certificate"), 0o600)
	errCases := map[string]ClientConfig{
		"missing CA file":  {CAFile: filepath.Join(t.TempDir(), "absent.pem")},
		"no PEM":           {CAFile: notPEM},
		"cert without key": {CertFile: "client.pem"},
		"key without cert": {KeyFile: "client-key.pem"},
	}
	for name, cfg := range errCases {
		if _, err := cfg.tlsConfig(); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if _, err := (ClientConfig{CAFile: notPEM}).tlsConfig(); err == nil || !strings.Contains(err.Error(), "no PEM") {
		t.Errorf("no PEM: error should say so, got %v", err)
	}
}
