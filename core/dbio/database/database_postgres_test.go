package database

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenPgxTunnelVerifyFull checks verify-full through a tunnel on 127.0.0.1,
// with a certificate that has only a DNS SAN (GitHub issue #733).
func TestOpenPgxTunnelVerifyFull(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "pgtls"},
		DNSNames:              []string{"pgtls"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)

	rootCert := filepath.Join(t.TempDir(), "root.pem")
	require.NoError(t, os.WriteFile(rootCert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600))
	serverTLS := &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}

	// handshake accepts one connection, answers the SSLRequest and gives the TLS result
	handshake := func(ln net.Listener) (serverName string, err error) {
		c, err := ln.Accept()
		if err != nil {
			return "", err
		}
		defer c.Close()
		if _, err = io.ReadFull(c, make([]byte, 8)); err != nil {
			return "", err
		}
		if _, err = c.Write([]byte("S")); err != nil {
			return "", err
		}
		tc := tls.Server(c, serverTLS)
		err = tc.Handshake()
		return tc.ConnectionState().ServerName, err
	}

	for _, tt := range []struct {
		name    string
		tlsHost string
		ok      bool
	}{
		{"original host", "pgtls", true},
		{"tunnel address", "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer ln.Close()

			type result struct {
				serverName string
				err        error
			}
			done := make(chan result, 1)
			go func() {
				name, err := handshake(ln)
				done <- result{name, err}
			}()

			connURL := "postgresql://u:p@" + ln.Addr().String() + "/db?sslmode=verify-full&connect_timeout=5&sslrootcert=" + rootCert
			db, err := openPgx(connURL, tt.tlsHost)
			require.NoError(t, err)
			defer db.Close()
			_ = db.Ping() // the fake server stops after the handshake

			res := <-done
			if tt.ok {
				assert.NoError(t, res.err)
				assert.Equal(t, "pgtls", res.serverName)
			} else {
				assert.Error(t, res.err, "client must reject a certificate without the 127.0.0.1 IP SAN")
			}
		})
	}
}
