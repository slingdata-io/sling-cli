package database

import (
	"crypto/tls"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOpenPgxTunnelVerifyFull checks verify-full through a tunnel on 127.0.0.1,
// with a certificate that has only a DNS SAN (GitHub issue #733).
func TestOpenPgxTunnelVerifyFull(t *testing.T) {
	cert, certPEM, _ := newTestCert(t, "pgtls")
	rootCert := filepath.Join(t.TempDir(), "root.pem")
	require.NoError(t, os.WriteFile(rootCert, certPEM, 0600))
	serverTLS := &tls.Config{Certificates: []tls.Certificate{cert}}

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
