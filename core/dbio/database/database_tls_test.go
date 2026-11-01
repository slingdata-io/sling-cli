package database

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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestCert makes a self-signed certificate with only a DNS SAN
func newTestCert(t *testing.T, host string) (cert tls.Certificate, certPEM, keyPEM []byte) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: host},
		DNSNames:              []string{host},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	cert = tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return
}

func TestMakeTlsConfig(t *testing.T) {
	_, certPEM, keyPEM := newTestCert(t, "dbtls")
	dir := t.TempDir()
	certFile, keyFile := filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	require.NoError(t, os.WriteFile(certFile, certPEM, 0600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0600))

	for _, tt := range []struct {
		name       string
		props      map[string]string
		nilConfig  bool
		skipVerify bool
		clientCert bool
		rootCAs    bool
		err        bool
	}{
		{name: "ca without tls", props: map[string]string{"cert_ca_file": certFile}, nilConfig: true},
		{name: "ca with tls custom", props: map[string]string{"tls": "custom", "cert_ca_file": certFile}, rootCAs: true},
		{name: "ca with tls true", props: map[string]string{"tls": "true", "certificate_ca": certFile}, rootCAs: true},
		{name: "raw pem values", props: map[string]string{"certificate": string(certPEM), "certificate_key": string(keyPEM), "certificate_ca": string(certPEM)}, clientCert: true, rootCAs: true},
		{name: "new keys as files", props: map[string]string{"certificate": certFile, "certificate_key": keyFile}, clientCert: true},
		{name: "legacy raw pem", props: map[string]string{"cert_file": string(certPEM), "cert_key_file": string(keyPEM)}, clientCert: true},
		{name: "skip-verify with client cert", props: map[string]string{"tls": "skip-verify", "cert_file": certFile, "cert_key_file": keyFile}, skipVerify: true, clientCert: true},
		{name: "invalid ca", props: map[string]string{"tls": "true", "cert_ca_file": "not-a-file"}, err: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, err := NewConn("mysql://u:p@dbtls:3306/db")
			require.NoError(t, err)
			for k, v := range tt.props {
				conn.SetProp(k, v)
			}

			tlsConfig, err := conn.(*MySQLConn).makeTlsConfig()
			if tt.err {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.nilConfig {
				assert.Nil(t, tlsConfig)
				return
			}
			require.NotNil(t, tlsConfig)
			assert.Equal(t, tt.skipVerify, tlsConfig.InsecureSkipVerify)
			assert.Equal(t, tt.clientCert, len(tlsConfig.Certificates) == 1)
			assert.Equal(t, tt.rootCAs, tlsConfig.RootCAs != nil)
		})
	}
}

func TestMySQLTlsURL(t *testing.T) {
	conn, err := NewConn("mysql://u:p@mysqltls:3306/db")
	require.NoError(t, err)
	myConn := conn.(*MySQLConn)
	connID := myConn.GetProp("sling_conn_id")

	myConn.SetProp("tls", "true")
	assert.Contains(t, myConn.GetURL(), "tls=true", "no registered config, keep the driver value")

	myConn.tlsRegistered = true
	assert.Contains(t, myConn.GetURL(), "tls="+connID)

	myConn.SetProp("tls", "skip-verify")
	assert.Contains(t, myConn.GetURL(), "tls=skip-verify")
}

func TestSQLServerTunnelURL(t *testing.T) {
	conn, err := NewConn("sqlserver://sa:p@mssqltls:1433?database=master")
	require.NoError(t, err)
	msConn := conn.(*MsSQLServerConn)
	tunnelURL := "sqlserver://sa:p@127.0.0.1:5555?database=master&encrypt=true"

	assert.NotContains(t, msConn.GetURL(tunnelURL), "hostNameInCertificate", "no tunnel")

	msConn.tlsHost = "mssqltls"
	assert.Contains(t, msConn.GetURL(tunnelURL), "hostNameInCertificate=mssqltls")

	userURL := msConn.GetURL(tunnelURL + "&hostnameincertificate=other")
	assert.Contains(t, userURL, "hostnameincertificate=other")
	assert.NotContains(t, userURL, "mssqltls", "keep the value of the user")
}

func TestOpenClickhouseTunnel(t *testing.T) {
	cert, _, _ := newTestCert(t, "chtls")

	for _, tt := range []struct {
		name, params, tlsHost, sni string
	}{
		{"secure", "secure=true", "chtls", "chtls"},
		{"skip verify", "secure=true&skip_verify=true", "chtls", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sni := make(chan string, 1)
			serverTLS := &tls.Config{
				Certificates: []tls.Certificate{cert},
				GetConfigForClient: func(hello *tls.ClientHelloInfo) (*tls.Config, error) {
					sni <- hello.ServerName
					return nil, nil
				},
			}
			ln, err := tls.Listen("tcp", "127.0.0.1:0", serverTLS)
			require.NoError(t, err)
			defer ln.Close()
			go func() {
				if c, err := ln.Accept(); err == nil {
					_ = c.(*tls.Conn).Handshake()
					c.Close()
				}
			}()

			connURL := "clickhouse://default:p@" + ln.Addr().String() + "/default?dial_timeout=2s&" + tt.params
			db, err := openClickhouseTunnel(connURL, tt.tlsHost)
			require.NoError(t, err)
			defer db.Close()
			_ = db.Ping() // the fake server stops after the handshake

			select {
			case got := <-sni:
				assert.Equal(t, tt.sni, got)
			case <-time.After(5 * time.Second):
				t.Fatal("no TLS client hello")
			}
		})
	}
}
