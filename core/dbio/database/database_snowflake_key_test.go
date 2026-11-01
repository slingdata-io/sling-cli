package database

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Secret managers can store a PEM on one line (the Azure portal does this).
func TestGetEncodedPrivateKeyOneLine(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	pemStr := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))

	want, err := getEncodedPrivateKey(pemStr, "")
	require.NoError(t, err)

	trimmed := strings.TrimSpace(pemStr)
	cases := map[string]string{
		"spaces":       strings.ReplaceAll(trimmed, "\n", " "),
		"no separator": strings.ReplaceAll(trimmed, "\n", ""),
		"escaped":      strings.ReplaceAll(trimmed, "\n", `\n`),
		"escaped crlf": strings.ReplaceAll(trimmed, "\n", `\r\n`),
		"crlf":         strings.ReplaceAll(trimmed, "\n", "\r\n"),
	}
	for name, in := range cases {
		got, err := getEncodedPrivateKey(in, "")
		if assert.NoError(t, err, name) {
			assert.Equal(t, want, got, name)
		}
	}

	pkcs1 := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	_, err = getEncodedPrivateKey(strings.ReplaceAll(strings.TrimSpace(pkcs1), "\n", " "), "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PKCS#1")
}
