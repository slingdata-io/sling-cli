package iop

import (
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

func newTestHostKey(t *testing.T) ssh.Signer {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(priv)
	require.NoError(t, err)
	return signer
}

// setTestHome makes sure that the user's ~/.ssh/known_hosts has no effect
func setTestHome(t *testing.T) string {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	return home
}

func writeKnownHosts(t *testing.T, path, addr string, key ssh.PublicKey) {
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	line := knownhosts.Line([]string{knownhosts.Normalize(addr)}, key) + "\n"
	require.NoError(t, os.WriteFile(path, []byte(line), 0o600))
}

func TestSSHHostKeyCallback(t *testing.T) {
	home := setTestHome(t)

	server := newTestHostKey(t).PublicKey()
	other := newTestHostKey(t).PublicKey()
	fingerprint := ssh.FingerprintSHA256(server)
	authorizedKey := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(server)))
	remote := &net.TCPAddr{IP: net.ParseIP("10.0.0.5"), Port: 2222}
	addr := "sftp.example.test:2222"

	knownFile := filepath.Join(home, "custom_known_hosts")
	writeKnownHosts(t, knownFile, addr, server)
	otherKnownFile := filepath.Join(home, "other_known_hosts")
	writeKnownHosts(t, otherKnownFile, addr, other)

	cases := []struct {
		name    string
		hostKey SSHHostKey
		errLike string
	}{
		{name: "fingerprint matches", hostKey: NewSSHHostKey(fingerprint, "", "")},
		{name: "public key matches", hostKey: NewSSHHostKey(authorizedKey, "", "true")},
		{name: "one of several matches", hostKey: NewSSHHostKey(ssh.FingerprintSHA256(other)+", "+fingerprint, "", "")},
		{name: "invalid ssh_host_key", hostKey: NewSSHHostKey("abc123", "", ""), errLike: "invalid ssh_host_key"},
		{name: "fingerprint differs", hostKey: NewSSHHostKey(ssh.FingerprintSHA256(other), "", "false"), errLike: "is not in ssh_host_key or known_hosts"},
		{name: "fingerprint differs, known_hosts matches", hostKey: NewSSHHostKey(ssh.FingerprintSHA256(other), knownFile, "")},
		{name: "fingerprint differs, known_hosts differs", hostKey: NewSSHHostKey(ssh.FingerprintSHA256(other), otherKnownFile, ""), errLike: "is not in ssh_host_key or known_hosts"},
		{name: "not strict", hostKey: NewSSHHostKey("", otherKnownFile, "false")},
		{name: "unknown warns", hostKey: NewSSHHostKey("", "", "")},
		{name: "unknown strict", hostKey: NewSSHHostKey("", "", "true"), errLike: "ssh_host_key: " + fingerprint},
		{name: "known_hosts matches", hostKey: NewSSHHostKey("", knownFile, "true")},
		{name: "known_hosts differs warns", hostKey: NewSSHHostKey("", otherKnownFile, "")},
		{name: "known_hosts differs strict", hostKey: NewSSHHostKey("", otherKnownFile, "true"), errLike: "does not match known_hosts"},
		{name: "known_hosts missing", hostKey: NewSSHHostKey("", filepath.Join(home, "missing"), ""), errLike: "could not read ssh_known_hosts"},
	}

	check := func(hk SSHHostKey) error {
		callback, _, err := hk.Callback(addr)
		if err != nil {
			return err
		}
		return callback(addr, remote, server)
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := check(c.hostKey)
			if c.errLike == "" {
				assert.NoError(t, err)
			} else if assert.Error(t, err) {
				assert.Contains(t, err.Error(), c.errLike)
			}
		})
	}

	t.Run("default known_hosts", func(t *testing.T) {
		writeKnownHosts(t, filepath.Join(home, ".ssh", "known_hosts"), addr, server)
		defer os.Remove(filepath.Join(home, ".ssh", "known_hosts"))
		assert.NoError(t, check(NewSSHHostKey("", "", "true")))
	})

	t.Run("invalid default known_hosts is ignored", func(t *testing.T) {
		path := filepath.Join(home, ".ssh", "known_hosts")
		require.NoError(t, os.WriteFile(path, []byte("not a valid line\n"), 0o600))
		defer os.Remove(path)
		assert.NoError(t, check(NewSSHHostKey("", "", "")))
	})
}

// TestSSHHostKeyServer makes sure that the client does not send the password
// to a server with an unknown host key in strict mode
func TestSSHHostKeyServer(t *testing.T) {
	home := setTestHome(t)

	var passwordSent atomic.Bool
	config := &ssh.ServerConfig{
		PasswordCallback: func(ssh.ConnMetadata, []byte) (*ssh.Permissions, error) {
			passwordSent.Store(true)
			return nil, nil
		},
	}
	// the server has two key types, like most OpenSSH servers
	ecdsaKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	ecdsaSigner, err := ssh.NewSignerFromKey(ecdsaKey)
	require.NoError(t, err)
	ed25519Signer := newTestHostKey(t)
	config.AddHostKey(ecdsaSigner)
	config.AddHostKey(ed25519Signer)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, chans, reqs, err := ssh.NewServerConn(conn, config)
				if err != nil {
					return
				}
				go ssh.DiscardRequests(reqs)
				for ch := range chans {
					ch.Reject(ssh.Prohibited, "test")
				}
			}()
		}
	}()

	port := listener.Addr().(*net.TCPAddr).Port
	connectWith := func(options SSHOptions) error {
		client := &SSHClient{Host: "127.0.0.1", Port: port, User: "u", Password: "secret", Options: options}
		defer client.Close()
		return client.Connect()
	}
	connect := func(hostKey SSHHostKey) error {
		return connectWith(SSHOptions{HostKey: hostKey})
	}

	err = connect(NewSSHHostKey("", "", "true"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is not verified")
	assert.False(t, passwordSent.Load(), "password must not reach an unverified server")

	err = connect(NewSSHHostKey("", "", ""))
	require.NoError(t, err)
	assert.True(t, passwordSent.Load())

	// known_hosts has only the ed25519 key. The client must request it
	// instead of the ecdsa key, which Go prefers by default.
	knownFile := filepath.Join(home, "known_hosts")
	writeKnownHosts(t, knownFile, g.F("127.0.0.1:%d", port), ed25519Signer.PublicKey())
	assert.NoError(t, connect(NewSSHHostKey("", knownFile, "true")))

	assert.NoError(t, connect(NewSSHHostKey(ssh.FingerprintSHA256(ecdsaSigner.PublicKey()), "", "true")))

	// the server does not offer 3des-cbc, so the handshake must fail
	notStrict := NewSSHHostKey("", "", "false")
	assert.NoError(t, connectWith(SSHOptions{HostKey: notStrict, Ciphers: []string{ssh.CipherAES256CTR}}))
	err = connectWith(SSHOptions{HostKey: notStrict, Ciphers: []string{ssh.InsecureCipherTripleDESCBC}})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "no common algorithm for client to server cipher")
	}
}

func TestSSHOptionsConfig(t *testing.T) {
	options := NewSSHOptions(func(key string) string {
		return map[string]string{
			"ssh_ciphers":        "aes256-gcm@openssh.com, chacha20-poly1305@openssh.com",
			"ssh_kex_algorithms": "curve25519-sha256",
		}[key]
	})
	config, err := options.Config()
	require.NoError(t, err)
	assert.Equal(t, []string{ssh.CipherAES256GCM, ssh.CipherChaCha20Poly1305}, config.Ciphers)
	assert.Equal(t, []string{ssh.KeyExchangeCurve25519}, config.KeyExchanges)

	// the default offers legacy algorithms after the secure ones
	config, err = SSHOptions{}.Config()
	require.NoError(t, err)
	assert.Equal(t, ssh.SupportedAlgorithms().Ciphers[0], config.Ciphers[0])
	assert.Contains(t, config.Ciphers, ssh.InsecureCipherTripleDESCBC)
	assert.Equal(t, ssh.InsecureKeyExchangeDHGEXSHA1, config.KeyExchanges[len(config.KeyExchanges)-1])

	_, err = SSHOptions{Ciphers: []string{"aes256-gcm"}}.Config()
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), `invalid ssh_ciphers value "aes256-gcm"`)
	}
	_, err = SSHOptions{KeyExchanges: []string{"ssh-ed25519"}}.Config()
	assert.Error(t, err)
}

func TestSSHHostKeyAlgorithms(t *testing.T) {
	assert.Nil(t, hostKeyAlgorithms(nil))

	algorithms := hostKeyAlgorithms([]ssh.PublicKey{newTestHostKey(t).PublicKey()})
	assert.Equal(t, ssh.KeyAlgoED25519, algorithms[0])
	assert.Subset(t, algorithms, ssh.SupportedAlgorithms().HostKeys)
	assert.Subset(t, algorithms, ssh.InsecureAlgorithms().HostKeys)
	assert.Len(t, algorithms, len(slices.Compact(slices.Sorted(slices.Values(algorithms)))))
}
