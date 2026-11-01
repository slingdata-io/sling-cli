package filesys

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/flarco/g"
	"github.com/pkg/sftp"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

// startTestSSHServer serves SFTP and direct-tcpip (for ssh_tunnel) with user "u" and password "p"
func startTestSSHServer(t *testing.T) (port int, hostKey ssh.PublicKey) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(priv)
	require.NoError(t, err)

	config := &ssh.ServerConfig{
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if c.User() == "u" && string(pass) == "p" {
				return nil, nil
			}
			return nil, g.Error("bad password")
		},
	}
	config.AddHostKey(signer)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })

	serveChannel := func(newCh ssh.NewChannel) {
		switch newCh.ChannelType() {
		case "session":
			ch, reqs, err := newCh.Accept()
			if err != nil {
				return
			}
			for req := range reqs {
				if req.Type == "subsystem" && string(req.Payload[4:]) == "sftp" {
					req.Reply(true, nil)
					server, err := sftp.NewServer(ch)
					if err == nil {
						server.Serve()
					}
					ch.Close()
					return
				}
				req.Reply(false, nil)
			}
		case "direct-tcpip":
			var target struct {
				Host       string
				Port       uint32
				OriginHost string
				OriginPort uint32
			}
			if err := ssh.Unmarshal(newCh.ExtraData(), &target); err != nil {
				newCh.Reject(ssh.ConnectionFailed, err.Error())
				return
			}
			remote, err := net.Dial("tcp", net.JoinHostPort(target.Host, g.F("%d", target.Port)))
			if err != nil {
				newCh.Reject(ssh.ConnectionFailed, err.Error())
				return
			}
			ch, reqs, err := newCh.Accept()
			if err != nil {
				remote.Close()
				return
			}
			go ssh.DiscardRequests(reqs)
			pipe(ch, remote)
		default:
			newCh.Reject(ssh.UnknownChannelType, "not supported")
		}
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_, chans, reqs, err := ssh.NewServerConn(conn, config)
				if err != nil {
					conn.Close()
					return
				}
				go ssh.DiscardRequests(reqs)
				for newCh := range chans {
					go serveChannel(newCh)
				}
			}()
		}
	}()

	return ln.Addr().(*net.TCPAddr).Port, signer.PublicKey()
}

// startTestSOCKS5 serves SOCKS5 CONNECT without auth. It counts the connections.
func startTestSOCKS5(t *testing.T) (port int, hits *atomic.Int32) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	hits = &atomic.Int32{}

	handle := func(conn net.Conn) {
		// greeting: VER NMETHODS METHODS...
		buf := make([]byte, 262)
		if _, err := io.ReadFull(conn, buf[:2]); err != nil || buf[0] != 0x05 {
			conn.Close()
			return
		}
		if _, err := io.ReadFull(conn, buf[:buf[1]]); err != nil {
			conn.Close()
			return
		}
		conn.Write([]byte{0x05, 0x00})

		// request: VER CMD RSV ATYP ADDR PORT
		if _, err := io.ReadFull(conn, buf[:4]); err != nil || buf[1] != 0x01 {
			conn.Close()
			return
		}
		var host string
		switch buf[3] {
		case 0x01:
			io.ReadFull(conn, buf[:4])
			host = net.IP(buf[:4]).String()
		case 0x03:
			io.ReadFull(conn, buf[:1])
			n := int(buf[0])
			io.ReadFull(conn, buf[:n])
			host = string(buf[:n])
		default:
			conn.Close()
			return
		}
		io.ReadFull(conn, buf[:2])
		addr := net.JoinHostPort(host, g.F("%d", binary.BigEndian.Uint16(buf[:2])))

		remote, err := net.Dial("tcp", addr)
		if err != nil {
			conn.Write([]byte{0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
			conn.Close()
			return
		}
		hits.Add(1)
		conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		pipe(conn, remote)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handle(conn)
		}
	}()

	return ln.Addr().(*net.TCPAddr).Port, hits
}

func pipe(a, b io.ReadWriteCloser) {
	go func() { io.Copy(a, b); a.Close(); b.Close() }()
	go func() { io.Copy(b, a); a.Close(); b.Close() }()
}

func TestSftpProxy(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home) // no default ~/.ssh/id_rsa or known_hosts
	t.Setenv("USERPROFILE", home)

	sshPort, hostKey := startTestSSHServer(t)
	sshAddr := g.F("127.0.0.1:%d", sshPort)

	// strict mode passes only if the host key is checked for the SFTP server address, not the proxy address
	knownHostsFile := filepath.Join(home, "known_hosts")
	line := knownhosts.Line([]string{knownhosts.Normalize(sshAddr)}, hostKey) + "\n"
	require.NoError(t, os.WriteFile(knownHostsFile, []byte(line), 0o600))

	props := func(extra ...string) []string {
		return append([]string{
			"HOST=127.0.0.1",
			g.F("PORT=%d", sshPort),
			"USER=u",
			"PASSWORD=p",
			"SSH_KNOWN_HOSTS=" + knownHostsFile,
			"SSH_STRICT_HOST_KEY=true",
		}, extra...)
	}

	roundTrip := func(t *testing.T, fs FileSysClient) {
		uri := g.F("sftp://%s%s/test1.txt", sshAddr, filepath.ToSlash(t.TempDir()))
		_, err := fs.Write(uri, strings.NewReader("abcde"))
		require.NoError(t, err)

		reader, err := fs.GetReader(uri)
		require.NoError(t, err)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "abcde", string(data))

		nodes, err := fs.List(uri)
		require.NoError(t, err)
		assert.Contains(t, nodes.URIs(), uri)

		require.NoError(t, Delete(fs, uri))
	}

	t.Run("direct", func(t *testing.T) {
		proxyPort, hits := startTestSOCKS5(t)
		fs, err := NewFileSysClient(dbio.TypeFileSftp, props(g.F("PROXY_URL=socks5://127.0.0.1:%d", proxyPort))...)
		require.NoError(t, err)
		defer fs.Close()

		roundTrip(t, fs)
		assert.EqualValues(t, 1, hits.Load())
	})

	t.Run("ssh_tunnel", func(t *testing.T) {
		// the proxy reaches the tunnel server only. The SFTP connection goes through the tunnel.
		proxyPort, hits := startTestSOCKS5(t)
		fs, err := NewFileSysClient(dbio.TypeFileSftp, props(
			g.F("PROXY_URL=socks5h://127.0.0.1:%d", proxyPort),
			g.F("SSH_TUNNEL=ssh://u:p@%s", sshAddr),
		)...)
		require.NoError(t, err)
		defer fs.Close()

		roundTrip(t, fs)
		assert.EqualValues(t, 1, hits.Load())
	})

	t.Run("unreachable proxy", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		deadPort := ln.Addr().(*net.TCPAddr).Port
		ln.Close()

		_, err = NewFileSysClient(dbio.TypeFileSftp, props(g.F("PROXY_URL=socks5://127.0.0.1:%d", deadPort))...)
		require.Error(t, err)
		assert.Contains(t, err.Error(), g.F("via proxy 127.0.0.1:%d", deadPort))
	})

	t.Run("bad proxy url", func(t *testing.T) {
		_, err := NewFileSysClient(dbio.TypeFileSftp, props("PROXY_URL=socks5://user:secret@host:badport")...)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "could not parse proxy URL")
		assert.NotContains(t, err.Error(), "secret")

		_, err = NewFileSysClient(dbio.TypeFileSftp, props("PROXY_URL=http://127.0.0.1:8080")...)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown scheme")
	})
}
