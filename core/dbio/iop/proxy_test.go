package iop

import (
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/stretchr/testify/assert"
)

func minimalSOCKS5(t *testing.T, listener net.Listener) {
	t.Helper()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go handleSOCKS5Conn(t, conn)
	}
}

func handleSOCKS5Conn(t *testing.T, conn net.Conn) {
	t.Helper()
	defer conn.Close()

	buf := make([]byte, 258)
	n, err := conn.Read(buf)
	if err != nil || n < 3 || buf[0] != 0x05 {
		return
	}
	conn.Write([]byte{0x05, 0x00})

	n, err = conn.Read(buf)
	if err != nil || n < 10 || buf[1] != 0x01 {
		return
	}

	var targetAddr string
	switch buf[3] {
	case 0x01:
		targetAddr = fmt.Sprintf("%d.%d.%d.%d:%d",
			buf[4], buf[5], buf[6], buf[7],
			int(buf[8])<<8|int(buf[9]))
	case 0x03:
		domainLen := int(buf[4])
		domain := string(buf[5 : 5+domainLen])
		portOff := 5 + domainLen
		targetAddr = fmt.Sprintf("%s:%d", domain, int(buf[portOff])<<8|int(buf[portOff+1]))
	default:
		return
	}

	remote, err := net.DialTimeout("tcp", targetAddr, 5*time.Second)
	if err != nil {
		conn.Write([]byte{0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
		return
	}
	defer remote.Close()

	conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})

	done := make(chan struct{}, 2)
	go func() { io.Copy(remote, conn); done <- struct{}{} }()
	go func() { io.Copy(conn, remote); done <- struct{}{} }()
	<-done
}

func startEchoServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	g.AssertNoError(t, err)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				io.Copy(conn, conn)
			}()
		}
	}()
	return ln
}

func TestOpenTunnelProxy_NoProxyConfigured(t *testing.T) {
	os.Unsetenv("ALL_PROXY")

	_, err := OpenTunnelProxy("example.com", 5432)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no proxy configured")
}

func TestOpenTunnelProxy_UnreachableProxy(t *testing.T) {
	// Point ALL_PROXY at a port with nothing listening
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	g.AssertNoError(t, err)
	deadPort := ln.Addr().(*net.TCPAddr).Port
	ln.Close() // close immediately so the port is unreachable

	t.Setenv("ALL_PROXY", fmt.Sprintf("socks5://127.0.0.1:%d", deadPort))

	localPort, err := OpenTunnelProxy("127.0.0.1", 5432)
	// tunnel setup succeeds (listener created), but connecting through it should fail
	g.AssertNoError(t, err)

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", localPort), 5*time.Second)
	g.AssertNoError(t, err)
	defer conn.Close()

	// write some data to trigger the proxy dial
	conn.Write([]byte("hello"))

	// read should fail because the proxy is unreachable
	buf := make([]byte, 16)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, err = conn.Read(buf)
	assert.Error(t, err, "expected read error when proxy is unreachable")
}

func TestOpenTunnelProxy_ForwardsTraffic(t *testing.T) {
	echoLn := startEchoServer(t)
	defer echoLn.Close()
	echoAddr := echoLn.Addr().(*net.TCPAddr)

	socksLn, err := net.Listen("tcp", "127.0.0.1:0")
	g.AssertNoError(t, err)
	defer socksLn.Close()
	go minimalSOCKS5(t, socksLn)

	t.Setenv("ALL_PROXY", fmt.Sprintf("socks5://127.0.0.1:%d", socksLn.Addr().(*net.TCPAddr).Port))

	localPort, err := OpenTunnelProxy("127.0.0.1", echoAddr.Port)
	g.AssertNoError(t, err)
	assert.Greater(t, localPort, 0)

	// single connection round-trip
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", localPort), 5*time.Second)
	g.AssertNoError(t, err)
	defer conn.Close()

	msg := []byte("hello through socks5 proxy")
	_, err = conn.Write(msg)
	g.AssertNoError(t, err)

	buf := make([]byte, len(msg))
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = io.ReadFull(conn, buf)
	g.AssertNoError(t, err)
	assert.Equal(t, msg, buf)

	// multiple connections through the same tunnel
	for i := 0; i < 3; i++ {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", localPort), 5*time.Second)
		g.AssertNoError(t, err)

		payload := []byte(fmt.Sprintf("connection %d", i))
		_, err = c.Write(payload)
		g.AssertNoError(t, err)

		rbuf := make([]byte, len(payload))
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = io.ReadFull(c, rbuf)
		g.AssertNoError(t, err)
		assert.Equal(t, payload, rbuf)
		c.Close()
	}
}
