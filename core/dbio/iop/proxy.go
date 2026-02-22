package iop

import (
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/flarco/g"
	"golang.org/x/net/proxy"
)

// OpenTunnelProxy forwards traffic through the SOCKS5 proxy in ALL_PROXY
func OpenTunnelProxy(tgtHost string, tgtPort int) (localPort int, err error) {
	proxyURL := os.Getenv("ALL_PROXY")
	if proxyURL == "" {
		return 0, g.Error("no proxy configured in environment (ALL_PROXY)")
	}

	u, err := url.Parse(proxyURL)
	if err != nil {
		return 0, g.Error(err, "could not parse ALL_PROXY URL")
	}

	dialer, err := proxy.FromURL(u, proxy.Direct)
	if err != nil {
		return 0, g.Error(err, "could not create dialer from ALL_PROXY URL")
	}

	localPort, err = g.GetPort("localhost:0")
	if err != nil {
		return 0, g.Error(err, "could not acquire local port for proxy tunnel")
	}

	localAddr := g.F("127.0.0.1:%d", localPort)
	listener, err := net.Listen("tcp", localAddr)
	if err != nil {
		return 0, g.Error(err, "unable to open local port "+localAddr)
	}

	remoteAddr := g.F("%s:%d", tgtHost, tgtPort)

	go func() {
		for {
			localConn, err := listener.Accept()
			if err != nil && strings.Contains(err.Error(), "use of closed network") {
				return
			} else if err != nil {
				g.LogError(g.Error(err, "error accepting proxy tunnel connection"))
				listener.Close()
				return
			}
			go forwardProxy(localConn, dialer, remoteAddr)
		}
	}()

	g.Debug("SOCKS5 proxy tunnel established -> 127.0.0.1:%d to %s", localPort, remoteAddr)

	return localPort, nil
}

func forwardProxy(localConn net.Conn, dialer proxy.Dialer, remoteAddr string) {
	remoteConn, err := dialer.Dial("tcp", remoteAddr)
	if err != nil {
		g.LogError(g.Error(err, "unable to connect to remote server via proxy "+remoteAddr))
		localConn.Close()
		return
	}

	var closeOnce sync.Once
	closeAll := func() {
		localConn.Close()
		remoteConn.Close()
	}

	// Copy localConn.Reader to remoteConn.Writer
	go func() {
		io.Copy(remoteConn, localConn)
		closeOnce.Do(closeAll)
	}()

	// Copy remoteConn.Reader to localConn.Writer
	go func() {
		io.Copy(localConn, remoteConn)
		closeOnce.Do(closeAll)
	}()
}
