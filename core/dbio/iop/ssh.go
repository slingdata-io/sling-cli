package iop

import (
	"bytes"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/pkg/sftp"
	"github.com/spf13/cast"
	"golang.org/x/crypto/ssh"
)

// SSHClient is a client to connect to a ssh server
// with the main goal of forwarding ports
type SSHClient struct {
	Host          string
	Port          int
	User          string
	Password      string
	TgtHost       string
	TgtPort       int
	PrivateKey    string
	Passphrase    string
	Err           error
	allConns      []net.Conn
	localListener net.Listener
	config        *ssh.ClientConfig
	client        *ssh.Client
	cmd           *exec.Cmd
	stdout        bytes.Buffer
	stderr        bytes.Buffer
}

// SftpClient returns an SftpClient
func (s *SSHClient) SftpClient() (sftpClient *sftp.Client, err error) {
	return sftp.NewClient(s.client)
}

// NewSession creates a new SSH session
func (s *SSHClient) NewSession() (*ssh.Session, error) {
	if s.client == nil {
		return nil, g.Error("SSH client not connected")
	}
	return s.client.NewSession()
}

// Connect connects to the server
func (s *SSHClient) Connect() (err error) {

	authMethods := []ssh.AuthMethod{}
	// Create the Signer for this private key.
	if s.PrivateKey != "" {
		_, err := os.Stat(s.PrivateKey)
		if err == nil {
			prvKeyBytes, err := os.ReadFile(s.PrivateKey)
			if err != nil {
				return g.Error(err, "Could not read private key: "+s.PrivateKey)
			}
			s.PrivateKey = string(prvKeyBytes)
		}

		var signer ssh.Signer
		if s.Passphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase([]byte(s.PrivateKey), []byte(s.Passphrase))
		} else {
			signer, err = ssh.ParsePrivateKey([]byte(s.PrivateKey))
		}
		if err != nil {
			return g.Error(err, "unable to parse private key")
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	if s.Password != "" {
		authMethods = append(authMethods, ssh.Password(s.Password))
	}

	if len(authMethods) == 0 {
		return g.Error("need to provide password, public key or private key")
	}

	// homeDir := g.UserHomeDir()
	// hostKeyCallback, err := knownhosts.New(path.Join(homeDir, ".ssh", "known_hosts"))
	// if err != nil {
	// 	g.Debug("could not create hostkeycallback function, using InsecureIgnoreHostKey")
	// 	hostKeyCallback = ssh.InsecureIgnoreHostKey()
	// }
	hostKeyCallback := ssh.InsecureIgnoreHostKey()

	var config ssh.Config
	config.SetDefaults()

	// from ssh common.go (supportedCiphers)
	// allow all supported cyphers
	config.Ciphers = append(
		config.Ciphers,
		`arcfour256`,
		`arcfour128`,
		`arcfour`,
		`aes128-cbc`,
		`3des-cbc`,
		`diffie-hellman-group-exchange-sha256`,
	)

	config.KeyExchanges = append(
		config.KeyExchanges,
		`rsa-sha2-256-cert-v01@openssh.com`,
		`rsa-sha2-512-cert-v01@openssh.com`,
		`ssh-rsa-cert-v01@openssh.com`,
		`ssh-dss-cert-v01@openssh.com`,
		`ecdsa-sha2-nistp256-cert-v01@openssh.com`,
		`ecdsa-sha2-nistp384-cert-v01@openssh.com`, `ecdsa-sha2-nistp521-cert-v01@openssh.com`, `ssh-ed25519-cert-v01@openssh.com`,
		`diffie-hellman-group-exchange-sha256`,
		`diffie-hellman-group-exchange-sha1`,
		`ecdsa-sha2-nistp256`,
		`ecdsa-sha2-nistp384`,
		`ecdsa-sha2-nistp521`,
		`rsa-sha2-256`,
		`rsa-sha2-512`,
		`ssh-rsa`,
		`ssh-dss`,
		`ssh-ed25519`,
	)

	s.config = &ssh.ClientConfig{
		User:            s.User,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
		Config:          config,
	}

	// Connect to the remote server and perform the SSH handshake.
	sshAddr := g.F("%s:%d", s.Host, s.Port)
	s.client, err = ssh.Dial("tcp", sshAddr, s.config)
	if err != nil {
		return g.Error(err, "unable to connect to ssh server "+sshAddr)
	}
	return nil
}

// OpenPortForward forwards the port as specified
func (s *SSHClient) OpenPortForward() (localPort int, err error) {

	err = s.Connect()
	if err != nil {
		return 0, g.Error(err, "unable to connect to ssh server ")
	}

	localPort, err = g.GetPort("localhost:0")
	if err != nil {
		err = g.Error(err, "could not acquire local port")
		return
	}

	// Setup localListener (type net.Listener)
	localAddr := g.F("127.0.0.1:%d", localPort)
	s.localListener, err = net.Listen("tcp", localAddr)
	if err != nil {
		return 0, g.Error(err, "unable to open local port "+localAddr)
	}

	go func() {
		for {
			// Setup localConn (type net.Conn)
			localConn, err := s.localListener.Accept()
			if err != nil && strings.Contains(err.Error(), "use of closed network") {
				return
			} else if err != nil {
				s.Err = g.Error(err, "error accepting local connection")
				g.LogError(s.Err)
				s.Close()
				return
			}
			go s.forward(localConn)
		}
	}()

	g.Debug(
		"SSH tunnel established -> 127.0.0.1:%d to %s:%d ",
		localPort, s.TgtHost, s.TgtPort,
	)

	return
}

func (s *SSHClient) forward(localConn net.Conn) error {
	// Setup sshConn (type net.Conn)
	remoteAddr := g.F("%s:%d", s.TgtHost, s.TgtPort)
	remoteConn, err := s.client.Dial("tcp", remoteAddr)
	if err != nil {
		return g.Error(err, "unable to connect to remote server "+remoteAddr)
	}

	// Copy localConn.Reader to sshConn.Writer
	go func() {
		_, err = io.Copy(remoteConn, localConn)
		if err != nil && strings.Contains(err.Error(), "use of closed network") {
			return
		} else if err == io.EOF {
			return
		} else if err != nil {
			g.LogError(err, "failed io.Copy(sshConn, localConn)")
			return
		}
	}()

	// Copy sshConn.Reader to localConn.Writer
	go func() {
		_, err = io.Copy(localConn, remoteConn)
		if err != nil && strings.Contains(err.Error(), "use of closed network") {
			return
		} else if err == io.EOF {
			return
		} else if err != nil {
			g.LogError(err, "failed io.Copy(localConn, sshConn)")
			return
		}
	}()

	s.allConns = append(s.allConns, remoteConn)
	s.allConns = append(s.allConns, localConn)

	return nil
}

// RunAsProcess uses a separate process
// enables to use public key auth
// https://git-scm.com/book/pt-pt/v2/Git-no-Servidor-Generating-Your-SSH-Public-Key
func (s *SSHClient) RunAsProcess() (localPort int, err error) {
	if s.cmd != nil {
		return 0, g.Error("already running")
	}
	localPort, err = g.GetPort("localhost:0")
	if err != nil {
		err = g.Error(err, "could not acquire local port")
		return
	}

	_, err = exec.LookPath("ssh")
	if err != nil {
		err = g.Error(err, "ssh not found")
		return
	}
	_, err = exec.LookPath("sshpass")
	if err != nil {
		err = g.Error(err, "sshpass not found")
		return
	}

	// ssh -P -N -L 5000:localhost:5432 user@myapp.com
	s.cmd = exec.Command(
		"sshpass",
		"-p",
		s.Password,
		"ssh",
		g.F("-p%d", s.Port),
		"-o StrictHostKeyChecking=no",
		"-o UserKnownHostsFile=/dev/null",
		"-4",
		"-N",
		g.F("-L %d:%s:%d", localPort, s.TgtHost, s.TgtPort),
		g.F("%s@%s", s.User, s.Host),
	)
	s.cmd.Stderr = &s.stderr
	s.cmd.Stdout = &s.stdout

	go func() {
		cmdStr := strings.ReplaceAll(
			strings.Join(s.cmd.Args, " "),
			"-p "+s.Password, "-p ***",
		)
		g.Trace("SSH Command: %s", cmdStr)
		s.Err = s.cmd.Run()
	}()

	// wait until it connects
	st := time.Now()
	for {
		time.Sleep(200 * time.Millisecond)
		_, stderr := s.GetOutput()
		// g.Debug(stderr)

		tcpAddr := g.F("127.0.0.1:%d", localPort)
		conn, err := net.DialTimeout("tcp", tcpAddr, 1*time.Second)
		if err == nil {
			time.Sleep(500 * time.Millisecond)
			_, stderr := s.GetOutput()
			conn.Close()
			if strings.Contains(stderr, "open failed") {
				// https://unix.stackexchange.com/questions/14160/ssh-tunneling-error-channel-1-open-failed-administratively-prohibited-open
				s.Close()
				err = g.Error(stderr)
				return 0, err
			}
			break
		}

		if s.Err != nil {
			err = g.Error(s.Err, stderr)
			return 0, err
		}

		if time.Since(st).Seconds() > 10 {
			// timeout
			err = g.Error("ssh connect attempt timed out")
			if stderr != "" {
				err = g.Error(stderr)
			}
			s.Close()
			return 0, err
		}
	}

	g.Debug(
		"SSH tunnel established -> 127.0.0.1:%d to %s:%d ",
		localPort, s.TgtHost, s.TgtPort,
	)

	return
}

// GetOutput return stdout & stderr outputs
func (s *SSHClient) GetOutput() (stdout string, stderr string) {
	bufToString := func(buf *bytes.Buffer) string {
		strArr := []string{}
		for {
			line, err := buf.ReadString('\n')
			if err == io.EOF {
				break
			} else if err != nil {
				break
			}
			line = strings.TrimSpace(line)
			strArr = append(strArr, line)
		}
		return strings.Join(strArr, "\n")
	}

	return bufToString(&s.stdout), bufToString(&s.stderr)
}

// Close stops the client connection
func (s *SSHClient) Close() {
	if s.cmd != nil {
		err := s.cmd.Process.Kill()
		g.LogError(err)
		s.cmd = nil
	} else {
		for _, conn := range s.allConns {
			err := conn.Close()
			g.LogError(err)
		}
		if s.localListener != nil {
			err := s.localListener.Close()
			g.LogError(err)
		}
		if s.client != nil {
			err := s.client.Close()
			g.LogError(err)
		}
	}
}

func OpenTunnelSSH(tgtHost string, tgtPort int, tunnelURL, privateKey, passphrase string) (localPort int, err error) {

	sshU, err := url.Parse(tunnelURL)
	if err != nil {
		return 0, g.Error(err, "could not parse SSH_TUNNEL URL")
	}

	sshHost := sshU.Hostname()
	sshPort := cast.ToInt(sshU.Port())
	if sshPort == 0 {
		sshPort = 22
	}
	sshUser := sshU.User.Username()
	sshPassword, _ := sshU.User.Password()

	sshClient := &SSHClient{
		Host:       sshHost,
		Port:       sshPort,
		User:       sshUser,
		Password:   sshPassword,
		TgtHost:    tgtHost,
		TgtPort:    tgtPort,
		PrivateKey: privateKey,
		Passphrase: passphrase,
	}

	localPort, err = sshClient.OpenPortForward()
	if err != nil {
		return 0, g.Error(err, "could not connect to ssh server")
	}

	return
}
