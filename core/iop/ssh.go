package iop

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"

	h "github.com/flarco/gutil"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
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

// Connect connects to the server
func (s *SSHClient) Connect() (err error) {

	authMethods := []ssh.AuthMethod{}
	// Create the Signer for this private key.
	if s.PrivateKey != "" {
		_, err := os.Stat(s.PrivateKey)
		if err == nil {
			prvKeyBytes, err := ioutil.ReadFile(s.PrivateKey)
			if err != nil {
				return h.Error(err, "Could not read private key: "+s.PrivateKey)
			}
			s.PrivateKey = string(prvKeyBytes)
		}
		signer, err := ssh.ParsePrivateKey([]byte(s.PrivateKey))
		if err != nil {
			return h.Error(err, "unable to parse private key")
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	} else if s.Password != "" {
		authMethods = append(authMethods, ssh.Password(s.Password))
	} else if s.Password == "" {
		return h.Error(fmt.Errorf("need to provide password, public key or private key"), "need to provide password, public key or private key")
	}

	homeDir := h.UserHomeDir()
	hostKeyCallback, err := knownhosts.New(homeDir + "/.ssh/known_hosts")
	if err != nil {
		h.Info("could not create hostkeycallback function, using InsecureIgnoreHostKey")
		hostKeyCallback = ssh.InsecureIgnoreHostKey()
	}
	hostKeyCallback = ssh.InsecureIgnoreHostKey()

	s.config = &ssh.ClientConfig{
		User:            s.User,
		Auth:            authMethods,
		HostKeyCallback: hostKeyCallback,
	}

	// Connect to the remote server and perform the SSH handshake.
	sshAddr := h.F("%s:%d", s.Host, s.Port)
	s.client, err = ssh.Dial("tcp", sshAddr, s.config)
	if err != nil {
		return h.Error(err, "unable to connect to ssh server "+sshAddr)
	}
	return nil
}

// OpenPortForward forwards the port as specified
func (s *SSHClient) OpenPortForward() (localPort int, err error) {

	err = s.Connect()
	if err != nil {
		return 0, h.Error(err, "unable to connect to ssh server ")
	}

	localPort, err = h.GetPort("localhost:0")
	if err != nil {
		err = h.Error(err, "could not acquire local port")
		return
	}

	// Setup localListener (type net.Listener)
	localAddr := h.F("127.0.0.1:%d", localPort)
	s.localListener, err = net.Listen("tcp", localAddr)
	if err != nil {
		return 0, h.Error(err, "unable to open local port "+localAddr)
	}

	go func() {
		for {
			// Setup localConn (type net.Conn)
			localConn, err := s.localListener.Accept()
			if err != nil && strings.Contains(err.Error(), "use of closed network") {
				return
			} else if err != nil {
				s.Err = h.Error(err, "error accepting local connection")
				h.LogError(s.Err)
				s.Close()
				return
			}
			go s.forward(localConn)
		}
	}()

	h.Debug(
		"SSH tunnel established -> 127.0.0.1:%d to %s:%d ",
		localPort, s.TgtHost, s.TgtPort,
	)

	return
}

func (s *SSHClient) forward(localConn net.Conn) error {
	// Setup sshConn (type net.Conn)
	remoteAddr := h.F("%s:%d", s.TgtHost, s.TgtPort)
	remoteConn, err := s.client.Dial("tcp", remoteAddr)
	if err != nil {
		return h.Error(err, "unable to connect to remote server "+remoteAddr)
	}

	// Copy localConn.Reader to sshConn.Writer
	go func() {
		_, err = io.Copy(remoteConn, localConn)
		if err != nil && strings.Contains(err.Error(), "use of closed network") {
			return
		} else if err == io.EOF {
			return
		} else if err != nil {
			h.LogError(err, "failed io.Copy(sshConn, localConn)")
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
			h.LogError(err, "failed io.Copy(localConn, sshConn)")
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
		return 0, h.Error(fmt.Errorf("already running"))
	}
	localPort, err = h.GetPort("localhost:0")
	if err != nil {
		err = h.Error(err, "could not acquire local port")
		return
	}

	_, err = exec.LookPath("ssh")
	if err != nil {
		err = h.Error(err, "ssh not found")
		return
	}
	_, err = exec.LookPath("sshpass")
	if err != nil {
		err = h.Error(err, "sshpass not found")
		return
	}

	// ssh -P -N -L 5000:localhost:5432 user@myapp.com
	s.cmd = exec.Command(
		"sshpass",
		"-p",
		s.Password,
		"ssh",
		h.F("-p%d", s.Port),
		"-o StrictHostKeyChecking=no",
		"-o UserKnownHostsFile=/dev/null",
		"-4",
		"-N",
		h.F("-L %d:%s:%d", localPort, s.TgtHost, s.TgtPort),
		h.F("%s@%s", s.User, s.Host),
	)
	s.cmd.Stderr = &s.stderr
	s.cmd.Stdout = &s.stdout

	go func() {
		cmdStr := strings.ReplaceAll(
			strings.Join(s.cmd.Args, " "),
			"-p "+s.Password, "-p ***",
		)
		h.Trace("SSH Command: %s", cmdStr)
		s.Err = s.cmd.Run()
		// h.LogError(s.Err)
	}()

	// wait until it connects
	st := time.Now()
	for {
		time.Sleep(200 * time.Millisecond)
		_, stderr := s.GetOutput()
		// h.Debug(stderr)

		tcpAddr := h.F("127.0.0.1:%d", localPort)
		conn, err := net.DialTimeout("tcp", tcpAddr, 1*time.Second)
		if err == nil {
			time.Sleep(500 * time.Millisecond)
			_, stderr := s.GetOutput()
			conn.Close()
			if strings.Contains(stderr, "open failed") {
				// https://unix.stackexchange.com/questions/14160/ssh-tunneling-error-channel-1-open-failed-administratively-prohibited-open
				s.Close()
				err = fmt.Errorf(stderr)
				return 0, err
			}
			break
		}

		if s.Err != nil {
			err = h.Error(s.Err, stderr)
			return 0, err
		}

		if time.Since(st).Seconds() > 10 {
			// timeout
			err = fmt.Errorf("ssh connect attempt timed out")
			if stderr != "" {
				err = fmt.Errorf(stderr)
			}
			s.Close()
			return 0, err
		}
	}

	h.Debug(
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
		s.cmd.Process.Kill()
		s.cmd = nil
	} else {
		for _, conn := range s.allConns {
			conn.Close()
		}
		if s.localListener != nil {
			s.localListener.Close()
		}
		if s.client != nil {
			s.client.Close()
		}
	}
}
