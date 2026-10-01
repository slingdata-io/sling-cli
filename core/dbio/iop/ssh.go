package iop

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/flarco/g"
	"github.com/pkg/sftp"
	"github.com/spf13/cast"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

const sshHostKeyDocsURL = "https://docs.slingdata.io/concepts/ssh-host-keys"

// sshHostKeyWarned keeps the host keys that already got a warning in this process
var sshHostKeyWarned sync.Map

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
	Options       SSHOptions
	HostKeyAddr   string // verify the host key for this address instead of Host:Port (e.g. through a tunnel)
	Err           error
	allConns      []net.Conn
	localListener net.Listener
	config        *ssh.ClientConfig
	client        *ssh.Client
}

// SSHOptions are the ssh_* settings of a connection. They apply to the
// SSH tunnel server and to the SFTP server.
type SSHOptions struct {
	HostKey      SSHHostKey
	Ciphers      []string // nil: the default list
	KeyExchanges []string // nil: the default list
}

// NewSSHOptions creates SSHOptions from the ssh_* connection properties
func NewSSHOptions(getProp func(key string) string) SSHOptions {
	return SSHOptions{
		HostKey:      NewSSHHostKey(getProp("ssh_host_key"), getProp("ssh_known_hosts"), getProp("ssh_strict_host_key")),
		Ciphers:      splitSSHList(getProp("ssh_ciphers")),
		KeyExchanges: splitSSHList(getProp("ssh_kex_algorithms")),
	}
}

func splitSSHList(value string) (list []string) {
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			list = append(list, item)
		}
	}
	return list
}

// Config returns the algorithms to offer. The default list puts legacy
// algorithms after the secure ones, so that old servers can connect.
func (o SSHOptions) Config() (config ssh.Config, err error) {
	config.SetDefaults()
	config.Ciphers = append(config.Ciphers, ssh.InsecureAlgorithms().Ciphers...)
	config.KeyExchanges = append(config.KeyExchanges, ssh.KeyExchangeDHGEXSHA256, ssh.InsecureKeyExchangeDHGEXSHA1)

	supported, insecure := ssh.SupportedAlgorithms(), ssh.InsecureAlgorithms()
	if o.Ciphers != nil {
		if config.Ciphers, err = validateSSHList("ssh_ciphers", o.Ciphers, supported.Ciphers, insecure.Ciphers); err != nil {
			return config, err
		}
	}
	if o.KeyExchanges != nil {
		valid := append(supported.KeyExchanges, "curve25519-sha256@libssh.org")
		if config.KeyExchanges, err = validateSSHList("ssh_kex_algorithms", o.KeyExchanges, valid, insecure.KeyExchanges); err != nil {
			return config, err
		}
	}
	return config, nil
}

// validateSSHList returns an error for a name that Go does not implement,
// because the ssh package ignores unknown names without an error
func validateSSHList(prop string, list []string, valid ...[]string) ([]string, error) {
	all := slices.Concat(valid...)
	for _, name := range list {
		if !slices.Contains(all, name) {
			return nil, g.Error("invalid %s value %#v. Valid values are: %s", prop, name, strings.Join(all, ", "))
		}
	}
	return list, nil
}

// SSHHostKey verifies the host key of an SSH server
type SSHHostKey struct {
	Keys       []string // accepted fingerprints (SHA256:...) or public keys
	KnownHosts string   // known_hosts file to read, in addition to ~/.ssh/known_hosts
	Strict     *bool    // nil: warn when the key is not verified
}

// NewSSHHostKey creates an SSHHostKey from the values of the
// ssh_host_key, ssh_known_hosts and ssh_strict_host_key properties
func NewSSHHostKey(keys, knownHosts, strict string) SSHHostKey {
	hk := SSHHostKey{Keys: splitSSHList(keys), KnownHosts: knownHosts}
	if strict != "" {
		hk.Strict = g.Bool(cast.ToBool(strict))
	}
	return hk
}

// Callback returns the host key callback for the server at addr. It also returns
// the host key algorithms to request, so that the server sends a known key type.
// A key in ssh_host_key or in a known_hosts file is trusted.
func (hk SSHHostKey) Callback(addr string) (callback ssh.HostKeyCallback, algorithms []string, err error) {
	known := []ssh.PublicKey{}
	for _, want := range hk.Keys {
		if pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(want)); err == nil {
			known = append(known, pub)
		} else if !strings.HasPrefix(want, "SHA256:") {
			return nil, nil, g.Error("invalid ssh_host_key value %#v. Use a SHA256 fingerprint or a public key. See %s", want, sshHostKeyDocsURL)
		}
	}

	if len(hk.Keys) == 0 && hk.Strict != nil && !*hk.Strict {
		return ssh.InsecureIgnoreHostKey(), nil, nil
	}

	knownHosts, err := hk.loadKnownHosts()
	if err != nil {
		return nil, nil, err
	}

	callback = func(_ string, remote net.Addr, key ssh.PublicKey) error {
		if hk.hasKey(key) {
			return nil
		}

		problem := "is not verified"
		if knownHosts != nil {
			var keyErr *knownhosts.KeyError
			err := knownHosts(addr, remote, key)
			switch {
			case err == nil:
				return nil
			case !errors.As(err, &keyErr):
				return g.Error(err, "SSH host key of %s is not valid", addr)
			case slices.ContainsFunc(keyErr.Want, func(k knownhosts.KnownKey) bool { return k.Key.Type() == key.Type() }):
				problem = "does not match known_hosts"
			default:
				problem = "is not known"
			}
		}
		if len(hk.Keys) > 0 {
			problem = "is not in ssh_host_key or known_hosts"
		}
		return hk.unverified(addr, problem, key)
	}

	known = append(known, knownHostKeys(knownHosts, addr)...)
	return callback, hostKeyAlgorithms(known), nil
}

// hasKey returns true if ssh_host_key has the fingerprint or the public key
func (hk SSHHostKey) hasKey(key ssh.PublicKey) bool {
	fingerprint := ssh.FingerprintSHA256(key)
	for _, want := range hk.Keys {
		if want == fingerprint {
			return true
		}
		if pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(want)); err == nil && bytes.Equal(pub.Marshal(), key.Marshal()) {
			return true
		}
	}
	return false
}

// unverified returns an error in strict mode. Otherwise it logs a warning once.
// A value in ssh_host_key turns on strict mode.
func (hk SSHHostKey) unverified(addr, problem string, key ssh.PublicKey) error {
	fingerprint := ssh.FingerprintSHA256(key)
	msg := g.F("SSH host key of %s %s (%s %s).", addr, problem, key.Type(), fingerprint)
	lines := []string{
		"  Make sure that this is the key of the server. Then set one of these connection properties:",
		g.F("    ssh_host_key: %s   # trust this host (separate several keys with commas)", fingerprint),
		"    ssh_strict_host_key: false   # skip verification",
		"  See " + sshHostKeyDocsURL,
	}
	if len(hk.Keys) > 0 {
		lines = slices.Delete(lines, 2, 3) // ssh_host_key has priority over ssh_strict_host_key
		lines[0] = "  Make sure that this is the key of the server. Then add it to the connection property:"
	}
	advice := strings.Join(lines, "\n")

	if len(hk.Keys) > 0 || (hk.Strict != nil && *hk.Strict) {
		return g.Error("%s\n%s", msg, advice)
	}

	if _, warned := sshHostKeyWarned.LoadOrStore(addr+" "+fingerprint, true); !warned {
		g.Warn("%s A future release will reject this connection.\n%s", msg, advice)
	}
	return nil
}

// loadKnownHosts reads ssh_known_hosts and ~/.ssh/known_hosts.
// It returns nil if there is no file to read.
func (hk SSHHostKey) loadKnownHosts() (ssh.HostKeyCallback, error) {
	homeDir, _ := os.UserHomeDir()
	files := []string{}
	if hk.KnownHosts != "" {
		path := hk.KnownHosts
		if strings.HasPrefix(path, "~/") {
			path = filepath.Join(homeDir, path[2:])
		}
		files = append(files, path)
	}

	// an invalid default file must not break connections that worked before
	if path := filepath.Join(homeDir, ".ssh", "known_hosts"); homeDir != "" && g.PathExists(path) {
		if _, err := knownhosts.New(path); err != nil {
			g.Debug("ignoring %s: %s", path, err.Error())
		} else {
			files = append(files, path)
		}
	}

	if len(files) == 0 {
		return nil, nil
	}

	callback, err := knownhosts.New(files...)
	if err != nil {
		return nil, g.Error(err, "could not read ssh_known_hosts")
	}
	return callback, nil
}

// knownHostKeys returns the keys that known_hosts has for addr
func knownHostKeys(knownHosts ssh.HostKeyCallback, addr string) (keys []ssh.PublicKey) {
	if knownHosts == nil {
		return nil
	}

	// a probe key that is never known makes the callback list the known keys
	probe, _ := ssh.NewPublicKey(ed25519.PublicKey(make([]byte, ed25519.PublicKeySize)))
	var keyErr *knownhosts.KeyError
	if err := knownHosts(addr, &net.TCPAddr{IP: net.IPv4zero}, probe); errors.As(err, &keyErr) {
		for _, known := range keyErr.Want {
			keys = append(keys, known.Key)
		}
	}
	return keys
}

// hostKeyAlgorithms puts the algorithms of the known keys first, as OpenSSH does.
// It returns nil (the default list) if there are no known keys.
func hostKeyAlgorithms(known []ssh.PublicKey) (algorithms []string) {
	if len(known) == 0 {
		return nil
	}
	for _, key := range known {
		if key.Type() == ssh.KeyAlgoRSA {
			algorithms = append(algorithms, ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSASHA256)
		}
		algorithms = append(algorithms, key.Type())
	}
	algorithms = append(algorithms, ssh.SupportedAlgorithms().HostKeys...)
	algorithms = append(algorithms, ssh.InsecureAlgorithms().HostKeys...)

	unique := []string{}
	for _, algo := range algorithms {
		if !slices.Contains(unique, algo) {
			unique = append(unique, algo)
		}
	}
	return unique
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

	sshAddr := g.F("%s:%d", s.Host, s.Port)
	hostKeyAddr := s.HostKeyAddr
	if hostKeyAddr == "" {
		hostKeyAddr = sshAddr
	}

	config, err := s.Options.Config()
	if err != nil {
		return err
	}

	hostKeyCallback, hostKeyAlgorithms, err := s.Options.HostKey.Callback(hostKeyAddr)
	if err != nil {
		return err
	}

	s.config = &ssh.ClientConfig{
		User:              s.User,
		Auth:              authMethods,
		HostKeyCallback:   hostKeyCallback,
		HostKeyAlgorithms: hostKeyAlgorithms,
		Config:            config,
	}

	// Connect to the remote server and perform the SSH handshake.
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

// Close stops the client connection
func (s *SSHClient) Close() {
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

func OpenTunnelSSH(tgtHost string, tgtPort int, tunnelURL, privateKey, passphrase string, options SSHOptions) (localPort int, err error) {

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
		Options:    options,
	}

	localPort, err = sshClient.OpenPortForward()
	if err != nil {
		return 0, g.Error(err, "could not connect to ssh server")
	}

	return
}
