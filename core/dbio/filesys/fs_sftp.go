package filesys

import (
	"bufio"
	"context"
	"io"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/flarco/g"
	"github.com/pkg/sftp"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// SftpFileSysClient is for SFTP / SSH file ops
type SftpFileSysClient struct {
	BaseFileSysClient
	client    *sftp.Client
	sshClient *iop.SSHClient
}

// Init initializes the fs client
func (fs *SftpFileSysClient) Init(ctx context.Context) (err error) {
	instance := FileSysClient(fs)
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)
	return fs.Connect()
}

// Prefix returns the url prefix
func (fs *SftpFileSysClient) Prefix(suffix ...string) string {
	return g.F("%s://%s:%s", fs.FsType().String(), fs.GetProp("host"), fs.GetProp("port")) + strings.Join(suffix, "")

}

// GetPath returns the path of url
func (fs *SftpFileSysClient) GetPath(uri string) (path string, err error) {
	// normalize, in case url is provided without prefix
	uri = NormalizeURI(fs, uri)

	_, path, err = ParseURL(uri)
	if err != nil {
		return
	}

	return path, err
}

// Connect initiates the Google Cloud Storage client
func (fs *SftpFileSysClient) Connect() (err error) {

	if fs.GetProp("PRIVATE_KEY") == "" {
		defPrivKey := path.Join(g.UserHomeDir(), ".ssh", "id_rsa")
		if g.PathExists(defPrivKey) {
			g.Debug("adding default private key (%s) as auth method for SFTP", defPrivKey)
			fs.SetProp("PRIVATE_KEY", defPrivKey)
		}
	}

	if fs.GetProp("URL") != "" {
		u, err := url.Parse(fs.GetProp("URL"))
		if err != nil {
			return g.Error(err, "could not parse SFTP URL")
		}

		host := u.Hostname()
		user := u.User.Username()
		password, _ := u.User.Password()
		sshPort := cast.ToInt(u.Port())
		if sshPort == 0 {
			sshPort = 22
		}

		if user != "" {
			fs.SetProp("USER", user)
		}
		if password != "" {
			fs.SetProp("PASSWORD", password)
		}
		if host != "" {
			fs.SetProp("HOST", host)
		}
		if sshPort != 0 {
			fs.SetProp("PORT", cast.ToString(sshPort))
		}
	}

	fs.sshClient = &iop.SSHClient{
		Host:       fs.GetProp("HOST"),
		Port:       cast.ToInt(fs.GetProp("PORT")),
		User:       fs.GetProp("USER"),
		Password:   fs.GetProp("PASSWORD"),
		PrivateKey: fs.GetProp("PRIVATE_KEY"), // raw value or path to ssh key file
		Passphrase: fs.GetProp("PASSPHRASE"),
	}

	// via SSH tunnel
	if sshTunnelURL := fs.GetProp("ssh_tunnel"); sshTunnelURL != "" {

		tunnelPrivateKey := fs.GetProp("ssh_private_key")
		tunnelPassphrase := fs.GetProp("ssh_passphrase")

		localPort, err := iop.OpenTunnelSSH(fs.sshClient.Host, fs.sshClient.Port, sshTunnelURL, tunnelPrivateKey, tunnelPassphrase)
		if err != nil {
			return g.Error(err, "could not connect to ssh tunnel server")
		}

		fs.sshClient.Host = "127.0.0.1"
		fs.sshClient.Port = localPort
	}

	err = fs.sshClient.Connect()
	if err != nil {
		return g.Error(err, "unable to connect to ssh server ")
	}

	fs.client, err = fs.sshClient.SftpClient()
	if err != nil {
		return g.Error(err, "unable to start SFTP client")
	}

	return nil
}

// Close closes the client
func (fs *SftpFileSysClient) Close() error {
	if fs.client != nil {
		return fs.client.Close()
	}
	return nil
}

// List list objects in path
func (fs *SftpFileSysClient) List(url string) (nodes FileNodes, err error) {
	path, err := fs.GetPath(url)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+url)
		return
	}

	pattern, err := makeGlob(NormalizeURI(fs, url))
	if err != nil {
		err = g.Error(err, "Error Parsing url pattern: "+url)
		return
	}

	var files []os.FileInfo
	stat, err := fs.client.Stat(strings.TrimSuffix(path, "/"))
	if err == nil && (!stat.IsDir() || !strings.HasSuffix(path, "/")) {
		node := FileNode{
			URI:     g.F("%s%s", fs.Prefix("/"), path),
			Updated: stat.ModTime().Unix(),
			Size:    cast.ToUint64(stat.Size()),
			IsDir:   stat.IsDir(),
		}
		nodes.Add(node)
		return
	}

	if path == "" {
		path = "/"
	} else if path != "/" {
		path = strings.TrimSuffix(path, "/")
	}
	files, err = fs.client.ReadDir(path)
	if err != nil {
		return nodes, g.Error(err, "error listing path")
	}

	if path == "/" {
		path = ""
	}

	for _, file := range files {
		node := FileNode{
			URI:     g.F("%s%s%s", fs.Prefix("/"), path+"/", file.Name()),
			Updated: file.ModTime().Unix(),
			Size:    cast.ToUint64(file.Size()),
			IsDir:   file.IsDir(),
		}
		nodes.AddWhere(pattern, 0, node)
	}

	return
}

// ListRecursive list objects in path recursively
func (fs *SftpFileSysClient) ListRecursive(uri string) (nodes FileNodes, err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	pattern, err := makeGlob(NormalizeURI(fs, uri))
	if err != nil {
		err = g.Error(err, "Error Parsing url pattern: "+uri)
		return
	}

	ts := fs.GetRefTs().Unix()

	var files []os.FileInfo
	stat, err := fs.client.Stat(strings.TrimSuffix(path, "/"))
	if err == nil {
		node := FileNode{
			URI:     g.F("%s%s", fs.Prefix("/"), path),
			Updated: stat.ModTime().Unix(),
			Size:    cast.ToUint64(stat.Size()),
			IsDir:   stat.IsDir(),
		}
		if pattern == nil {
			nodes.Add(node)
		}
		if !stat.IsDir() {
			return
		}
	}

	path = strings.TrimSuffix(path, "/")
	files, err = fs.client.ReadDir(path)
	if err != nil {
		return nodes, g.Error(err, "error listing path")
	}

	for _, file := range files {
		node := FileNode{
			URI:     g.F("%s%s%s", fs.Prefix("/"), path+"/", file.Name()),
			Updated: file.ModTime().Unix(),
			Size:    cast.ToUint64(file.Size()),
			IsDir:   file.IsDir(),
		}

		if file.IsDir() {
			subNodes, err := fs.ListRecursive(node.Path() + "/")
			if err != nil {
				return nil, g.Error(err, "error listing sub path")
			}
			nodes.AddWhere(pattern, ts, subNodes...)
		} else {
			nodes.AddWhere(pattern, ts, node)
		}
	}

	return
}

// Delete list objects in path
func (fs *SftpFileSysClient) delete(uri string) (err error) {
	_, err = fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	nodes, err := fs.ListRecursive(uri)
	if err != nil {
		return g.Error(err, "error listing path")
	}

	for _, sNode := range nodes.Files() {
		err = fs.client.Remove(sNode.Path())
		if err != nil {
			return g.Error(err, "error deleting path "+sNode.URI)
		}
	}

	for _, sNode := range nodes.Folders() {
		err = fs.client.Remove(sNode.Path())
		if err != nil {
			return g.Error(err, "error deleting path "+sNode.URI)
		}
	}

	return nil
}

// MkdirAll creates child directories
func (fs *SftpFileSysClient) MkdirAll(path string) (err error) {
	return fs.client.MkdirAll(path)
}

func (fs *SftpFileSysClient) Write(urlStr string, reader io.Reader) (bw int64, err error) {
	path, err := fs.GetPath(urlStr)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}
	// manage concurrency
	defer fs.Context().Wg.Write.Done()
	fs.Context().Wg.Write.Add()

	filePathArr := strings.Split(path, "/")
	if len(filePathArr) > 1 {
		folderPath := strings.Join(filePathArr[:len(filePathArr)-1], "/")
		err = fs.client.MkdirAll(folderPath)
		if err != nil {
			err = g.Error(err, "Unable to create directory "+folderPath)
			return
		}
	}

	file, err := fs.client.Create(path)
	if err != nil && strings.Contains(err.Error(), "SSH_FX_OP_UNSUPPORTED") {
		// https://github.com/pkg/sftp/issues/305
		file, err = fs.client.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	}
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return
	}
	defer file.Close()

	bw, err = io.Copy(io.Writer(file), reader)
	if err != nil {
		err = g.Error(err, "Error writing from reader")
	}
	return
}

// GetReader return a reader for the given path
func (fs *SftpFileSysClient) GetReader(urlStr string) (reader io.Reader, err error) {
	path, err := fs.GetPath(urlStr)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}

	file, err := fs.client.Open(path)
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return
	}

	pipeR, pipeW := io.Pipe()

	go func() {
		defer pipeW.Close()

		reader = bufio.NewReader(file)

		_, err = io.Copy(pipeW, reader)
		if err != nil {
			fs.Context().CaptureErr(g.Error(err, "Error writing from reader"))
			fs.Context().Cancel()
			g.LogError(fs.Context().Err())
		}

	}()

	return pipeR, err
}

// GetWriter creates the file if non-existent and return a writer
func (fs *SftpFileSysClient) GetWriter(urlStr string) (writer io.Writer, err error) {
	path, err := fs.GetPath(urlStr)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}
	file, err := fs.client.Create(path)
	if err != nil && strings.Contains(err.Error(), "SSH_FX_OP_UNSUPPORTED") {
		// https://github.com/pkg/sftp/issues/305
		file, err = fs.client.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC)
	}
	if err != nil {
		err = g.Error(err, "Unable to open "+path)
		return
	}
	writer = io.Writer(file)
	return
}
