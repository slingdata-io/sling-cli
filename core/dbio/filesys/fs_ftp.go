package filesys

import (
	"bufio"
	"context"
	"crypto/tls"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/jlaffaye/ftp"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// FtpFileSysClient is for FTP file systems
type FtpFileSysClient struct {
	BaseFileSysClient
	client    *ftp.ServerConn
	readerMux sync.Mutex // for reader files one at a time
}

// Init initializes the fs client
func (fs *FtpFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)
	fs.readerMux = sync.Mutex{}
	return fs.Connect()
}

// Prefix returns the url prefix
func (fs *FtpFileSysClient) Prefix(suffix ...string) string {
	return g.F("%s://%s:%s", fs.FsType().String(), fs.GetProp("host"), fs.GetProp("port")) + strings.Join(suffix, "")
}

// GetPath returns the path of url
func (fs *FtpFileSysClient) GetPath(uri string) (path string, err error) {
	// normalize, in case url is provided without prefix
	uri = NormalizeURI(fs, uri)

	host, path, err := ParseURL(uri)
	if err != nil {
		return
	}

	if fs.GetProp("host") != host {
		err = g.Error("URL bucket differs from connection bucket. %s != %s", host, fs.GetProp("host"))
	}

	return path, err
}

// Connect initiates the Google Cloud Storage client
func (fs *FtpFileSysClient) Connect() (err error) {

	timeout := cast.ToInt(fs.GetProp("timeout"))
	if timeout == 0 {
		timeout = 5
	}

	if fs.GetProp("port") == "" {
		fs.SetProp("port", "21")
	}

	if fs.GetProp("url") != "" {
		u, err := url.Parse(fs.GetProp("url"))
		if err != nil {
			return g.Error(err, "could not parse SFTP URL")
		}

		host := u.Hostname()
		user := u.User.Username()
		password, _ := u.User.Password()
		port := cast.ToInt(u.Port())

		if user != "" {
			fs.SetProp("user", user)
		}
		if password != "" {
			fs.SetProp("password", password)
		}
		if host != "" {
			fs.SetProp("host", host)
		}
		if port != 0 {
			fs.SetProp("port", cast.ToString(port))
		}
	}

	// via SSH tunnel
	if sshTunnelURL := fs.GetProp("ssh_tunnel"); sshTunnelURL != "" {

		tunnelPrivateKey := fs.GetProp("ssh_private_key")
		tunnelPassphrase := fs.GetProp("ssh_passphrase")

		localPort, err := iop.OpenTunnelSSH(fs.GetProp("host"), cast.ToInt(fs.GetProp("port")), sshTunnelURL, tunnelPrivateKey, tunnelPassphrase)
		if err != nil {
			return g.Error(err, "could not connect to ssh tunnel server")
		}

		fs.SetProp("host", "127.0.0.1")
		fs.SetProp("port", cast.ToString(localPort))
	}

	address := g.F("%s:%s", fs.GetProp("host"), fs.GetProp("port"))

	options := []ftp.DialOption{
		ftp.DialWithTimeout(time.Duration(timeout) * time.Second),
		ftp.DialWithForceListHidden(true),
		// ftp.DialWithDebugOutput(os.Stderr),
	}

	if cast.ToBool(fs.GetProp("ftps")) {
		tlsConfig := &tls.Config{
			// Enable TLS 1.2.
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		}
		options = append(options, ftp.DialWithExplicitTLS(tlsConfig))
	}

	fs.client, err = ftp.Dial(address, options...)
	if err != nil {
		return g.Error(err, "unable to connect to ftp server ")
	}

	err = fs.client.Login(fs.GetProp("user"), fs.GetProp("password"))
	if err != nil {
		return g.Error(err, "unable to login into ftp server")
	}

	return nil
}

func (fs *FtpFileSysClient) Close() error {
	if fs.client != nil {
		fs.client.Logout()
		if err := fs.client.Quit(); err != nil {
			return g.Error(err, "could not close ftp connection")
		}

		// if !cast.ToBool(fs.GetProp("silent")) {
		// 	g.Debug(`closed "%s" connection (%s)`, fs.FsType(), fs.GetProp("sling_conn_id"))
		// }
	}
	return nil
}
func (fs *FtpFileSysClient) Reconnect(reason string) {
	// if !cast.ToBool(fs.GetProp("silent")) {
	// 	g.Debug(`reconnecting "%s" connection (%s) due to: %s`, fs.FsType(), fs.GetProp("sling_conn_id"), reason)
	// }

	fs.Close()
	fs.Connect()
}

// List list objects in path
func (fs *FtpFileSysClient) List(url string) (nodes FileNodes, err error) {
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

	// to ensure correct working dir
	fs.client.ChangeDir("/")

	file, err := fs.client.GetEntry(strings.TrimSuffix(path, "/"))
	if err == nil && file.Name != "." {
		if path == file.Name {
			path = ""
		} else {
			path = path + "/"
			if strings.HasSuffix(path, "//") {
				path = strings.TrimSuffix(path, "/")
			}
		}
		node := FileNode{
			URI:     g.F("%s%s%s", fs.Prefix("/"), path, file.Name),
			Updated: file.Time.Unix(),
			Size:    file.Size,
			IsDir:   file.Type == ftp.EntryTypeFolder,
		}
		if !node.IsDir {
			nodes.AddWhere(pattern, 0, node)
			return
		} else if !strings.HasSuffix(url, "/") && !strings.Contains(url, "*") {
			nodes.Add(node)
			// need trailing slash to enumerate children
			return
		}
	}

	entries, err := fs.client.List(path)
	if err != nil {
		return nodes, g.Error(err, "error listing path")
	}

	for _, file := range entries {
		if g.In(file.Name, "..", ".") {
			continue
		} else if path == file.Name {
			path = "" //
		} else {
			path = path + "/"
			if strings.HasSuffix(path, "//") {
				path = strings.TrimSuffix(path, "/")
			}
		}

		node := FileNode{
			URI:     g.F("%s%s%s", fs.Prefix("/"), path, file.Name),
			Size:    file.Size,
			Updated: file.Time.Unix(),
			IsDir:   file.Type == ftp.EntryTypeFolder,
		}

		// replace double slash issue
		node.URI = fs.cleanUpNodeURI(node.URI)
		nodes.AddWhere(pattern, 0, node)
	}

	return
}

// ListRecursive list objects in path recursively
func (fs *FtpFileSysClient) ListRecursive(url string) (nodes FileNodes, err error) {

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

	// to ensure correct working dir
	fs.client.ChangeDir("/")

	// g.Info("url = %s | path = %s", url, path)
	ts := fs.GetRefTs().Unix()

	filePath := strings.TrimSuffix(path, "/")
	file, err := fs.client.GetEntry(filePath)
	if err == nil && file.Name != "." {
		if filePath == file.Name {
			filePath = ""
		} else {
			filePath = filePath + "/"
			if strings.HasSuffix(filePath, "//") {
				filePath = strings.TrimSuffix(filePath, "/")
			}
		}
		node := FileNode{
			URI:     g.F("%s%s%s", fs.Prefix("/"), filePath, file.Name),
			Updated: file.Time.Unix(),
			Size:    file.Size,
			IsDir:   file.Type == ftp.EntryTypeFolder,
		}
		if !node.IsDir {
			nodes.Add(node)
			return
		}
	}

	entries, err := fs.client.List(path)
	if err != nil {
		err = g.Error(err, "Error listing: "+path)
		return
	}
	// names := lo.Map(entries, func(e *ftp.Entry, i int) string { return e.Name })
	// g.Warn("      path = %s  |  names = %s", path, g.Marshal(names))

	for _, file := range entries {
		if g.In(file.Name, "..", ".") {
			continue
		} else if path == file.Name {
			path = "" //
		} else {
			path = path + "/"
			if strings.HasSuffix(path, "//") {
				path = strings.TrimSuffix(path, "/")
			}
		}

		node := FileNode{
			URI:     g.F("%s%s%s", fs.Prefix("/"), path, file.Name),
			Size:    file.Size,
			Updated: file.Time.Unix(),
			IsDir:   file.Type == ftp.EntryTypeFolder,
		}

		if node.IsDir {
			dirPath := g.F("%s%s%s", fs.Prefix("/"), strings.TrimPrefix(path, "/"), file.Name)
			dirPath = strings.TrimSuffix(dirPath, "/") + "/"
			// g.Warn("   dirPath = " + dirPath)
			subNodes, err := fs.ListRecursive(dirPath)
			if err != nil {
				return nil, g.Error(err, "error listing sub path")
			}
			nodes.AddWhere(pattern, ts, subNodes...)
		}

		// replace double slash issue
		node.URI = fs.cleanUpNodeURI(node.URI)
		// g.Warn("    node.URI = " + node.URI)
		nodes.AddWhere(pattern, ts, node)
	}

	return
}

// cleanUpNodeURI replace double slash issue
func (fs *FtpFileSysClient) cleanUpNodeURI(uri string) string {
	return strings.ReplaceAll(
		strings.ReplaceAll(
			strings.ReplaceAll(
				uri, "://", ":::",
			), "//", "/"),
		":::", "://",
	)
}

// Delete list objects in path
func (fs *FtpFileSysClient) delete(uri string) (err error) {
	_, err = fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	nodes, err := fs.ListRecursive(uri)
	if err != nil {
		return g.Error(err, "error listing path")
	}

	for _, node := range nodes.Files() {
		err = fs.client.Delete(strings.TrimSuffix(node.Path(), "/"))
		if err != nil && !strings.Contains(err.Error(), "No such file") && !strings.Contains(err.Error(), "550") {
			return g.Error(err, "error deleting path "+node.URI)
		}
	}

	for _, node := range nodes.Folders() {
		err = fs.client.RemoveDirRecur(strings.TrimSuffix(node.Path(), "/"))
		if err != nil && !strings.Contains(err.Error(), "No such file") && !strings.Contains(err.Error(), "550") {
			return g.Error(err, "error deleting path "+node.URI)
		}
	}

	return nil
}

// MkdirAll creates child directories
func (fs *FtpFileSysClient) MkdirAll(path string) (err error) {
	// to ensure correct working dir
	fs.client.ChangeDir("/")

	filePathArr := strings.Split(path, "/")
	if len(filePathArr) > 1 {
		folderPath := strings.Join(filePathArr[:len(filePathArr)-1], "/")

		// need to make all parents
		parts := []string{}
		for _, part := range strings.Split(folderPath, "/") {
			parts = append(parts, part)
			dirPath := strings.Join(parts, "/")
			if _, err := fs.client.GetEntry(dirPath); err == nil {
				continue // don't make directory if exists
			}

			err = fs.client.MakeDir(dirPath)
			if err != nil {
				if !strings.Contains(err.Error(), "exists") && !strings.Contains(err.Error(), "250") {
					err = g.Error(err, "Unable to create directory '%s'", dirPath)
					return
				}
				fs.Reconnect(err.Error()) // improves stability
			}
		}
	}
	return nil
}

func (fs *FtpFileSysClient) Write(urlStr string, reader io.Reader) (bw int64, err error) {
	path, err := fs.GetPath(urlStr)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}
	// manage concurrency
	defer fs.Context().Wg.Write.Done()
	fs.Context().Wg.Write.Add()

	// to ensure correct working dir
	fs.client.ChangeDir("/")

	path = strings.TrimPrefix(path, "/")
	if filePathArr := strings.Split(path, "/"); len(filePathArr) > 1 {
		if err = fs.MkdirAll(path); err != nil {
			return 0, g.Error(err, "Unable to create directory '%s'", path)
		}
	}

	err = fs.client.Stor(path, reader)
	if err != nil {
		err = g.Error(err, "Unable to write "+path)
		return
	}

	return
}

// GetReader return a reader for the given path
func (fs *FtpFileSysClient) GetReader(urlStr string) (reader io.Reader, err error) {
	path, err := fs.GetPath(urlStr)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}

	// connector can only read one file at a time
	fs.readerMux.Lock()

	file, err := fs.client.Retr(path)
	if err != nil {
		if strings.Contains(err.Error(), "229") {
			fs.Reconnect(err.Error())
			file, err = fs.client.Retr(path)
		}
		if err != nil {
			err = g.Error(err, "Unable to open "+path)
			return
		}
	}

	pipeR, pipeW := io.Pipe()

	go func() {
		defer fs.readerMux.Unlock()
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
