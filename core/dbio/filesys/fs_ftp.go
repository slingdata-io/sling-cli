package filesys

import (
	"bufio"
	"context"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/jlaffaye/ftp"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"
)

// FtpFileSysClient is for FTP file systems
type FtpFileSysClient struct {
	BaseFileSysClient
	client *ftp.ServerConn
}

// Init initializes the fs client
func (fs *FtpFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)
	return fs.Connect()
}

// Prefix returns the url prefix
func (fs *FtpFileSysClient) Prefix(suffix ...string) string {
	return g.F("%s://%s", fs.fsType.String(), fs.GetProp("host")) + strings.Join(suffix, "")
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

	if fs.GetProp("URL") != "" {
		u, err := url.Parse(fs.GetProp("URL"))
		if err != nil {
			return g.Error(err, "could not parse SFTP URL")
		}

		host := u.Hostname()
		user := u.User.Username()
		password, _ := u.User.Password()
		port := cast.ToInt(u.Port())
		if port == 0 {
			port = 21
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
		if port != 0 {
			fs.SetProp("PORT", cast.ToString(port))
		}
	}

	address := g.F("%s:21", fs.GetProp("host"))
	if port := fs.GetProp("port"); port != "" {
		address = g.F("%s:%s", fs.GetProp("host"), fs.GetProp("port"))
	}

	fs.client, err = ftp.Dial(address, ftp.DialWithTimeout(time.Duration(timeout)*time.Second))
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
	if err := fs.client.Quit(); err != nil {
		return g.Error(err, "could not close ftp connection")
	}
	return nil
}

// List list objects in path
func (fs *FtpFileSysClient) List(url string) (paths dbio.FileNodes, err error) {
	path, err := fs.GetPath(url)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+url)
		return
	}

	entries, err := fs.client.List(path)
	if err != nil {
		return paths, g.Error(err, "error listing path")
	}

	for _, entry := range entries {
		file := dbio.FileNode{
			URI:     g.F("%s/%s%s", fs.Prefix(), path, entry.Name),
			Size:    entry.Size,
			Updated: entry.Time.Unix(),
		}
		if entry.Type == ftp.EntryTypeFolder {
			path = path + "/"
		}
		paths = append(paths, file)
	}

	return
}

// ListRecursive list objects in path recursively
func (fs *FtpFileSysClient) ListRecursive(url string) (nodes dbio.FileNodes, err error) {
	return dbio.FileNodes{{URI: url}}, nil

	path, err := fs.GetPath(url)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+url)
		return
	}

	pattern, err := makeGlob(url)
	if err != nil {
		err = g.Error(err, "Error Parsing url pattern: "+url)
		return
	}

	ts := fs.GetRefTs().Unix()

	entries, err := fs.client.List(path)
	if err != nil {
		err = g.Error(err, "Error listing: "+path)
		return
	}

	for _, file := range entries {
		node := dbio.FileNode{
			URI:     g.F("%s/%s%s", fs.Prefix(), path, file.Name),
			Size:    file.Size,
			Updated: file.Time.Unix(),
		}

		path := g.F("%s%s%s", fs.Prefix(), path, file.Name)
		if file.Type == ftp.EntryTypeFolder {
			subNodes, err := fs.ListRecursive(path)
			// g.P(subPaths)
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
func (fs *FtpFileSysClient) delete(urlStr string) (err error) {
	// _, path, err := ParseURL(urlStr)
	// if err != nil {
	// 	err = g.Error(err, "Error Parsing url: "+urlStr)
	// 	return
	// }
	// path = "/" + fs.cleanKey(path)

	// err = fs.client.Delete(path)
	// if err != nil && !strings.Contains(err.Error(), "not exist") {
	// 	return g.Error(err, "error deleting path")
	// }
	return nil
}

// MkdirAll creates child directories
func (fs *FtpFileSysClient) MkdirAll(path string) (err error) {
	fs.client.MakeDir(path)
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

	path = strings.TrimPrefix(path, "/")
	filePathArr := strings.Split(path, "/")
	if len(filePathArr) > 1 {
		folderPath := strings.Join(filePathArr[:len(filePathArr)-1], "/")
		err = fs.client.MakeDir(folderPath)
		if err != nil {
			err = g.Error(err, "Unable to create directory '%s'", folderPath)
			return
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

	file, err := fs.client.Retr(path)
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
