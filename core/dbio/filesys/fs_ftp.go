package filesys

import (
	"bufio"
	"context"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/jlaffaye/ftp"
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

func (fs *FtpFileSysClient) getPrefix() string {
	return g.F(
		"ftp://%s@%s:%s",
		fs.GetProp("USER"),
		fs.GetProp("HOST"),
		fs.GetProp("PORT"),
	)
}

func (fs *FtpFileSysClient) cleanKey(key string) string {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")
	return key
}

// List list objects in path
func (fs *FtpFileSysClient) List(url string) (paths []string, err error) {
	_, path, err := ParseURL(url)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+url)
		return
	}
	path = "/" + fs.cleanKey(path)

	entries, err := fs.client.List(path)
	if err != nil {
		return paths, g.Error(err, "error listing path")
	}

	for _, entry := range entries {
		path := g.F("%s%s%s", fs.getPrefix(), path, entry.Name)
		if entry.Type == ftp.EntryTypeFolder {
			path = path + "/"
		}
		paths = append(paths, path)
	}

	sort.Strings(paths)

	return
}

// ListRecursive list objects in path recursively
func (fs *FtpFileSysClient) ListRecursive(url string) (paths []string, err error) {
	return []string{url}, nil

	_, path, err := ParseURL(url)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+url)
		return
	}
	path = "/" + fs.cleanKey(path)
	ts := fs.GetRefTs()

	entries, err := fs.client.List(path)
	if err != nil {
		err = g.Error(err, "Error listing: "+path)
		return
	}

	for _, file := range entries {
		if ts.IsZero() || file.Time.IsZero() || file.Time.After(ts) {
			path := g.F("%s%s%s", fs.getPrefix(), path, file.Name)
			if file.Type == ftp.EntryTypeFolder {
				subPaths, err := fs.ListRecursive(path)
				// g.P(subPaths)
				if err != nil {
					return []string{}, g.Error(err, "error listing sub path")
				}
				paths = append(paths, subPaths...)
			} else {
				paths = append(paths, path)
			}
		}
	}
	sort.Strings(paths)

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
	_, path, err := ParseURL(urlStr)
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
	_, path, err := ParseURL(urlStr)
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
