//go:build cgo

package database

import (
	"net/http"
	"os"
	"path/filepath"

	"github.com/flarco/g"
	"github.com/psanford/sqlite3vfs"
	"github.com/psanford/sqlite3vfshttp"
)

type roundTripper struct {
	referer   string
	userAgent string
}

func (rt *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if rt.referer != "" {
		req.Header.Set("Referer", rt.referer)
	}

	if rt.userAgent != "" {
		req.Header.Set("User-Agent", rt.userAgent)
	}

	tr := http.DefaultTransport

	if req.URL.Scheme == "file" {
		path := req.URL.Path
		root := filepath.Dir(path)
		base := filepath.Base(path)
		tr = http.NewFileTransport(http.Dir(root))
		req.URL.Path = base
	}

	return tr.RoundTrip(req)
}

func registerHttpVFS(httpURL string) error {
	vfs := sqlite3vfshttp.HttpVFS{
		URL: httpURL,
		RoundTripper: &roundTripper{
			referer:   os.Getenv("DBIO_APP"),
			userAgent: os.Getenv("DBIO_APP"),
		},
	}

	err := sqlite3vfs.RegisterVFS("httpvfs", &vfs)
	if err != nil {
		return g.Error(err, "register vfs err")
	}
	return nil
}
