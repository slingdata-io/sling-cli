package filesys

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
)

// HTTPFileSysClient is for HTTP files
type HTTPFileSysClient struct {
	BaseFileSysClient
	context  g.Context
	client   *http.Client
	username string
	password string
}

// Init initializes the fs client
func (fs *HTTPFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)
	return fs.Connect()
}

// Connect initiates the http client
func (fs *HTTPFileSysClient) Connect() (err error) {
	// fs.client = &http.Client{Timeout: 20 * time.Second}
	fs.client = &http.Client{}
	fs.username = fs.GetProp("HTTP_USER")
	fs.password = fs.GetProp("HTTP_PASSWORD")
	return
}

// Prefix returns the url prefix
func (fs *HTTPFileSysClient) Prefix(suffix ...string) string {
	return g.F("%s://%s", fs.FsType().String(), fs.GetProp("host")) + strings.Join(suffix, "")
}

// GetPath returns the path of url
func (fs *HTTPFileSysClient) GetPath(uri string) (path string, err error) {
	// normalize, in case url is provided without prefix
	uri = fs.Prefix("/") + strings.TrimLeft(strings.TrimPrefix(uri, fs.Prefix()), "/")

	host, path, err := ParseURL(uri)
	if err != nil {
		return
	}

	if fs.GetProp("host") != host {
		err = g.Error("URL bucket differs from connection bucket. %s != %s", host, fs.GetProp("host"))
	}

	return path, err
}

func (fs *HTTPFileSysClient) doGet(url string) (resp *http.Response, err error) {
	// Request the HTML page.
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, g.Error(err, "could not construct request")
	}

	if fs.username != "" && fs.password != "" {
		req.SetBasicAuth(fs.username, fs.password)
	}

	req.Header.Set("DNT", "1")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")

	// req.Header.Write(os.Stdout)
	g.Trace(url)
	resp, err = fs.client.Do(req)
	if err != nil {
		return nil, g.Error(err, "could not load HTTP url")
	}
	g.Trace("Content-Type: " + resp.Header.Get("Content-Type"))

	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		err = g.Error("status code error: %d %s", resp.StatusCode, resp.Status)
		return resp, g.Error(err, "status code error: %d %s", resp.StatusCode, resp.Status)
	}

	return
}

// Delete :
func (fs *HTTPFileSysClient) delete(path string) (err error) {
	err = g.Error("cannot delete a HTTP file")
	return
}

// List lists all urls on the page
func (fs *HTTPFileSysClient) List(url string) (nodes dbio.FileNodes, err error) {

	if strings.HasPrefix(url, "https://docs.google.com/spreadsheets/d") {
		return dbio.FileNodes{{URI: url}}, nil
	}

	if strings.HasSuffix(url, "/") {
		url = url[:len(url)-1]
	}

	resp, err := fs.doGet(url)
	if err != nil {
		return nil, g.Error(err, "could not load HTTP url")
	}

	if !strings.Contains(resp.Header.Get("Content-Type"), "text/html") {
		// We have no easy way of determining if the url is a page with link
		// or if the url is a body of data. Fow now, return url if not "text/html"
		nodes.Add(dbio.FileNode{URI: url})
		return
	}

	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nodes, g.Error(err, "could not parse HTTP body")
	}
	// html, _ := doc.Html()
	// g.Trace("Html: " + html)

	// Find the review items
	urlParent := url
	urlArr := strings.Split(url, "/")
	if len(urlArr) > 3 {
		urlParent = strings.Join(urlArr[:len(urlArr)-1], "/")
	}

	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		// For each item found, get the band and title
		link, ok := s.Attr("href")
		if ok {
			if strings.HasPrefix(link, "http://") || strings.HasPrefix(link, "https://") {
				nodes.Add(dbio.FileNode{URI: link})
			} else {
				nodes.Add(dbio.FileNode{URI: urlParent + "/" + link})
			}
		}
	})
	return
}

// ListRecursive lists all urls on the page
func (fs *HTTPFileSysClient) ListRecursive(url string) (nodes dbio.FileNodes, err error) {
	return fs.List(url)
}

// GetReader gets a reader for an HTTP resource (download)
func (fs *HTTPFileSysClient) GetReader(url string) (reader io.Reader, err error) {
	if strings.HasPrefix(url, "https://docs.google.com/spreadsheets/d") {
		ggs, err := iop.NewGoogleSheetFromURL(
			url, "GSHEET_CLIENT_JSON_BODY="+fs.GetProp("GSHEET_CLIENT_JSON_BODY"),
		)
		if err != nil {
			return nil, g.Error(err, "could not load google sheets")
		}
		for k, v := range fs.Client().Props() {
			ggs.Props[k] = v
		}

		data, err := ggs.GetDataset(fs.GetProp("GSHEET_SHEET_NAME"))
		if err != nil {
			return nil, g.Error(err, "could not open sheet: "+fs.GetProp("GSHEET_SHEET_NAME"))
		}
		return data.Stream().NewCsvReader(0, 0), nil
	}

	resp, err := fs.doGet(url)
	if err != nil {
		return nil, g.Error(err, "could not load HTTP url")
	}
	g.Trace("ContentLength: %d", resp.ContentLength)

	return resp.Body, nil
}

// Write uploads an HTTP file
func (fs *HTTPFileSysClient) Write(urlStr string, reader io.Reader) (bw int64, err error) {
	if strings.HasPrefix(urlStr, "https://docs.google.com/spreadsheets/d") {
		ggs, err := iop.NewGoogleSheetFromURL(
			urlStr, "GSHEET_CLIENT_JSON_BODY="+fs.GetProp("GSHEET_CLIENT_JSON_BODY"),
		)
		if err != nil {
			return 0, g.Error(err, "could not load google sheets")
		}

		csv := iop.CSV{Reader: reader}
		ds, err := csv.ReadStream()
		if err != nil {
			return 0, g.Error(err, "could not parse csv stream")
		}

		err = ggs.WriteSheet(fs.GetProp("GSHEET_SHEET_NAME"), ds, fs.GetProp("GSHEET_MODE"))
		if err != nil {
			return 0, g.Error(err, "could not write to sheet: "+fs.GetProp("GSHEET_SHEET_NAME"))
		}
		return 0, nil
	}

	err = g.Error("cannot write a HTTP file (yet)")
	return
}
