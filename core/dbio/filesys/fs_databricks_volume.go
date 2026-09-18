package filesys

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/spf13/cast"
)

// DatabricksVolumeFileSysClient handles Databricks Unity Catalog Volumes via Files REST API
// API Reference: https://docs.databricks.com/api/workspace/files
// Endpoints:
// - PUT /api/2.0/fs/files/Volumes/{catalog}/{schema}/{volume}/{path}?overwrite=true
// - GET /api/2.0/fs/files/Volumes/{catalog}/{schema}/{volume}/{path}
// - DELETE /api/2.0/fs/files/Volumes/{catalog}/{schema}/{volume}/{path}
// - GET /api/2.0/fs/directories/Volumes/{catalog}/{schema}/{volume}/{path}
// - PUT /api/2.0/fs/directories/Volumes/{catalog}/{schema}/{volume}/{path}
type DatabricksVolumeFileSysClient struct {
	BaseFileSysClient
	client     *http.Client
	host       string
	token      string
	catalog    string
	schema     string
	volume     string
	basePrefix string
}

type databricksDirListResp struct {
	Contents []databricksFileInfo `json:"contents"`
	NextPageToken string          `json:"next_page_token"`
}

type databricksFileInfo struct {
	Path         string `json:"path"`
	IsDirectory  bool   `json:"is_directory"`
	FileSize     int64  `json:"file_size"`
	LastModified int64  `json:"last_modified"`
}

// Init initializes the Databricks Volume filesystem client
func (fs *DatabricksVolumeFileSysClient) Init(ctx context.Context) (err error) {
	instance := FileSysClient(fs)
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)

	// Props can come from host, token, catalog, schema, volume
	fs.host = fs.GetProp("host")
	fs.token = fs.GetProp("token")
	if fs.token == "" {
		fs.token = fs.GetProp("DATABRICKS_TOKEN")
	}
	if fs.host == "" {
		fs.host = fs.GetProp("DATABRICKS_HOST")
	}

	rawURL := fs.GetProp("url", "URL")
	if rawURL != "" {
		fs.parseURL(rawURL)
	}

	if cat := fs.GetProp("catalog"); cat != "" {
		fs.catalog = cat
	}
	if sch := fs.GetProp("schema"); sch != "" {
		fs.schema = sch
	}
	if vol := fs.GetProp("volume"); vol != "" {
		fs.volume = vol
	}

	// Clean host
	fs.host = strings.TrimPrefix(fs.host, "https://")
	fs.host = strings.TrimPrefix(fs.host, "http://")
	fs.host = strings.TrimRight(fs.host, "/")

	fs.client = &http.Client{
		Timeout: 30 * time.Minute, // Allow long streams for big files
	}

	return nil
}

func (fs *DatabricksVolumeFileSysClient) parseURL(rawURL string) {
	// e.g. databricks-volume://catalog/schema/volume/path
	// or databricks-volume://host/Volumes/catalog/schema/volume/path
	// or databricks://Volumes/catalog/schema/volume/path
	cleaned := rawURL
	for _, prefix := range []string{"databricks-volume://", "databricks://", "volume://"} {
		if strings.HasPrefix(cleaned, prefix) {
			cleaned = strings.TrimPrefix(cleaned, prefix)
			break
		}
	}

	parts := strings.Split(strings.Trim(cleaned, "/"), "/")
	if len(parts) > 0 && parts[0] == "Volumes" {
		parts = parts[1:]
	}

	// Check if the first part looks like a domain (e.g. adb-123.azuredatabricks.net)
	if len(parts) > 0 && (strings.Contains(parts[0], ".databricks.com") || strings.Contains(parts[0], ".azuredatabricks.net")) {
		if fs.host == "" {
			fs.host = parts[0]
		}
		parts = parts[1:]
		if len(parts) > 0 && parts[0] == "Volumes" {
			parts = parts[1:]
		}
	}

	if len(parts) >= 1 && fs.catalog == "" {
		fs.catalog = parts[0]
	}
	if len(parts) >= 2 && fs.schema == "" {
		fs.schema = parts[1]
	}
	if len(parts) >= 3 && fs.volume == "" {
		fs.volume = parts[2]
	}
}

// Prefix returns the url prefix
func (fs *DatabricksVolumeFileSysClient) Prefix(suffix ...string) string {
	var prefix string
	if fs.catalog != "" && fs.schema != "" && fs.volume != "" {
		prefix = fmt.Sprintf("databricks-volume://%s/%s/%s/", fs.catalog, fs.schema, fs.volume)
	} else {
		prefix = "databricks-volume://"
	}
	return prefix + strings.Join(suffix, "")
}

// GetPath converts a uri into the internal volume path starting with /Volumes/...
func (fs *DatabricksVolumeFileSysClient) GetPath(uri string) (volumePath string, err error) {
	uri = NormalizeURI(fs, uri)

	clean := uri
	for _, prefix := range []string{"databricks-volume://", "databricks://", "volume://"} {
		if strings.HasPrefix(clean, prefix) {
			clean = strings.TrimPrefix(clean, prefix)
			break
		}
	}

	parts := strings.Split(strings.Trim(clean, "/"), "/")
	if len(parts) > 0 && (strings.Contains(parts[0], ".databricks.com") || strings.Contains(parts[0], ".azuredatabricks.net")) {
		parts = parts[1:]
	}
	if len(parts) > 0 && parts[0] == "Volumes" {
		parts = parts[1:]
	}

	var catalog, schema, volume string
	var subparts []string

	if len(parts) >= 3 {
		catalog = parts[0]
		schema = parts[1]
		volume = parts[2]
		subparts = parts[3:]
	} else {
		catalog = fs.catalog
		schema = fs.schema
		volume = fs.volume
		subparts = parts
	}

	if catalog == "" || schema == "" || volume == "" {
		return "", g.Error("invalid volume URI, catalog/schema/volume must be specified: %s", uri)
	}

	p := fmt.Sprintf("/Volumes/%s/%s/%s", catalog, schema, volume)
	if len(subparts) > 0 && subparts[0] != "" {
		p = p + "/" + strings.Join(subparts, "/")
	}
	return p, nil
}

// doRequest executes an HTTP request against Databricks Files REST API with retries
func (fs *DatabricksVolumeFileSysClient) doRequest(ctx context.Context, method, apiPath string, body io.Reader, query url.Values) (*http.Response, error) {
	if fs.host == "" {
		return nil, g.Error("databricks host is required")
	}
	if fs.token == "" {
		return nil, g.Error("databricks token is required")
	}

	urlStr := fmt.Sprintf("https://%s%s", fs.host, apiPath)
	if len(query) > 0 {
		urlStr = urlStr + "?" + query.Encode()
	}

	var lastErr error
	maxRetries := 3

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			sleepDur := time.Duration(1<<attempt) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(sleepDur):
			}
		}

		req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
		if err != nil {
			return nil, g.Error(err, "could not build request")
		}

		req.Header.Set("Authorization", "Bearer "+fs.token)
		if method == http.MethodPut {
			req.Header.Set("Content-Type", "application/octet-stream")
		}

		resp, err := fs.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = fmt.Errorf("server error %s (%d)", resp.Status, resp.StatusCode)
			continue
		}

		return resp, nil
	}

	return nil, g.Error(lastErr, "request failed after %d retries for %s %s", maxRetries, method, urlStr)
}

type countingReader struct {
	reader io.Reader
	count  int64
}

func (cr *countingReader) Read(p []byte) (n int, err error) {
	n, err = cr.reader.Read(p)
	cr.count += int64(n)
	return n, err
}

// Write writes a stream directly to a Databricks volume using the Files REST API
func (fs *DatabricksVolumeFileSysClient) Write(uri string, reader io.Reader) (bw int64, err error) {
	volumePath, err := fs.GetPath(uri)
	if err != nil {
		return 0, err
	}

	apiPath := "/api/2.0/fs/files" + volumePath
	query := url.Values{}
	query.Set("overwrite", "true")

	// Track bytes written
	cr := &countingReader{reader: reader}

	resp, err := fs.doRequest(fs.Context().Ctx, http.MethodPut, apiPath, cr, query)
	if err != nil {
		return cr.count, g.Error(err, "failed to PUT file to Databricks Volume: %s", volumePath)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBytes, _ := io.ReadAll(resp.Body)
		return cr.count, g.Error("error writing to Databricks Volume %s (status %d): %s", volumePath, resp.StatusCode, string(respBytes))
	}

	return cr.count, nil
}

// GetReader returns a reader for reading a file from Databricks volume
func (fs *DatabricksVolumeFileSysClient) GetReader(uri string) (reader io.Reader, err error) {
	volumePath, err := fs.GetPath(uri)
	if err != nil {
		return nil, err
	}

	apiPath := "/api/2.0/fs/files" + volumePath
	resp, err := fs.doRequest(fs.Context().Ctx, http.MethodGet, apiPath, nil, nil)
	if err != nil {
		return nil, g.Error(err, "failed to GET file from Databricks Volume: %s", volumePath)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, g.Error("file not found in Databricks Volume: %s", volumePath)
	} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, g.Error("error reading from Databricks Volume %s (status %d): %s", volumePath, resp.StatusCode, string(respBytes))
	}

	return resp.Body, nil
}

// delete deletes a file or directory in a Databricks volume
func (fs *DatabricksVolumeFileSysClient) delete(uri string) (err error) {
	volumePath, err := fs.GetPath(uri)
	if err != nil {
		return err
	}

	apiPath := "/api/2.0/fs/files" + volumePath
	resp, err := fs.doRequest(fs.Context().Ctx, http.MethodDelete, apiPath, nil, nil)
	if err != nil {
		return g.Error(err, "failed to DELETE file in Databricks Volume: %s", volumePath)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBytes, _ := io.ReadAll(resp.Body)
		return g.Error("error deleting Databricks Volume file %s (status %d): %s", volumePath, resp.StatusCode, string(respBytes))
	}

	return nil
}

// MkdirAll creates a directory in a Databricks volume
func (fs *DatabricksVolumeFileSysClient) MkdirAll(uri string) (err error) {
	volumePath, err := fs.GetPath(uri)
	if err != nil {
		return err
	}

	apiPath := "/api/2.0/fs/directories" + volumePath
	resp, err := fs.doRequest(fs.Context().Ctx, http.MethodPut, apiPath, bytes.NewReader([]byte{}), nil)
	if err != nil {
		return g.Error(err, "failed to create directory in Databricks Volume: %s", volumePath)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBytes, _ := io.ReadAll(resp.Body)
		return g.Error("error creating Databricks Volume directory %s (status %d): %s", volumePath, resp.StatusCode, string(respBytes))
	}

	return nil
}

// List lists objects in a Databricks volume path
func (fs *DatabricksVolumeFileSysClient) List(uri string) (nodes FileNodes, err error) {
	return fs.doList(uri, false)
}

// ListRecursive lists objects in a Databricks volume path recursively
func (fs *DatabricksVolumeFileSysClient) ListRecursive(uri string) (nodes FileNodes, err error) {
	return fs.doList(uri, true)
}

func (fs *DatabricksVolumeFileSysClient) doList(uri string, recursive bool) (nodes FileNodes, err error) {
	volumePath, err := fs.GetPath(uri)
	if err != nil {
		return nil, err
	}

	pattern, err := makeGlob(NormalizeURI(fs, uri))
	if err != nil {
		return nil, g.Error(err, "error parsing glob pattern: %s", uri)
	}

	nodes, err = fs.listDirRecursive(volumePath, recursive)
	if err != nil {
		return nil, err
	}

	if pattern != nil {
		filtered := FileNodes{}
		for _, n := range nodes {
			if (*pattern).Match(n.URI) {
				filtered = append(filtered, n)
			}
		}
		return filtered, nil
	}

	return nodes, nil
}

func (fs *DatabricksVolumeFileSysClient) listDirRecursive(volumePath string, recursive bool) (nodes FileNodes, err error) {
	apiPath := "/api/2.0/fs/directories" + strings.TrimRight(volumePath, "/")
	query := url.Values{}

	pageToken := ""
	for {
		if pageToken != "" {
			query.Set("page_token", pageToken)
		}

		resp, err := fs.doRequest(fs.Context().Ctx, http.MethodGet, apiPath, nil, query)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode == http.StatusNotFound {
			resp.Body.Close()
			return nodes, nil
		} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			respBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, g.Error("error listing directory %s (status %d): %s", volumePath, resp.StatusCode, string(respBytes))
		}

		var dirResp databricksDirListResp
		err = json.NewDecoder(resp.Body).Decode(&dirResp)
		resp.Body.Close()
		if err != nil {
			return nil, g.Error(err, "error decoding Databricks directory response")
		}

		for _, item := range dirResp.Contents {
			nodeURI := fmt.Sprintf("databricks-volume://%s", strings.TrimPrefix(item.Path, "/"))
			node := FileNode{
				URI:     nodeURI,
				IsDir:   item.IsDirectory,
				Size:    cast.ToUint64(item.FileSize),
				Updated: item.LastModified / 1000,
			}
			nodes = append(nodes, node)

			if recursive && item.IsDirectory {
				subNodes, err := fs.listDirRecursive(item.Path, true)
				if err != nil {
					return nil, err
				}
				nodes = append(nodes, subNodes...)
			}
		}

		if dirResp.NextPageToken == "" {
			break
		}
		pageToken = dirResp.NextPageToken
	}

	return nodes, nil
}
