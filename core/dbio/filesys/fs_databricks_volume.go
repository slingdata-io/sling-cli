package filesys

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"
)

// Files API single PUT is capped at 5 GiB.
// https://docs.databricks.com/api/workspace/files
const databricksVolumeMaxPutBytes int64 = 5 * 1024 * 1024 * 1024

const databricksVolumeRetryBuffer = 8 * 1024 * 1024

// DatabricksVolumeFileSysClient handles Databricks Unity Catalog Volumes via Files REST API
// API Reference: https://docs.databricks.com/api/workspace/files
// Canonical URI: databricks-volume://<catalog>/<schema>/<volume>/<path>
// Alias:         databricks://Volumes/<catalog>/<schema>/<volume>/<path>
type DatabricksVolumeFileSysClient struct {
	BaseFileSysClient
	client     *http.Client
	scheme     string
	host       string
	token      string
	catalog    string
	schema     string
	volume     string
	basePrefix string
}

type databricksDirListResp struct {
	Contents      []databricksFileInfo `json:"contents"`
	NextPageToken string               `json:"next_page_token"`
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
	fs.BaseFileSysClient.fsType = dbio.TypeFileDatabricksVolume

	rawHost := fs.GetProp("host")
	if rawHost == "" {
		rawHost = fs.GetProp("DATABRICKS_HOST")
	}
	if rawHost == "" {
		rawHost = os.Getenv("DATABRICKS_HOST")
	}
	fs.token = fs.GetProp("token")
	if fs.token == "" {
		fs.token = fs.GetProp("DATABRICKS_TOKEN")
	}
	if fs.token == "" {
		fs.token = os.Getenv("DATABRICKS_TOKEN")
	}

	fs.scheme = "https"
	if strings.HasPrefix(rawHost, "http://") || fs.GetProp("protocol") == "http" || fs.GetProp("use_ssl") == "false" || fs.GetProp("ssl") == "false" {
		fs.scheme = "http"
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

	if rawHost == "" {
		rawHost = fs.host
	}
	fs.host = strings.TrimPrefix(rawHost, "https://")
	fs.host = strings.TrimPrefix(fs.host, "http://")
	fs.host = strings.TrimRight(fs.host, "/")

	if fs.host == "" {
		return g.Error("databricks host is required")
	}
	if fs.token == "" {
		return g.Error("databricks token is required")
	}

	fs.client = &http.Client{
		Timeout: 30 * time.Minute,
		Transport: &http.Transport{
			ResponseHeaderTimeout: 60 * time.Second,
			IdleConnTimeout:       90 * time.Second,
		},
	}

	return nil
}

// FsType returns dbio.TypeFileDatabricksVolume
func (fs *DatabricksVolumeFileSysClient) FsType() dbio.Type {
	return dbio.TypeFileDatabricksVolume
}

func looksLikeHost(s string) bool {
	return strings.Contains(s, ".")
}

func stripScheme(rawURL string) string {
	cleaned := rawURL
	for _, prefix := range []string{"databricks-volume://", "databricks://"} {
		if strings.HasPrefix(cleaned, prefix) {
			cleaned = strings.TrimPrefix(cleaned, prefix)
			break
		}
	}
	return cleaned
}

func (fs *DatabricksVolumeFileSysClient) parseURL(rawURL string) {
	// e.g. databricks-volume://catalog/schema/volume/path
	// or databricks-volume://host/Volumes/catalog/schema/volume/path
	// or databricks://Volumes/catalog/schema/volume/path
	cleaned := stripScheme(rawURL)

	parts := strings.Split(strings.Trim(cleaned, "/"), "/")
	if len(parts) > 0 && parts[0] == "Volumes" {
		parts = parts[1:]
	}

	if len(parts) > 0 && looksLikeHost(parts[0]) {
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
		prefix = fmt.Sprintf("databricks-volume://%s/%s/%s", fs.catalog, fs.schema, fs.volume)
	} else {
		prefix = "databricks-volume://"
	}
	s := strings.TrimLeft(strings.Join(suffix, ""), "/")
	if s != "" {
		return strings.TrimRight(prefix, "/") + "/" + s
	}
	return strings.TrimRight(prefix, "/") + "/"
}

// stripDatabricksVolumePrefix returns the path inside the volume (after catalog/schema/volume).
func stripDatabricksVolumePrefix(host, path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	var cleaned []string
	for _, p := range parts {
		if p != "" {
			cleaned = append(cleaned, p)
		}
	}
	if strings.EqualFold(host, "Volumes") {
		if len(cleaned) >= 3 {
			return strings.Join(cleaned[3:], "/")
		}
		return ""
	}
	if len(cleaned) >= 2 {
		return strings.Join(cleaned[2:], "/")
	}
	return strings.Join(cleaned, "/")
}

func volumeURIFromAPIPath(apiPath string) string {
	p := strings.TrimPrefix(apiPath, "/")
	p = strings.TrimPrefix(p, "Volumes/")
	return "databricks-volume://" + p
}

// GetPath converts a uri into the internal volume path starting with /Volumes/...
func (fs *DatabricksVolumeFileSysClient) GetPath(uri string) (volumePath string, err error) {
	uri = NormalizeURI(fs, uri)

	clean := stripScheme(uri)

	rawParts := strings.Split(strings.Trim(clean, "/"), "/")
	var parts []string
	for _, p := range rawParts {
		if p != "" {
			parts = append(parts, p)
		}
	}

	if len(parts) > 0 && looksLikeHost(parts[0]) {
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
	if len(subparts) > 0 {
		p = p + "/" + strings.Join(subparts, "/")
	}
	return p, nil
}

type retryableBody struct {
	reader    io.Reader
	retryable bool
}

func prepareRequestBody(body io.Reader) retryableBody {
	if body == nil {
		return retryableBody{retryable: true}
	}
	if cr, ok := body.(*countingReader); ok {
		if _, seekable := cr.reader.(io.Seeker); seekable {
			return retryableBody{reader: cr, retryable: true}
		}
		return retryableBody{reader: cr, retryable: false}
	}
	if _, ok := body.(io.Seeker); ok {
		return retryableBody{reader: body, retryable: true}
	}

	buf, err := io.ReadAll(io.LimitReader(body, databricksVolumeRetryBuffer+1))
	if err != nil {
		return retryableBody{reader: io.MultiReader(bytes.NewReader(buf), body), retryable: false}
	}
	if int64(len(buf)) <= databricksVolumeRetryBuffer {
		return retryableBody{reader: bytes.NewReader(buf), retryable: true}
	}
	return retryableBody{reader: io.MultiReader(bytes.NewReader(buf), body), retryable: false}
}

// doRequest executes an HTTP request against Databricks Files REST API with retries
func (fs *DatabricksVolumeFileSysClient) doRequest(ctx context.Context, method, apiPath string, body io.Reader, query url.Values) (*http.Response, error) {
	if fs.host == "" {
		return nil, g.Error("databricks host is required")
	}
	if fs.token == "" {
		return nil, g.Error("databricks token is required")
	}

	urlStr := fmt.Sprintf("%s://%s%s", fs.scheme, fs.host, apiPath)
	if len(query) > 0 {
		urlStr = urlStr + "?" + query.Encode()
	}

	prepared := prepareRequestBody(body)

	var lastErr error
	maxRetries := 3

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			if !prepared.retryable {
				return nil, g.Error(lastErr, "request failed for %s %s", method, urlStr)
			}
			if seeker, ok := prepared.reader.(io.Seeker); ok {
				if _, err := seeker.Seek(0, io.SeekStart); err != nil {
					return nil, g.Error(err, "could not rewind request body")
				}
			}
			sleepDur := time.Duration(1<<attempt) * 500 * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(sleepDur):
			}
		}

		req, err := http.NewRequestWithContext(ctx, method, urlStr, prepared.reader)
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
			if !prepared.retryable {
				return nil, g.Error(err, "request failed for %s %s", method, urlStr)
			}
			continue
		}

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = fmt.Errorf("server error %s (%d)", resp.Status, resp.StatusCode)
			if !prepared.retryable {
				return nil, g.Error(lastErr, "request failed for %s %s", method, urlStr)
			}
			continue
		}

		return resp, nil
	}

	return nil, g.Error(lastErr, "request failed after %d retries for %s %s", maxRetries, method, urlStr)
}

type countingReader struct {
	reader io.Reader
	count  int64
	limit  int64
}

func (cr *countingReader) Read(p []byte) (n int, err error) {
	n, err = cr.reader.Read(p)
	cr.count += int64(n)
	if cr.limit > 0 && cr.count > cr.limit {
		return n, g.Error("Databricks Volume PUT exceeds the 5 GiB single-request limit (%d bytes). Split the file (file_max_bytes) or use a smaller payload", cr.count)
	}
	return n, err
}

func (cr *countingReader) Seek(offset int64, whence int) (int64, error) {
	seeker, ok := cr.reader.(io.Seeker)
	if !ok {
		return 0, fmt.Errorf("body is not seekable")
	}
	n, err := seeker.Seek(offset, whence)
	if err == nil && whence == io.SeekStart && offset == 0 {
		cr.count = 0
	}
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

	cr := &countingReader{reader: reader, limit: databricksVolumeMaxPutBytes}

	resp, err := fs.doRequest(fs.Context().Ctx, http.MethodPut, apiPath, cr, query)
	if err != nil {
		return 0, g.Error(err, "failed to PUT file to Databricks Volume: %s", volumePath)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBytes, _ := io.ReadAll(resp.Body)
		return 0, g.Error("error writing to Databricks Volume %s (status %d): %s", volumePath, resp.StatusCode, string(respBytes))
	}

	return cr.count, nil
}

// GetWriter pipes into Write.
func (fs *DatabricksVolumeFileSysClient) GetWriter(uri string) (writer io.Writer, err error) {
	pipeR, pipeW := io.Pipe()
	fs.Context().Wg.Write.Add()
	go func() {
		defer fs.Context().Wg.Write.Done()
		_, werr := fs.Write(uri, pipeR)
		pipeR.CloseWithError(werr)
	}()
	return pipeW, nil
}

// Buckets returns the configured volume as the single "bucket".
func (fs *DatabricksVolumeFileSysClient) Buckets() (paths []string, err error) {
	if fs.catalog != "" && fs.schema != "" && fs.volume != "" {
		return []string{fmt.Sprintf("%s/%s/%s", fs.catalog, fs.schema, fs.volume)}, nil
	}
	return
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

func (fs *DatabricksVolumeFileSysClient) isDirectory(volumePath string) bool {
	apiPath := "/api/2.0/fs/directories" + strings.TrimRight(volumePath, "/")
	resp, err := fs.doRequest(fs.Context().Ctx, http.MethodGet, apiPath, nil, nil)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func (fs *DatabricksVolumeFileSysClient) deleteFile(volumePath string) error {
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

func (fs *DatabricksVolumeFileSysClient) deleteEmptyDir(volumePath string) error {
	apiPath := "/api/2.0/fs/directories" + strings.TrimRight(volumePath, "/")
	resp, err := fs.doRequest(fs.Context().Ctx, http.MethodDelete, apiPath, nil, nil)
	if err != nil {
		return g.Error(err, "failed to DELETE directory in Databricks Volume: %s", volumePath)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBytes, _ := io.ReadAll(resp.Body)
		return g.Error("error deleting Databricks Volume directory %s (status %d): %s", volumePath, resp.StatusCode, string(respBytes))
	}
	return nil
}

func (fs *DatabricksVolumeFileSysClient) deleteRecursive(volumePath string) error {
	nodes, err := fs.listDirRecursive(volumePath, false)
	if err != nil {
		return err
	}

	// files first, then directories (API requires empty dirs)
	for _, n := range nodes {
		if n.IsDir {
			continue
		}
		childPath, err := fs.GetPath(n.URI)
		if err != nil {
			return err
		}
		if err := fs.deleteFile(childPath); err != nil {
			return err
		}
	}
	for _, n := range nodes {
		if !n.IsDir {
			continue
		}
		childPath, err := fs.GetPath(n.URI)
		if err != nil {
			return err
		}
		if err := fs.deleteRecursive(childPath); err != nil {
			return err
		}
	}
	return fs.deleteEmptyDir(volumePath)
}

// delete deletes a file or directory in a Databricks volume
func (fs *DatabricksVolumeFileSysClient) delete(uri string) (err error) {
	volumePath, err := fs.GetPath(uri)
	if err != nil {
		return err
	}

	if strings.HasSuffix(uri, "/") || fs.isDirectory(volumePath) {
		return fs.deleteRecursive(volumePath)
	}
	return fs.deleteFile(volumePath)
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

	listed, err := fs.listDirRecursive(volumePath, recursive)
	if err != nil {
		return nil, err
	}

	ts := fs.GetRefTs().Unix()
	nodes.AddWhere(pattern, ts, listed...)
	return nodes, nil
}

func (fs *DatabricksVolumeFileSysClient) listDirRecursive(volumePath string, recursive bool) (nodes FileNodes, err error) {
	apiPath := "/api/2.0/fs/directories" + strings.TrimRight(volumePath, "/")

	pageToken := ""
	for {
		query := url.Values{}
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
			node := FileNode{
				URI:     volumeURIFromAPIPath(item.Path),
				IsDir:   item.IsDirectory,
				Size:    cast.ToUint64(item.FileSize),
				Updated: item.LastModified / 1000, // Files API last_modified is epoch ms
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
