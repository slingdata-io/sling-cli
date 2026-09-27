package filesys

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newVolumeTestClient(t *testing.T, props map[string]string) *DatabricksVolumeFileSysClient {
	t.Helper()
	client := &DatabricksVolumeFileSysClient{}
	if props != nil {
		client.properties = props
	} else {
		client.properties = map[string]string{
			"host":  "adb-test.cloud.databricks.com",
			"token": "dapi_test",
		}
	}
	require.NoError(t, client.Init(context.Background()))
	return client
}

func TestDatabricksVolume_URLParsing(t *testing.T) {
	tests := []struct {
		url          string
		expectedPath string
		wantErr      bool
	}{
		{
			url:          "databricks-volume://my_cat/my_schema/my_vol/folder/data.parquet",
			expectedPath: "/Volumes/my_cat/my_schema/my_vol/folder/data.parquet",
		},
		{
			url:          "databricks://Volumes/my_cat/my_schema/my_vol/folder/data.parquet",
			expectedPath: "/Volumes/my_cat/my_schema/my_vol/folder/data.parquet",
		},
		{
			url:          "databricks-volume://adb-123.azuredatabricks.net/Volumes/my_cat/my_schema/my_vol/test.csv",
			expectedPath: "/Volumes/my_cat/my_schema/my_vol/test.csv",
		},
		{
			url:          "databricks-volume://custom.dns.example/Volumes/my_cat/my_schema/my_vol/test.csv",
			expectedPath: "/Volumes/my_cat/my_schema/my_vol/test.csv",
		},
		{
			url:     "databricks-volume://only_cat",
			wantErr: true,
		},
	}

	client := newVolumeTestClient(t, nil)

	for _, tt := range tests {
		t.Run(tt.url, func(t *testing.T) {
			path, err := client.GetPath(tt.url)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedPath, path)
		})
	}
}

func TestDatabricksVolume_ParseURLTypeSQLGuard(t *testing.T) {
	uType, _, _, err := ParseURLType("databricks://token:x@host.cloud.databricks.com/sql/1.0/warehouses/abc")
	assert.NoError(t, err)
	assert.NotEqual(t, dbio.TypeFileDatabricksVolume, uType)
	assert.Equal(t, dbio.TypeDbDatabricks, uType)

	uType, host, _, err := ParseURLType("databricks://Volumes/my_cat/my_schema/my_vol/file.parquet")
	assert.NoError(t, err)
	assert.Equal(t, dbio.TypeFileDatabricksVolume, uType)
	assert.Equal(t, "Volumes", host)

	uType, _, _, err = ParseURLType("databricks-volume://my_cat/my_schema/my_vol/file.parquet")
	assert.NoError(t, err)
	assert.Equal(t, dbio.TypeFileDatabricksVolume, uType)

	_, _, _, err = ParseURLType("volume://my_cat/my_schema/my_vol/file.parquet")
	assert.Error(t, err)
}

func TestDatabricksVolume_InitRequiresHostToken(t *testing.T) {
	client := &DatabricksVolumeFileSysClient{}
	client.properties = map[string]string{"catalog": "c", "schema": "s", "volume": "v"}
	err := client.Init(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "host is required")

	client = &DatabricksVolumeFileSysClient{}
	client.properties = map[string]string{"host": "adb.example.com"}
	err = client.Init(context.Background())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "token is required")
}

func TestDatabricksVolume_InitHostFromURL(t *testing.T) {
	client := &DatabricksVolumeFileSysClient{}
	client.properties = map[string]string{
		"token": "tok",
		"url":   "databricks-volume://adb-123.azuredatabricks.net/Volumes/my_cat/my_schema/my_vol/file.csv",
	}
	require.NoError(t, client.Init(context.Background()))
	assert.Equal(t, "adb-123.azuredatabricks.net", client.host)
	assert.Equal(t, "my_cat", client.catalog)
	assert.Equal(t, "my_schema", client.schema)
	assert.Equal(t, "my_vol", client.volume)
}

func TestDatabricksVolume_RESTOperations(t *testing.T) {
	var receivedPutBody []byte
	var receivedAuthHeader string
	var receivedMethod string
	var receivedURLPath string
	var receivedQuery string
	var listPages int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuthHeader = r.Header.Get("Authorization")
		receivedMethod = r.Method
		receivedURLPath = r.URL.Path
		receivedQuery = r.URL.RawQuery

		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/api/2.0/fs/files/"):
			b, _ := io.ReadAll(r.Body)
			receivedPutBody = b
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/2.0/fs/files/"):
			if strings.Contains(r.URL.Path, "missing") {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("mock volume content"))

		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/2.0/fs/files/"):
			if strings.Contains(r.URL.Path, "gone") {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/2.0/fs/directories/"):
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/2.0/fs/directories/"):
			listPages++
			if r.URL.Query().Get("page_token") == "" {
				resp := databricksDirListResp{
					Contents: []databricksFileInfo{
						{
							Path:         "/Volumes/my_cat/my_schema/my_vol/file1.parquet",
							IsDirectory:  false,
							FileSize:     1024,
							LastModified: 1700000000000,
						},
					},
					NextPageToken: "page-2",
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(resp)
				return
			}
			resp := databricksDirListResp{
				Contents: []databricksFileInfo{
					{
						Path:        "/Volumes/my_cat/my_schema/my_vol/file2.parquet",
						IsDirectory: false,
						FileSize:    2048,
					},
				},
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(resp)

		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")

	client := newVolumeTestClient(t, map[string]string{
		"host":     host,
		"protocol": "http",
		"token":    "dapi_test_token_123",
		"catalog":  "my_cat",
		"schema":   "my_schema",
		"volume":   "my_vol",
	})

	testContent := "column1,column2\nval1,val2\n"
	bw, err := client.Write("databricks-volume://my_cat/my_schema/my_vol/data.csv", strings.NewReader(testContent))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(testContent)), bw)
	assert.Equal(t, "Bearer dapi_test_token_123", receivedAuthHeader)
	assert.Equal(t, http.MethodPut, receivedMethod)
	assert.Equal(t, "/api/2.0/fs/files/Volumes/my_cat/my_schema/my_vol/data.csv", receivedURLPath)
	assert.Contains(t, receivedQuery, "overwrite=true")
	assert.Equal(t, testContent, string(receivedPutBody))

	reader, err := client.GetReader("databricks-volume://my_cat/my_schema/my_vol/data.csv")
	if assert.NoError(t, err) && reader != nil {
		readBytes, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, "mock volume content", string(readBytes))
	}

	_, err = client.GetReader("databricks-volume://my_cat/my_schema/my_vol/missing.csv")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "file not found")

	nodes, err := client.List("databricks-volume://my_cat/my_schema/my_vol")
	assert.NoError(t, err)
	assert.Len(t, nodes, 2)
	assert.Equal(t, "databricks-volume://my_cat/my_schema/my_vol/file1.parquet", nodes[0].URI)
	assert.Equal(t, uint64(1024), nodes[0].Size)
	assert.Equal(t, int64(1700000000), nodes[0].Updated)
	assert.Equal(t, "databricks-volume://my_cat/my_schema/my_vol/file2.parquet", nodes[1].URI)
	assert.Equal(t, 2, listPages)

	err = client.delete("databricks-volume://my_cat/my_schema/my_vol/data.csv")
	assert.NoError(t, err)
	assert.Equal(t, http.MethodDelete, receivedMethod)

	err = client.delete("databricks-volume://my_cat/my_schema/my_vol/gone.csv")
	assert.NoError(t, err)
}

func TestDatabricksVolume_DeleteDirectory(t *testing.T) {
	var methods []string
	var paths []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method+" "+r.URL.Path)
		paths = append(paths, r.URL.Path)

		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/2.0/fs/directories/"):
			if strings.HasSuffix(r.URL.Path, "/my_vol/folder") {
				resp := databricksDirListResp{
					Contents: []databricksFileInfo{
						{Path: "/Volumes/my_cat/my_schema/my_vol/folder/a.csv", IsDirectory: false, FileSize: 10},
					},
				}
				json.NewEncoder(w).Encode(resp)
				return
			}
			json.NewEncoder(w).Encode(databricksDirListResp{})
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	client := newVolumeTestClient(t, map[string]string{
		"host":     host,
		"protocol": "http",
		"token":    "tok",
		"catalog":  "my_cat",
		"schema":   "my_schema",
		"volume":   "my_vol",
	})

	err := client.delete("databricks-volume://my_cat/my_schema/my_vol/folder/")
	assert.NoError(t, err)

	joined := strings.Join(methods, "\n")
	assert.Contains(t, joined, "DELETE /api/2.0/fs/directories/Volumes/my_cat/my_schema/my_vol/folder")
	assert.Contains(t, joined, "DELETE /api/2.0/fs/files/Volumes/my_cat/my_schema/my_vol/folder/a.csv")
}

func TestDatabricksVolume_RetrySeekableVsNonSeekable(t *testing.T) {
	t.Run("seekable retried on 500", func(t *testing.T) {
		var puts int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				w.WriteHeader(http.StatusOK)
				return
			}
			n := atomic.AddInt32(&puts, 1)
			io.ReadAll(r.Body)
			if n == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		host := strings.TrimPrefix(server.URL, "http://")
		client := newVolumeTestClient(t, map[string]string{
			"host":     host,
			"protocol": "http",
			"token":    "tok",
			"catalog":  "my_cat",
			"schema":   "my_schema",
			"volume":   "my_vol",
		})

		_, err := client.Write("databricks-volume://my_cat/my_schema/my_vol/ok.csv", strings.NewReader("hello"))
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, atomic.LoadInt32(&puts), int32(2))
	})

	t.Run("non-seekable not retried", func(t *testing.T) {
		var puts int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				w.WriteHeader(http.StatusOK)
				return
			}
			atomic.AddInt32(&puts, 1)
			io.ReadAll(r.Body)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		host := strings.TrimPrefix(server.URL, "http://")
		client := newVolumeTestClient(t, map[string]string{
			"host":     host,
			"protocol": "http",
			"token":    "tok",
			"catalog":  "my_cat",
			"schema":   "my_schema",
			"volume":   "my_vol",
		})

		pr, pw := io.Pipe()
		go func() {
			pw.Write([]byte("streamed-bytes"))
			pw.Close()
		}()

		_, err := client.Write("databricks-volume://my_cat/my_schema/my_vol/pipe.csv", pr)
		assert.Error(t, err)
		assert.Equal(t, int32(1), atomic.LoadInt32(&puts))
	})
}

func TestDatabricksVolume_Prefix(t *testing.T) {
	client := newVolumeTestClient(t, map[string]string{
		"host":    "adb.example.com",
		"token":   "tok",
		"catalog": "my_cat",
		"schema":  "my_schema",
		"volume":  "my_vol",
	})
	assert.Equal(t, "databricks-volume://my_cat/my_schema/my_vol/", client.Prefix())
}

// Live Files REST check. Not in the default suite — skip unless:
//
//	DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_VOLUME (catalog.schema.volume)
func TestDatabricksVolume_Live(t *testing.T) {
	host := os.Getenv("DATABRICKS_HOST")
	token := os.Getenv("DATABRICKS_TOKEN")
	vol := os.Getenv("DATABRICKS_VOLUME")
	if host == "" || token == "" || vol == "" {
		t.Skip("DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_VOLUME not set")
	}

	parts := strings.Split(vol, ".")
	require.Len(t, parts, 3, "DATABRICKS_VOLUME must be catalog.schema.volume")
	catalog, schema, volume := parts[0], parts[1], parts[2]

	client := newVolumeTestClient(t, map[string]string{
		"host":    host,
		"token":   token,
		"catalog": catalog,
		"schema":  schema,
		"volume":  volume,
	})

	ts := time.Now().UTC().Format("20060102T150405Z")
	dirURI := fmt.Sprintf("databricks-volume://%s/%s/%s/sling_test/%s", catalog, schema, volume, ts)
	fileURI := dirURI + "/hello.txt"
	body := "sling volume live test\n"

	bw, err := client.Write(fileURI, strings.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, int64(len(body)), bw)

	t.Cleanup(func() {
		_ = client.delete(dirURI + "/")
	})

	reader, err := client.GetReader(fileURI)
	require.NoError(t, err)
	got, err := io.ReadAll(reader)
	if rc, ok := reader.(io.Closer); ok {
		rc.Close()
	}
	require.NoError(t, err)
	assert.Equal(t, body, string(got))

	nodes, err := client.List(dirURI)
	require.NoError(t, err)
	require.NotEmpty(t, nodes)
	assert.Equal(t, fileURI, nodes[0].URI)

	aliasURI := fmt.Sprintf("databricks://Volumes/%s/%s/%s/sling_test/%s/alias.txt", catalog, schema, volume, ts)
	_, err = client.Write(aliasURI, strings.NewReader("alias\n"))
	require.NoError(t, err)

	err = client.delete(fileURI)
	require.NoError(t, err)
	_, err = client.GetReader(fileURI)
	assert.Error(t, err)
}
