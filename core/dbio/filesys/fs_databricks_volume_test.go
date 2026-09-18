package filesys

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDatabricksVolume_URLParsing(t *testing.T) {
	tests := []struct {
		url          string
		expectedPath string
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
			url:          "volume://my_cat/my_schema/my_vol/data.parquet",
			expectedPath: "/Volumes/my_cat/my_schema/my_vol/data.parquet",
		},
		{
			url:          "databricks-volume://adb-123.azuredatabricks.net/Volumes/my_cat/my_schema/my_vol/test.csv",
			expectedPath: "/Volumes/my_cat/my_schema/my_vol/test.csv",
		},
	}

	for _, tt := range tests {
		client := &DatabricksVolumeFileSysClient{}
		err := client.Init(context.Background())
		assert.NoError(t, err)

		path, err := client.GetPath(tt.url)
		assert.NoError(t, err)
		assert.Equal(t, tt.expectedPath, path)
	}
}

func TestDatabricksVolume_RESTOperations(t *testing.T) {
	var receivedPutBody []byte
	var receivedAuthHeader string
	var receivedMethod string
	var receivedURLPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuthHeader = r.Header.Get("Authorization")
		receivedMethod = r.Method
		receivedURLPath = r.URL.Path

		switch {
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/api/2.0/fs/files/"):
			b, _ := io.ReadAll(r.Body)
			receivedPutBody = b
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/2.0/fs/files/"):
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("mock volume content"))

		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/2.0/fs/files/"):
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/2.0/fs/directories/"):
			resp := databricksDirListResp{
				Contents: []databricksFileInfo{
					{
						Path:        "/Volumes/my_cat/my_schema/my_vol/file1.parquet",
						IsDirectory: false,
						FileSize:    1024,
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

	client := &DatabricksVolumeFileSysClient{}
	client.properties = map[string]string{
		"host":     host,
		"protocol": "http",
		"token":    "dapi_test_token_123",
		"catalog":  "my_cat",
		"schema":   "my_schema",
		"volume":   "my_vol",
	}

	err := client.Init(context.Background())
	assert.NoError(t, err)

	// 1. Test Streaming Write (PUT)
	testContent := "column1,column2\nval1,val2\n"
	bw, err := client.Write("databricks-volume://my_cat/my_schema/my_vol/data.csv", strings.NewReader(testContent))
	assert.NoError(t, err)
	assert.Equal(t, int64(len(testContent)), bw)
	assert.Equal(t, "Bearer dapi_test_token_123", receivedAuthHeader)
	assert.Equal(t, http.MethodPut, receivedMethod)
	assert.Equal(t, "/api/2.0/fs/files/Volumes/my_cat/my_schema/my_vol/data.csv", receivedURLPath)
	assert.Equal(t, testContent, string(receivedPutBody))

	// 2. Test Read (GET)
	reader, err := client.GetReader("databricks-volume://my_cat/my_schema/my_vol/data.csv")
	if assert.NoError(t, err) && reader != nil {
		readBytes, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, "mock volume content", string(readBytes))
	}

	// 3. Test List
	nodes, err := client.List("databricks-volume://my_cat/my_schema/my_vol")
	assert.NoError(t, err)
	assert.Len(t, nodes, 1)
	assert.Equal(t, "databricks-volume://Volumes/my_cat/my_schema/my_vol/file1.parquet", nodes[0].URI)
	assert.Equal(t, uint64(1024), nodes[0].Size)

	// 4. Test Delete
	err = client.delete("databricks-volume://my_cat/my_schema/my_vol/data.csv")
	assert.NoError(t, err)
	assert.Equal(t, http.MethodDelete, receivedMethod)
}
