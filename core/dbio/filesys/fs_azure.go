package filesys

import (
	"context"
	"io"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"
)

// AzureFileSysClient is a file system client to write file to Microsoft's Azure file sys.
type AzureFileSysClient struct {
	BaseFileSysClient
	client    *azblob.Client
	account   string
	container string
	key       string
}

// Init initializes the fs client
func (fs *AzureFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)
	for _, key := range g.ArrStr("ACCOUNT", "CONN_STR", "SAS_SVC_URL") {
		if fs.GetProp(key) == "" {
			fs.SetProp(key, fs.GetProp("AZURE_"+key))
		}
	}

	fs.account = fs.GetProp("account")
	fs.container = fs.GetProp("container")

	return fs.Connect()
}

// Prefix returns the url prefix
func (fs *AzureFileSysClient) Prefix(suffix ...string) string {
	return g.F("https://%s.blob.core.windows.net/%s", fs.account, fs.container) + strings.Join(suffix, "")
}

// GetPath returns the path of url
func (fs *AzureFileSysClient) GetPath(uri string) (path string, err error) {
	// normalize, in case url is provided without prefix
	uri = NormalizeURI(fs, uri)

	host, path, err := ParseURL(uri)
	if err != nil {
		return
	}

	pathContainer := strings.Split(path, "/")[0]
	if !strings.HasPrefix(host, fs.account) {
		err = g.Error("URL account differs from connection account. %s != %s.blob.core.windows.net", host, fs.account)
	} else if pathContainer != fs.container {
		err = g.Error("URL container differs from connection container. %s != %s", pathContainer, fs.container)
	}

	// remove container
	path = strings.TrimPrefix(path, pathContainer+"/")

	return path, err
}

// Connect initiates the fs client connection
func (fs *AzureFileSysClient) Connect() (err error) {

	serviceURL := g.F("https://%s.blob.core.windows.net/", fs.account)
	if cs := fs.GetProp("CONN_STR"); cs != "" {
		connProps := g.KVArrToMap(strings.Split(cs, ";")...)
		fs.account = connProps["AccountName"]
		fs.key = connProps["AccountKey"]

		fs.client, err = azblob.NewClientFromConnectionString(cs, &azblob.ClientOptions{})
		if err != nil {
			err = g.Error(err, "Could not connect to Azure using provided CONN_STR")
			return
		}
	} else if cs := fs.GetProp("SAS_SVC_URL"); cs != "" {
		csArr := strings.Split(cs, "?")
		if len(csArr) != 2 {
			err = g.Error("Invalid provided SAS_SVC_URL")
			return
		}

		fs.client, err = azblob.NewClientWithNoCredential(cs, &azblob.ClientOptions{})
		if err != nil {
			err = g.Error(err, "Could not connect to Azure using provided SAS_SVC_URL")
			return
		}
	} else if ak := fs.GetProp("ACCOUNT_KEY"); ak != "" {
		cred, err := azblob.NewSharedKeyCredential(fs.account, ak)
		if err != nil {
			return g.Error(err, "Could not process shared key / account key")
		}

		fs.client, err = azblob.NewClientWithSharedKeyCredential(serviceURL, cred, &azblob.ClientOptions{})
		if err != nil {
			return g.Error(err, "Could not connect to Azure using shared key credentials")
		}
	} else {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return g.Error(err, "No Azure credentials provided")
		}

		fs.client, err = azblob.NewClient(serviceURL, cred, &azblob.ClientOptions{})
		if err != nil {
			return g.Error(err, "Could not connect to Azure using default credentials")
		}
	}
	return
}

// Buckets returns the containers found in the project
func (fs *AzureFileSysClient) Buckets() (paths []string, err error) {
	pager := fs.client.NewListContainersPager(&service.ListContainersOptions{})

	for pager.More() {
		// advance to the next page
		page, err := pager.NextPage(fs.Context().Ctx)
		if err != nil {
			err = g.Error(err, "Could not get list containers")
			return paths, err
		}

		for _, item := range page.ContainerItems {
			paths = append(paths, *item.Name)
		}
	}

	return
}

// List list objects in path
func (fs *AzureFileSysClient) List(uri string) (nodes dbio.FileNodes, err error) {
	key, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	baseKeys := map[string]int{}
	keyArr := strings.Split(key, "/")
	counter := 0
	maxItems := lo.Ternary(recursiveLimit == 0, 10000, recursiveLimit)

	pagerOpts := &container.ListBlobsFlatOptions{}
	if key != "" {
		pagerOpts.Prefix = g.String(key)
	}

	pager := fs.client.NewListBlobsFlatPager(fs.container, pagerOpts)

	// continue fetching pages until no more remain
	for pager.More() {
		// advance to the next page
		page, err := pager.NextPage(fs.Context().Ctx)
		if err != nil {
			err = g.Error(err, "Could not get list blob for: "+uri)
			return nodes, err
		}

		// print the blob names for this page
		for _, blob := range page.Segment.BlobItems {
			blobName := *blob.Name

			counter++
			if counter >= maxItems {
				g.Warn("Azure Storage returns results recursively by default. Limiting list results at %d items. Set SLING_RECURSIVE_LIMIT to increase.", maxItems)
				break
			} else if !strings.HasPrefix(blobName, key) {
				// needs to have correct key, since it's recursive
				continue
			}

			parts := strings.Split(strings.TrimSuffix(blobName, "/"), "/")
			baseKey := strings.Join(parts[:len(keyArr)], "/")
			baseKeys[baseKey]++

			if baseKeys[baseKey] == 1 {
				node := dbio.FileNode{
					URI:   g.F("%s%s", fs.Prefix("/"), baseKey),
					IsDir: len(parts) >= len(keyArr)+1,
				}

				if baseKey == strings.TrimSuffix(blobName, "/") {
					node.Size = cast.ToUint64(blob.Properties.ContentLength)
					node.Created = blob.Properties.CreationTime.Unix()
					node.Updated = blob.Properties.LastModified.Unix()
					node.IsDir = strings.HasSuffix(blobName, "/")
				}
				nodes.Add(node)
			}
		}
	}

	return
}

// ListRecursive list objects in path
func (fs *AzureFileSysClient) ListRecursive(uri string) (nodes dbio.FileNodes, err error) {
	key, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	pattern, err := makeGlob(NormalizeURI(fs, uri))
	if err != nil {
		err = g.Error(err, "Error Parsing url pattern: "+uri)
		return
	}

	pagerOpts := &container.ListBlobsFlatOptions{}
	if key != "" {
		pagerOpts.Prefix = g.String(key)
	}

	pager := fs.client.NewListBlobsFlatPager(fs.container, pagerOpts)

	ts := fs.GetRefTs().Unix()
	// continue fetching pages until no more remain
	for pager.More() {
		// advance to the next page
		page, err := pager.NextPage(fs.Context().Ctx)
		if err != nil {
			err = g.Error(err, "Could not get list blob for: "+uri)
			return nodes, err
		}

		// print the blob names for this page
		for _, blob := range page.Segment.BlobItems {
			blobName := *blob.Name

			lastModified := blob.Properties.LastModified
			file := dbio.FileNode{
				URI:     g.F("%s/%s", fs.Prefix(), blobName),
				Created: blob.Properties.CreationTime.Unix(),
				Updated: lastModified.Unix(),
				Size:    cast.ToUint64(blob.Properties.ContentLength),
			}
			nodes.AddWhere(pattern, ts, file)
		}
	}

	return
}

// Delete list objects in path
func (fs *AzureFileSysClient) delete(uri string) (err error) {

	path, err := fs.GetPath(uri)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+uri)
		return
	}

	deleteOpts := &blob.DeleteOptions{}
	_, err = fs.client.DeleteBlob(fs.Context().Ctx, fs.container, path, deleteOpts)
	if err != nil {
		err = g.Error(err, "Could not delete: "+uri)
		return err
	}

	return
}

func (fs *AzureFileSysClient) Write(uri string, reader io.Reader) (bw int64, err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		return
	}

	resp, err := fs.client.UploadStream(fs.Context().Ctx, fs.container, path, reader, &blockblob.UploadStreamOptions{})
	if err != nil {
		err = g.Error(err, "Error UploadStream: "+uri)
		return
	}
	_ = resp

	return
}

// GetReader returns an Azure FS reader
func (fs *AzureFileSysClient) GetReader(uri string) (reader io.Reader, err error) {
	key, err := fs.GetPath(uri)
	if err != nil {
		return
	}

	resp, err := fs.client.DownloadStream(fs.Context().Ctx, fs.container, key, &blob.DownloadStreamOptions{})
	if err != nil {
		err = g.Error(err, "Error DownloadStream: "+uri)
		return
	}

	reader = resp.Body

	return
}
