package filesys

import (
	"context"
	"io"
	"net/url"
	"strings"
	"time"

	azstorage "github.com/Azure/azure-sdk-for-go/storage"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/flarco/g"
)

// AzureFileSysClient is a file system client to write file to Microsoft's Azure file sys.
type AzureFileSysClient struct {
	BaseFileSysClient
	client  azstorage.Client
	context g.Context
	account string
	key     string
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
	return fs.Connect()
}

// Connect initiates the fs client connection
func (fs *AzureFileSysClient) Connect() (err error) {

	if cs := fs.GetProp("CONN_STR"); cs != "" {
		connProps := g.KVArrToMap(strings.Split(cs, ";")...)
		fs.account = connProps["AccountName"]
		fs.key = connProps["AccountKey"]
		fs.client, err = azstorage.NewClientFromConnectionString(cs)
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

		host, _, _ := ParseURL(cs)
		fs.account = strings.TrimRight(host, ".blob.core.windows.net")
		fs.client, err = azstorage.NewAccountSASClientFromEndpointToken(csArr[0], csArr[1])
		if err != nil {
			err = g.Error(err, "Could not connect to Azure using provided SAS_SVC_URL")
			return
		}
	} else {
		return g.Error("No Azure credentials preovided")
	}
	return
}

func (fs *AzureFileSysClient) getContainers() (containers []azstorage.Container, err error) {
	params := azstorage.ListContainersParameters{}
	resp, err := fs.client.GetBlobService().ListContainers(params)
	if err != nil {
		err = g.Error(err, "Could not ListContainers")
		return
	}
	containers = resp.Containers
	return
}

func (fs *AzureFileSysClient) getBlobs(container *azstorage.Container, params azstorage.ListBlobsParameters) (blobs []azstorage.Blob, err error) {
	resp, err := container.ListBlobs(params)
	if err != nil {
		err = g.Error(err, "Could not ListBlobs for: "+container.GetURL())
		return
	}
	blobs = resp.Blobs
	return
}

func (fs *AzureFileSysClient) getAuthContainerURL(container *azstorage.Container) (containerURL azblob.ContainerURL, err error) {

	if fs.GetProp("CONN_STR") != "" {
		credential, err := azblob.NewSharedKeyCredential(fs.account, fs.key)
		if err != nil {
			err = g.Error(err, "Unable to Authenticate Azure")
			return containerURL, err
		}
		pipeline := azblob.NewPipeline(credential, azblob.PipelineOptions{})
		contURL, _ := url.Parse(container.GetURL())
		containerURL = azblob.NewContainerURL(*contURL, pipeline)
	} else if fs.GetProp("SAS_SVC_URL") != "" {
		sasSvcURL, _ := url.Parse(fs.GetProp("SAS_SVC_URL"))
		serviceURL := azblob.NewServiceURL(*sasSvcURL, azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}))
		containerURL = serviceURL.NewContainerURL(container.Name)
	} else {
		err = g.Error(
			g.Error("Need to provide Azure credentials CONN_STR or SAS_SVC_URL"),
			"",
		)
		return
	}
	return
}

// Buckets returns the containers found in the project
func (fs *AzureFileSysClient) Buckets() (paths []string, err error) {
	containers, err := fs.getContainers()
	if err != nil {
		err = g.Error(err, "Could not get Containers")
		return paths, err
	}
	for _, container := range containers {
		paths = append(
			paths,
			g.F("https://%s.blob.core.windows.net/%s", fs.account, container.Name),
		)
	}
	return
}

func cleanKeyAzure(key string) string {
	key = strings.TrimPrefix(key, "/")
	key = strings.TrimSuffix(key, "/")
	return key
}

// List list objects in path
func (fs *AzureFileSysClient) List(url string) (paths []string, err error) {
	host, path, err := ParseURL(url)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+url)
		return
	}

	path = cleanKeyAzure(path)
	ts := fs.GetRefTs()

	svc := fs.client.GetBlobService()

	pathArr := strings.Split(path, "/")
	if path == "" {
		// list container
		containers, err := fs.getContainers()
		if err != nil {
			err = g.Error(err, "Could not getContainers for: "+url)
			return paths, err
		}
		for _, container := range containers {
			paths = append(
				paths,
				g.F("https://%s/%s", host, container.Name),
			)
		}
	} else if len(pathArr) == 1 {
		// list blobs
		container := svc.GetContainerReference(pathArr[0])
		blobs, err := fs.getBlobs(container, azstorage.ListBlobsParameters{Delimiter: "/"})
		if err != nil {
			err = g.Error(err, "Could not ListBlobs for: "+url)
			return paths, err
		}
		g.P(blobs)
		for _, blob := range blobs {
			lastModified := time.Time(blob.Properties.LastModified)
			if ts.IsZero() || lastModified.IsZero() || lastModified.After(ts) {
				paths = append(
					paths,
					g.F("https://%s/%s/%s", host, container.Name, blob.Name),
				)
			}
		}
	} else if len(pathArr) > 1 {
		container := svc.GetContainerReference(pathArr[0])
		prefix := strings.Join(pathArr[1:], "/")
		blobs, err := fs.getBlobs(
			container,
			azstorage.ListBlobsParameters{Delimiter: "/", Prefix: prefix + "/"},
		)
		if err != nil {
			err = g.Error(err, "Could not ListBlobs for: "+url)
			return paths, err
		}
		for _, blob := range blobs {
			lastModified := time.Time(blob.Properties.LastModified)
			if ts.IsZero() || lastModified.IsZero() || lastModified.After(ts) {
				paths = append(
					paths,
					g.F("https://%s/%s/%s", host, container.Name, blob.Name),
				)
			}
		}

		if len(blobs) == 0 {
			// maybe a file
			prefixArr := strings.Split(prefix, "/")
			parentPrefix := strings.Join(prefixArr[0:len(prefixArr)-1], "/")
			if parentPrefix != "" {
				parentPrefix = parentPrefix + "/"
			}
			blobs, err = fs.getBlobs(
				container,
				azstorage.ListBlobsParameters{Delimiter: "/", Prefix: parentPrefix},
			)

			if err != nil {
				err = g.Error(err, "Could not ListBlobs for: "+url)
				return paths, err
			}
			for _, blob := range blobs {
				// blob.Properties.LastModified
				blobFile := g.F("https://%s/%s/%s", host, container.Name, blob.Name)
				if blobFile == url {
					paths = append(
						paths,
						blobFile,
					)
				}
			}
		}

	} else {
		err = g.Error("Invalid Azure path: " + url)
		return
	}

	return
}

// ListRecursive list objects in path
func (fs *AzureFileSysClient) ListRecursive(url string) (paths []string, err error) {
	_, path, err := ParseURL(url)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+url)
		return
	}

	path = cleanKeyAzure(path)
	g.Trace("listing recursively => %s", path)

	if path != "" {
		// list blobs
		return fs.List(url)
	}

	// list blobs of each continer
	containers, err := fs.getContainers()
	if err != nil {
		err = g.Error(err, "Could not getContainers for: "+url)
		return paths, err
	}
	for _, container := range containers {
		contURL := container.GetURL()
		contPaths, err := fs.List(contURL)
		if err != nil {
			err = g.Error(err, "Could not List blobs for container: "+contURL)
			return paths, err
		}
		paths = append(paths, contPaths...)
	}

	return
}

// Delete list objects in path
func (fs *AzureFileSysClient) delete(urlStr string) (err error) {
	suffixWildcard := false
	if strings.HasSuffix(urlStr, "*") {
		urlStr = urlStr[:len(urlStr)-1]
		suffixWildcard = true
	}

	host, path, err := ParseURL(urlStr)
	if err != nil {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}

	path = cleanKeyAzure(path)
	svc := fs.client.GetBlobService()

	pathArr := strings.Split(path, "/")
	if path == "" {
		// list container
		containers, err := fs.getContainers()
		if err != nil {
			err = g.Error(err, "Could not getContainers for: "+urlStr)
			return err
		}
		for _, container := range containers {
			options := azstorage.DeleteContainerOptions{}
			err = container.Delete(&options)
			if err != nil {
				return g.Error(err, "Could not delete container: "+container.GetURL())
			}
		}
	} else if len(pathArr) == 1 {
		// list blobs
		container := svc.GetContainerReference(pathArr[0])
		blobs, err := fs.getBlobs(container, azstorage.ListBlobsParameters{Delimiter: "/"})
		if err != nil {
			err = g.Error(err, "Could not ListBlobs for: "+urlStr)
			return err
		}
		for _, blob := range blobs {
			if suffixWildcard && !strings.HasPrefix(blob.Name, pathArr[1]) {
				continue
			}
			options := azstorage.DeleteBlobOptions{}
			err = blob.Delete(&options)
			if err != nil {
				if strings.Contains(err.Error(), "BlobNotFound") {
					err = nil
				} else {
					return g.Error(err, "Could not delete blob: "+blob.GetURL())
				}
			}
		}
	} else if len(pathArr) > 1 {
		// list blobs
		container := svc.GetContainerReference(pathArr[0])
		prefix := strings.Join(pathArr[1:], "/")
		blobs, err := fs.getBlobs(
			container,
			azstorage.ListBlobsParameters{Delimiter: "/", Prefix: prefix + "/"},
		)
		if err != nil {
			err = g.Error(err, "Could not ListBlobs for: "+urlStr)
			return err
		}
		for _, blob := range blobs {
			if suffixWildcard && !strings.HasPrefix(blob.Name, pathArr[1]) {
				continue
			}
			options := azstorage.DeleteBlobOptions{}
			err = blob.Delete(&options)
			if err != nil {
				if strings.Contains(err.Error(), "BlobNotFound") {
					err = nil
				} else {
					return g.Error(err, "Could not delete blob: "+blob.GetURL())
				}
			}
		}
		if len(blobs) == 0 {
			// maybe a file
			prefixArr := strings.Split(prefix, "/")
			parentPrefix := strings.Join(prefixArr[0:len(prefixArr)-1], "/")
			if parentPrefix != "" {
				parentPrefix = parentPrefix + "/"
			}

			blobs, err = fs.getBlobs(
				container,
				azstorage.ListBlobsParameters{Delimiter: "/", Prefix: parentPrefix},
			)
			if err != nil {
				err = g.Error(err, "Could not ListBlobs for: "+urlStr)
				return err
			}

			for _, blob := range blobs {
				blobFile := g.F("https://%s/%s/%s", host, container.Name, blob.Name)
				if blobFile == urlStr {
					options := azstorage.DeleteBlobOptions{}
					err = blob.Delete(&options)
					if err != nil {
						if strings.Contains(err.Error(), "BlobNotFound") {
							err = nil
						} else {
							return g.Error(err, "Could not delete blob: "+blob.GetURL())
						}
					}
				}
			}
		}
	} else {
		err = g.Error(
			g.Error("Invalid Azure path: "+urlStr),
			"",
		)
	}
	return
}

func (fs *AzureFileSysClient) Write(urlStr string, reader io.Reader) (bw int64, err error) {
	host, path, err := ParseURL(urlStr)
	if err != nil || host == "" {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}

	path = cleanKeyAzure(path)

	pathArr := strings.Split(path, "/")

	if len(pathArr) < 2 {
		err = g.Error("Invalid Azure path (need blob URL): " + urlStr)
		err = g.Error(err)
		return
	}

	svc := fs.client.GetBlobService()
	container := svc.GetContainerReference(pathArr[0])
	_, err = container.CreateIfNotExists(&azstorage.CreateContainerOptions{Timeout: 20})
	if err != nil {
		err = g.Error(err, "Unable to create container: "+container.GetURL())
		return
	}

	blobPath := strings.Join(pathArr[1:], "/")
	blob := container.GetBlobReference(blobPath)
	err = blob.CreateBlockBlob(&azstorage.PutBlobOptions{})
	if err != nil {
		err = g.Error(err, "Unable to CreateBlockBlob: "+blob.GetURL())
		return
	}

	containerURL, err := fs.getAuthContainerURL(container)
	if err != nil {
		err = g.Error(err, "Unable to getAuthContainerURL: "+container.GetURL())
		return
	}

	blockBlobURL := containerURL.NewBlockBlobURL(blob.Name)

	pr, pw := io.Pipe()
	fs.Context().Wg.Write.Add()
	go func() {
		defer fs.Context().Wg.Write.Done()
		defer pw.Close()
		bw, err = io.Copy(pw, reader)
		if err != nil {
			fs.Context().CaptureErr(g.Error(err, "Error Copying"))
		}
	}()
	_, err = azblob.UploadStreamToBlockBlob(
		fs.Context().Ctx, pr, blockBlobURL,
		azblob.UploadStreamToBlockBlobOptions{
			BufferSize: 2 * 1024 * 1024,
			MaxBuffers: 3,
		},
	)
	fs.Context().Wg.Write.Wait()
	if err != nil {
		err = g.Error(err, "Error UploadStreamToBlockBlob: "+blockBlobURL.String())
		return
	}

	return
}

// GetReader returns an Azure FS reader
func (fs *AzureFileSysClient) GetReader(urlStr string) (reader io.Reader, err error) {
	host, path, err := ParseURL(urlStr)
	if err != nil || host == "" {
		err = g.Error(err, "Error Parsing url: "+urlStr)
		return
	}

	path = cleanKeyAzure(path)

	pathArr := strings.Split(path, "/")

	if len(pathArr) < 2 {
		err = g.Error("Invalid Azure path (need blob URL): " + urlStr)
		err = g.Error(err)
		return
	}

	svc := fs.client.GetBlobService()
	container := svc.GetContainerReference(pathArr[0])

	// containerURL, err := fs.getAuthContainerURL(container)
	// if err != nil {
	// 	err = g.Error(err, "Unable to getAuthContainerURL: "+container.GetURL())
	// 	return
	// }
	// blobURL := containerURL.NewBlobURL(pathArr[1])

	// data := make([]byte, 1000)
	// err = azblob.DownloadBlobToBuffer(
	// 	fs.context.Ctx, blobURL, 0, 0, data,
	// 	azblob.DownloadFromBlobOptions{
	// 		Parallelism: 10,
	// 	},
	// )
	// if err != nil {
	// 	err = g.Error(err, "Unable to DownloadBlobToBuffer Blob: "+blobURL.String())
	// 	return
	// }
	// reader = bytes.NewReader(data)

	blobPath := strings.Join(pathArr[1:], "/")
	blob := container.GetBlobReference(blobPath)
	options := &azstorage.GetBlobOptions{}
	reader, err = blob.Get(options)
	if err != nil {
		err = g.Error(err, "Unable to Get Blob: "+blob.GetURL())
		return
	}

	return
}
