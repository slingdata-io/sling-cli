package filesys

import (
	"context"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/datalakeerror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/filesystem"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/spf13/cast"
)

// ABFSFileSysClient is a file system client to write file to Azure Data Lake Storage Gen2 (ABFS)
type ABFSFileSysClient struct {
	BaseFileSysClient
	client     *service.Client
	account    string
	filesystem string
	parent     string
}

// Init initializes the fs client
func (fs *ABFSFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)

	// Support both ACCOUNT and AZURE_ACCOUNT naming
	for _, key := range g.ArrStr("ACCOUNT", "CONN_STR", "SAS_SVC_URL") {
		if fs.GetProp(key) == "" {
			fs.SetProp(key, fs.GetProp("AZURE_"+key))
		}
	}

	// Service principal keys that need to be set in env var
	// These are used by azidentity.NewDefaultAzureCredential
	for _, key := range g.ArrStr("CLIENT_ID", "TENANT_ID", "CLIENT_SECRET", "CLIENT_CERTIFICATE_PATH", "CLIENT_CERTIFICATE_PASSWORD") {
		if val := fs.GetProp(key); val != "" {
			os.Setenv("AZURE_"+key, val)
		}
	}

	fs.account = fs.GetProp("account")
	fs.filesystem = fs.GetProp("filesystem") // also called the workspace
	if fs.filesystem == "" {
		fs.filesystem = fs.GetProp("container") // fallback to container for compatibility
	}
	fs.parent = fs.GetProp("parent")

	return fs.Connect()
}

// Prefix returns the url prefix
func (fs *ABFSFileSysClient) Prefix(suffix ...string) string {
	var baseURL string
	if endpoint := fs.GetProp("endpoint"); endpoint != "" {
		baseURL = g.F("https://%s/%s", endpoint, fs.filesystem)
	} else if strings.Contains(fs.account, ".") {
		baseURL = g.F("https://%s/%s", fs.account, fs.filesystem)
	} else {
		baseURL = g.F("https://%s.dfs.core.windows.net/%s", fs.account, fs.filesystem)
	}
	if fs.parent != "" {
		baseURL = strings.TrimSuffix(g.F("%s/%s", baseURL, fs.parent), "/")
	}
	return baseURL + strings.Join(suffix, "")
}

// GetPath returns the path of url
func (fs *ABFSFileSysClient) GetPath(uri string) (path string, err error) {
	// normalize, in case url is provided without prefix
	uri = NormalizeURI(fs, uri)

	// Standard HTTPS URL parsing
	host, p, err2 := ParseURL(uri)
	if err2 != nil {
		err = err2
		return
	}
	path = p

	pathFilesystem := strings.Split(path, "/")[0]

	// Extract account from host - handle both standard and custom endpoints
	accountFromHost := host
	if strings.Contains(host, ".dfs.core.windows.net") {
		accountFromHost = strings.Split(host, ".dfs.core.windows.net")[0]
	} else if strings.Contains(host, ".dfs.fabric.microsoft.com") {
		accountFromHost = strings.Split(host, ".dfs.fabric.microsoft.com")[0]
	}

	// Compare accounts - handle both simple account names and full domains
	expectedAccount := fs.account
	if strings.Contains(fs.account, ".") {
		expectedAccount = strings.Split(fs.account, ".")[0]
	}

	if accountFromHost != expectedAccount {
		err = g.Error("URL account differs from connection account. %s != %s", accountFromHost, expectedAccount)
		return
	} else if pathFilesystem != fs.filesystem {
		err = g.Error("URL filesystem differs from connection filesystem. %s != %s", pathFilesystem, fs.filesystem)
		return
	}

	// remove filesystem prefix
	path = strings.TrimPrefix(path, pathFilesystem+"/")

	return path, err
}

// Connect initiates the fs client connection
func (fs *ABFSFileSysClient) Connect() (err error) {
	// Check if a custom endpoint is provided (for Fabric or other ABFS-compatible services)
	var serviceURL string
	if endpoint := fs.GetProp("endpoint"); endpoint != "" {
		serviceURL = g.F("https://%s/", endpoint)
	} else if strings.Contains(fs.account, ".") {
		// Account already contains full domain (e.g., onelake.dfs.fabric.microsoft.com)
		serviceURL = g.F("https://%s/", fs.account)
	} else {
		// Standard Azure Storage account
		serviceURL = g.F("https://%s.dfs.core.windows.net/", fs.account)
	}

	// Connection string authentication
	if cs := fs.GetProp("CONN_STR"); cs != "" {
		// Parse connection string for account and key
		connProps := g.KVArrToMap(strings.Split(cs, ";")...)
		fs.account = connProps["AccountName"]
		accountKey := connProps["AccountKey"]

		cred, err := azdatalake.NewSharedKeyCredential(fs.account, accountKey)
		if err != nil {
			return g.Error(err, "Could not process shared key from connection string")
		}

		fs.client, err = service.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
		if err != nil {
			return g.Error(err, "Could not connect to ABFS using provided CONN_STR")
		}
	} else if sasURL := fs.GetProp("SAS_SVC_URL"); sasURL != "" {
		// SAS URL authentication
		csArr := strings.Split(sasURL, "?")
		if len(csArr) != 2 {
			return g.Error("Invalid provided SAS_SVC_URL")
		}

		fs.client, err = service.NewClientWithNoCredential(sasURL, nil)
		if err != nil {
			return g.Error(err, "Could not connect to ABFS using provided SAS_SVC_URL")
		}
	} else if accountKey := fs.GetProp("ACCOUNT_KEY"); accountKey != "" {
		// Shared key authentication
		cred, err := azdatalake.NewSharedKeyCredential(fs.account, accountKey)
		if err != nil {
			return g.Error(err, "Could not process shared key / account key")
		}

		fs.client, err = service.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
		if err != nil {
			return g.Error(err, "Could not connect to ABFS using shared key credentials")
		}
	} else {
		// Default Azure credential (uses environment variables, managed identity, etc.)
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return g.Error(err, "No ABFS credentials provided")
		}

		fs.client, err = service.NewClient(serviceURL, cred, nil)
		if err != nil {
			return g.Error(err, "Could not connect to ABFS using default credentials")
		}
	}
	return nil
}

// Buckets returns the filesystems found in the account
func (fs *ABFSFileSysClient) Buckets() (paths []string, err error) {
	pager := fs.client.NewListFileSystemsPager(nil)

	for pager.More() {
		page, err := pager.NextPage(fs.Context().Ctx)
		if err != nil {
			return paths, g.Error(err, "Could not list filesystems")
		}

		for _, item := range page.FileSystemItems {
			paths = append(paths, *item.Name)
		}
	}

	return
}

// List list objects in path (non-recursive)
func (fs *ABFSFileSysClient) List(uri string) (nodes FileNodes, err error) {
	key, err := fs.GetPath(uri)
	if err != nil {
		return nodes, g.Error(err, "Error Parsing url: "+uri)
	}

	pattern, err := makeGlob(NormalizeURI(fs, uri))
	if err != nil {
		return nodes, g.Error(err, "Error Parsing url pattern: "+uri)
	}

	fsClient := fs.client.NewFileSystemClient(fs.filesystem)

	baseKeys := map[string]int{}
	keyArr := strings.Split(key, "/")
	counter := 0
	maxItems := lo.Ternary(recursiveLimit == 0, 10000, recursiveLimit)

	listOpts := &filesystem.ListPathsOptions{}
	if key != "" {
		listOpts.Prefix = &key
	}

	pager := fsClient.NewListPathsPager(false, listOpts)

	for pager.More() {
		page, err := pager.NextPage(fs.Context().Ctx)
		if err != nil {
			return nodes, g.Error(err, "Could not list paths for: "+uri)
		}

		g.Trace("pager found %d paths", len(page.PathList.Paths), g.M("recursive", false, "pattern", pattern, "key", key))

		for _, path := range page.PathList.Paths {
			pathName := *path.Name

			counter++
			if counter >= maxItems {
				g.Warn("ABFS returns results recursively by default. Limiting list results at %d items. Set SLING_RECURSIVE_LIMIT to increase.", maxItems)
				break
			}

			if key != "" && !strings.HasPrefix(pathName, key) {
				continue
			}

			// remove parent prefix from pathName since fs.Prefix() will include it
			// pathName = strings.TrimPrefix(pathName, fs.parent+"/")

			parts := strings.Split(strings.TrimSuffix(pathName, "/"), "/")
			baseKey := strings.Join(parts[:len(keyArr)], "/")
			baseKeys[baseKey]++

			if baseKeys[baseKey] == 1 {
				// remove parent prefix from pathName since fs.Prefix() will include it
				node := FileNode{
					URI:   g.F("%s/%s", fs.Prefix(), strings.TrimPrefix(baseKey, fs.parent+"/")),
					IsDir: len(parts) >= len(keyArr)+1,
				}

				if baseKey == strings.TrimSuffix(pathName, "/") && path.ContentLength != nil {
					node.Size = cast.ToUint64(*path.ContentLength)
					if path.CreationTime != nil && *path.CreationTime != "" {
						if t, err := time.Parse(time.RFC3339, *path.CreationTime); err == nil {
							node.Created = t.Unix()
						}
					}
					if path.LastModified != nil && *path.LastModified != "" {
						if t, err := time.Parse(time.RFC3339, *path.LastModified); err == nil {
							node.Updated = t.Unix()
						}
					}
					node.IsDir = lo.FromPtr(path.IsDirectory)
				}
				nodes.AddWhere(pattern, 0, node)
			}
		}
	}

	return
}

// // List list objects in path (non-recursive)
// func (fs *ABFSFileSysClient) List(uri string) (nodes FileNodes, err error) {
// 	return fs.doList(uri, false)
// }

// ListRecursive list objects in path recursively
func (fs *ABFSFileSysClient) ListRecursive(uri string) (nodes FileNodes, err error) {
	return fs.doList(uri, true)
}

func (fs *ABFSFileSysClient) doList(uri string, recursive bool) (nodes FileNodes, err error) {
	maxItems := lo.Ternary(recursiveLimit == 0, 10000, recursiveLimit)

	key, err := fs.GetPath(uri)
	if err != nil {
		return nodes, g.Error(err, "Error Parsing url: "+uri)
	}

	pattern, err := makeGlob(NormalizeURI(fs, uri))
	if err != nil {
		return nodes, g.Error(err, "Error Parsing url pattern: "+uri)
	}

	fsClient := fs.client.NewFileSystemClient(fs.filesystem)

	listOpts := &filesystem.ListPathsOptions{}
	if key != "" {
		listOpts.Prefix = &key
	}

	pager := fsClient.NewListPathsPager(recursive, listOpts)
	ts := fs.GetRefTs().Unix()

	for pager.More() {
		page, err := pager.NextPage(fs.Context().Ctx)
		if err != nil {
			return nodes, g.Error(err, "Could not list paths recursively for: "+uri)
		}

		g.Trace("pager found %d paths", len(page.PathList.Paths), g.M("recursive", recursive, "pattern", pattern, "key", key))

		for _, path := range page.PathList.Paths {
			pathName := *path.Name

			// remove parent prefix from pathName since fs.Prefix() will include it
			pathName = strings.TrimPrefix(pathName, fs.parent+"/")

			node := FileNode{
				URI:   g.F("%s/%s", fs.Prefix(), pathName),
				IsDir: lo.FromPtr(path.IsDirectory),
			}

			if path.ContentLength != nil {
				node.Size = cast.ToUint64(*path.ContentLength)
			}
			if path.CreationTime != nil && *path.CreationTime != "" {
				if t, err := time.Parse(time.RFC3339, *path.CreationTime); err == nil {
					node.Created = t.Unix()
				}
			}
			if path.LastModified != nil && *path.LastModified != "" {
				if t, err := time.Parse(time.RFC3339, *path.LastModified); err == nil {
					node.Updated = t.Unix()
				}
			}

			nodes.AddWhere(pattern, ts, node)

			if len(nodes) >= maxItems {
				g.Warn("ABFS returns results recursively by default. Limiting list results at %d items. Set SLING_RECURSIVE_LIMIT to increase.", maxItems)
				return nodes, nil
			}
		}
	}

	return
}

// delete removes a file or directory
func (fs *ABFSFileSysClient) delete(uri string) (err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		return g.Error(err, "Error Parsing url: "+uri)
	}

	// Get properties to determine if it's a file or directory
	fileClient := fs.client.NewFileSystemClient(fs.filesystem).NewFileClient(path)
	props, err := fileClient.GetProperties(fs.Context().Ctx, nil)

	if err != nil {
		if datalakeerror.HasCode(err, datalakeerror.PathNotFound) {
			return nil
		}
		return g.Error(err, "Could not get properties for: "+uri)
	}

	// Check if it's a directory by looking for the Hdi_isfolder metadata
	isDirectory := false
	if props.Metadata != nil {
		if _, exists := props.Metadata["Hdi_isfolder"]; exists {
			isDirectory = true
		}
	}

	if isDirectory {
		// Delete as directory
		dirClient := fs.client.NewFileSystemClient(fs.filesystem).NewDirectoryClient(path)
		if _, err = dirClient.Delete(fs.Context().Ctx, nil); err != nil {
			err = g.Error(err, "Could not delete directory: "+uri)
		}
	} else {
		// Delete as file
		if _, err = fileClient.Delete(fs.Context().Ctx, nil); err != nil {
			err = g.Error(err, "Could not delete file: "+uri)
		}
	}

	if err != nil && strings.Contains(err.Error(), "Path not found") {
		err = nil
	}

	return err
}

// Write writes a stream to a file
func (fs *ABFSFileSysClient) Write(uri string, reader io.Reader) (bw int64, err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		return 0, err
	}

	fileClient := fs.client.NewFileSystemClient(fs.filesystem).NewFileClient(path)

	// Create the file first
	_, err = fileClient.Create(fs.Context().Ctx, nil)
	if err != nil {
		return 0, g.Error(err, "Error creating file: "+uri)
	}

	// Upload the content
	err = fileClient.UploadStream(fs.Context().Ctx, reader, nil)
	if err != nil {
		return 0, g.Error(err, "Error uploading stream: "+uri)
	}

	// Get file properties to determine size
	props, err := fileClient.GetProperties(fs.Context().Ctx, nil)
	if err == nil && props.ContentLength != nil {
		bw = *props.ContentLength
	}

	return bw, nil
}

// GetReader returns an ABFS reader
func (fs *ABFSFileSysClient) GetReader(uri string) (reader io.Reader, err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		return nil, err
	}

	fileClient := fs.client.NewFileSystemClient(fs.filesystem).NewFileClient(path)

	resp, err := fileClient.DownloadStream(fs.Context().Ctx, nil)
	if err != nil {
		// Check if it's a "not found" error
		if datalakeerror.HasCode(err, datalakeerror.PathNotFound) {
			return nil, g.Error("File not found: " + uri)
		}
		return nil, g.Error(err, "Error downloading stream: "+uri)
	}

	reader = resp.Body

	return reader, nil
}
