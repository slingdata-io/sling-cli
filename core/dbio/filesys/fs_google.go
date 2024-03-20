package filesys

import (
	"context"
	"io"
	"os"
	"strings"

	gcstorage "cloud.google.com/go/storage"
	"github.com/flarco/g"
	"github.com/samber/lo"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/spf13/cast"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GoogleFileSysClient is a file system client to write file to Amazon's S3 file sys.
type GoogleFileSysClient struct {
	BaseFileSysClient
	client    *gcstorage.Client
	context   g.Context
	bucket    string
	projectID string
}

// Init initializes the fs client
func (fs *GoogleFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)

	for _, key := range g.ArrStr("BUCKET", "KEY_FILE", "KEY_BODY", "CRED_API_KEY") {
		if fs.GetProp(key) == "" {
			fs.SetProp(key, fs.GetProp("GC_"+key))
		}
	}
	if fs.GetProp("KEY_FILE") == "" {
		fs.SetProp("KEY_FILE", fs.GetProp("KEYFILE")) // dbt style
	}

	return fs.Connect()
}

// Prefix returns the url prefix
func (fs *GoogleFileSysClient) Prefix(suffix ...string) string {
	return g.F("%s://%s", fs.fsType.String(), fs.bucket) + strings.Join(suffix, "")
}

// GetPath returns the path of url
func (fs *GoogleFileSysClient) GetPath(uri string) (path string, err error) {
	// normalize, in case url is provided without prefix
	uri = fs.Prefix("/") + strings.TrimLeft(strings.TrimPrefix(uri, fs.Prefix()), "/")

	host, path, err := ParseURL(uri)
	if err != nil {
		return
	}

	if fs.bucket != host {
		err = g.Error("URL bucket differs from connection bucket. %s != %s", host, fs.bucket)
	}

	return path, err
}

// Connect initiates the Google Cloud Storage client
func (fs *GoogleFileSysClient) Connect() (err error) {
	var authOption option.ClientOption
	var credJsonBody string

	if val := fs.GetProp("KEY_BODY"); val != "" {
		credJsonBody = val
		authOption = option.WithCredentialsJSON([]byte(val))
	} else if val := fs.GetProp("KEY_FILE"); val != "" {
		authOption = option.WithCredentialsFile(val)
		b, err := os.ReadFile(val)
		if err != nil {
			return g.Error(err, "could not read google cloud key file")
		}
		credJsonBody = string(b)
	} else if val := fs.GetProp("CRED_API_KEY"); val != "" {
		authOption = option.WithAPIKey(val)
	} else if val := fs.GetProp("GOOGLE_APPLICATION_CREDENTIALS"); val != "" {
		authOption = option.WithCredentialsFile(val)
		b, err := os.ReadFile(val)
		if err != nil {
			return g.Error(err, "could not read google cloud key file")
		}
		credJsonBody = string(b)
	} else {
		creds, err := google.FindDefaultCredentials(fs.Context().Ctx)
		if err != nil {
			return g.Error(err, "No Google credentials provided or could not find Application Default Credentials.")
		}
		authOption = option.WithCredentials(creds)
	}

	fs.bucket = fs.GetProp("BUCKET")
	if credJsonBody != "" {
		m := g.M()
		g.Unmarshal(credJsonBody, &m)
		fs.projectID = cast.ToString(m["project_id"])
	}

	fs.client, err = gcstorage.NewClient(fs.Context().Ctx, authOption)
	if err != nil {
		err = g.Error(err, "Could not connect to GS Storage")
		return
	}

	return nil
}

func (fs *GoogleFileSysClient) Write(path string, reader io.Reader) (bw int64, err error) {
	key, err := fs.GetPath(path)
	if err != nil {
		return
	}

	obj := fs.client.Bucket(fs.bucket).Object(key)
	wc := obj.NewWriter(fs.Context().Ctx)
	bw, err = io.Copy(wc, reader)
	if err != nil {
		err = g.Error(err, "Error Copying")
		return
	}

	if err = wc.Close(); err != nil {
		err = g.Error(err, "Error Closing writer")
		return
	}
	return
}

// GetReader returns the reader for the given path
func (fs *GoogleFileSysClient) GetReader(path string) (reader io.Reader, err error) {
	key, err := fs.GetPath(path)
	if err != nil {
		return
	}
	reader, err = fs.client.Bucket(fs.bucket).Object(key).NewReader(fs.Context().Ctx)
	if err != nil {
		err = g.Error(err, "Could not get reader for "+path)
		return
	}
	return
}

// Buckets returns the buckets found in the project
func (fs *GoogleFileSysClient) Buckets() (paths []string, err error) {
	// Create S3 service client
	it := fs.client.Buckets(fs.Context().Ctx, fs.projectID)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			err = nil
			break
		} else if err != nil {
			err = g.Error(err, "Error Iterating")
			return paths, err
		}
		paths = append(paths, g.F("gs://%s", attrs.Name))
	}
	return
}

// List returns the list of objects
func (fs *GoogleFileSysClient) List(uri string) (nodes dbio.FileNodes, err error) {
	key, err := fs.GetPath(uri)
	if err != nil {
		return
	}

	baseKeys := map[string]int{}
	keyArr := strings.Split(key, "/")
	counter := 0
	maxItems := lo.Ternary(recursiveLimit == 0, 5000, recursiveLimit)

	query := &gcstorage.Query{Prefix: key}
	query.SetAttrSelection([]string{"Name", "Size", "Created", "Updated", "Owner"})
	it := fs.client.Bucket(fs.bucket).Objects(fs.Context().Ctx, query)

	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			err = nil
			break
		} else if err != nil {
			err = g.Error(err, "Error Iterating")
			return nodes, err
		}

		counter++
		if attrs.Name == "" {
			continue
		} else if counter >= maxItems {
			g.Warn("Google storage returns results recursively by default. Limiting results at %d items. Set SLING_RECURSIVE_LIMIT to increase.", maxItems)
			break
		} else if !strings.HasPrefix(attrs.Name, key) {
			// needs to have correct key, since it's recursive
			continue
		}

		parts := strings.Split(strings.TrimSuffix(attrs.Name, "/"), "/")
		baseKey := strings.Join(parts[:len(keyArr)], "/")
		baseKeys[baseKey]++

		if baseKeys[baseKey] == 1 {
			node := dbio.FileNode{
				URI:   g.F("%s%s", fs.Prefix("/"), baseKey),
				IsDir: len(parts) >= len(keyArr)+1,
			}

			if baseKey == strings.TrimSuffix(attrs.Name, "/") {
				node.Size = cast.ToUint64(attrs.Size)
				node.Created = attrs.Created.Unix()
				node.Updated = attrs.Updated.Unix()
				node.Owner = attrs.Owner
				node.IsDir = strings.HasSuffix(attrs.Name, "/")
			}
			nodes.Add(node)
		}
	}
	return
}

// ListRecursive returns the list of objects recursively
func (fs *GoogleFileSysClient) ListRecursive(uri string) (nodes dbio.FileNodes, err error) {
	key, err := fs.GetPath(uri)
	if err != nil {
		return
	}

	filter := makeFilter(uri)
	ts := fs.GetRefTs()

	query := &gcstorage.Query{Prefix: key}
	query.SetAttrSelection([]string{"Name", "Size", "Created", "Updated", "Owner"})
	it := fs.client.Bucket(fs.bucket).Objects(fs.Context().Ctx, query)
	for {
		attrs, err := it.Next()
		// g.P(attrs)
		if err == iterator.Done {
			err = nil
			break
		}
		if err != nil {
			err = g.Error(err, "Error Iterating")
			return nodes, err
		}
		if attrs.Name == "" {
			continue
		}

		if ts.IsZero() || attrs.Updated.IsZero() || attrs.Updated.After(ts) {
			node := dbio.FileNode{
				URI:     g.F("%s/%s", fs.Prefix(), attrs.Name),
				Size:    cast.ToUint64(attrs.Size),
				Created: attrs.Created.Unix(),
				Updated: attrs.Updated.Unix(),
				Owner:   attrs.Owner,
			}
			nodes.AddPattern(filter, node)
		}
	}
	return
}

// Delete list objects in path
func (fs *GoogleFileSysClient) delete(urlStr string) (err error) {

	urlStrs, err := fs.ListRecursive(urlStr)
	if err != nil {
		err = g.Error(err, "Error List from url: "+urlStr)
		return
	}

	delete := func(key string) {
		defer fs.Context().Wg.Write.Done()
		o := fs.client.Bucket(fs.bucket).Object(key)
		if err = o.Delete(fs.Context().Ctx); err != nil {
			if strings.Contains(err.Error(), "doesn't exist") {
				g.Debug("tried to delete %s\n%s", urlStr, err.Error())
				err = nil
			} else {
				err = g.Error(err, "Could not delete "+urlStr)
				fs.Context().CaptureErr(err)
			}
		}
	}

	for _, path := range urlStrs {
		key, err := fs.GetPath(path.URI)
		if err != nil {
			return err
		}
		fs.Context().Wg.Write.Add()
		go delete(key)
	}

	fs.Context().Wg.Write.Wait()
	if fs.Context().Err() != nil {
		err = g.Error(fs.Context().Err(), "Could not delete "+urlStr)
	}
	return
}
