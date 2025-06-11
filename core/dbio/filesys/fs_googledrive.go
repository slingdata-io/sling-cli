package filesys

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/samber/lo"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// GoogleDriveFileSysClient is a file system client for Google Drive
type GoogleDriveFileSysClient struct {
	BaseFileSysClient
	client       *drive.Service
	context      g.Context
	rootFolderID string // Optional: Use a specific folder as root
	fileID       string // Optional: Direct file ID for single file access
}

// Init initializes the fs client
func (fs *GoogleDriveFileSysClient) Init(ctx context.Context) (err error) {
	var instance FileSysClient
	instance = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)

	// Support both prefixed and non-prefixed keys
	for _, key := range g.ArrStr("KEY_FILE", "KEY_BODY", "CLIENT_ID", "CLIENT_SECRET", "ACCESS_TOKEN", "REFRESH_TOKEN", "FOLDER_ID", "FILE_ID") {
		if fs.GetProp(key) == "" {
			fs.SetProp(key, fs.GetProp("GDRIVE_"+key))
		}
	}

	// Set root folder ID and file ID if provided
	fs.rootFolderID = fs.GetProp("FOLDER_ID")
	fs.fileID = fs.GetProp("FILE_ID")

	if fs.rootFolderID != "" {
		g.Trace("Using root folder ID: %s", fs.rootFolderID)
	}
	if fs.fileID != "" {
		g.Trace("Using file ID: %s", fs.fileID)
	}

	return fs.Connect()
}

// Prefix returns the url prefix
func (fs *GoogleDriveFileSysClient) Prefix(suffix ...string) string {
	return "gdrive://" + strings.Join(suffix, "")
}

// GetPath returns the path of url
func (fs *GoogleDriveFileSysClient) GetPath(uri string) (path string, err error) {
	// normalize, in case url is provided without prefix
	uri = NormalizeURI(fs, uri)

	_, path, err = ParseURL(uri)
	if err != nil {
		return
	}

	// Google Drive paths should start with / if not empty
	if path != "" && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	g.Trace("GetPath: uri='%s' -> path='%s'", uri, path)
	return path, err
}

// Connect initiates the Google Drive client
func (fs *GoogleDriveFileSysClient) Connect() (err error) {
	var authOption option.ClientOption

	// set scopes
	scopes := []string{drive.DriveScope}
	if val := fs.GetProp("SCOPES"); val != "" {
		if err = g.Unmarshal(val, &scopes); err != nil {
			return g.Error(err, "could not parse provided scopes")
		}
	}

	// Try different authentication methods
	if val := fs.GetProp("KEY_BODY"); val != "" {
		// Service account credentials from JSON body
		config, err := google.JWTConfigFromJSON([]byte(val), scopes...)
		if err != nil {
			return g.Error(err, "could not parse Google Drive service account key")
		}
		g.Trace("using KEYBODY JSON payload for google drive authentication")
		authOption = option.WithTokenSource(config.TokenSource(fs.Context().Ctx))
	} else if val := fs.GetProp("KEY_FILE"); val != "" {
		// Service account credentials from file
		b, err := os.ReadFile(val)
		if err != nil {
			return g.Error(err, "could not read Google Drive key file")
		}
		config, err := google.JWTConfigFromJSON(b, scopes...)
		if err != nil {
			return g.Error(err, "could not parse Google Drive service account key")
		}
		g.Trace("using KEY_FILE (%s) for google drive authentication", val)
		authOption = option.WithTokenSource(config.TokenSource(fs.Context().Ctx))
	} else if clientID := fs.GetProp("CLIENT_ID"); clientID != "" && fs.GetProp("CLIENT_SECRET") != "" {
		// OAuth2 credentials
		config := &oauth2.Config{
			ClientID:     clientID,
			ClientSecret: fs.GetProp("CLIENT_SECRET"),
			Endpoint:     google.Endpoint,
			Scopes:       scopes,
		}

		token := &oauth2.Token{
			AccessToken:  fs.GetProp("ACCESS_TOKEN"),
			RefreshToken: fs.GetProp("REFRESH_TOKEN"),
			TokenType:    "Bearer",
		}

		// If we have an access token expiry, parse it
		if expiry := fs.GetProp("TOKEN_EXPIRY"); expiry != "" {
			if t, err := time.Parse(time.RFC3339, expiry); err == nil {
				token.Expiry = t
			}
		}

		g.Trace("using OAuth for google drive authentication")
		authOption = option.WithTokenSource(config.TokenSource(fs.Context().Ctx, token))
	} else {
		// Try default credentials
		creds, err := google.FindDefaultCredentials(fs.Context().Ctx, scopes...)
		if err != nil {
			return g.Error(err, "No Google Drive credentials provided or could not find Application Default Credentials.")
		}
		g.Trace("using default credentials for google drive authentication")
		authOption = option.WithCredentials(creds)
	}

	fs.client, err = drive.NewService(fs.Context().Ctx, authOption)
	if err != nil {
		err = g.Error(err, "Could not connect to Google Drive")
		return
	}

	// Validate rootFolderID if provided
	if fs.rootFolderID != "" {
		_, err = fs.client.Files.Get(fs.rootFolderID).SupportsAllDrives(true).Fields("id").Do()
		if err != nil {
			if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == 404 {
				return g.Error("Root folder ID '%s' does not exist or is not accessible", fs.rootFolderID)
			}
			return g.Error(err, "Could not verify root folder ID: %s", fs.rootFolderID)
		}
		g.Trace("Verified root folder ID: %s", fs.rootFolderID)
	}

	// Validate fileID if provided
	if fs.fileID != "" {
		_, err = fs.client.Files.Get(fs.fileID).Fields("id").SupportsAllDrives(true).Do()
		if err != nil {
			if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == 404 {
				return g.Error("File ID '%s' does not exist or is not accessible", fs.fileID)
			}
			return g.Error(err, "Could not verify file ID: %s", fs.fileID)
		}
		g.Trace("Verified file ID: %s", fs.fileID)
	}

	return nil
}

// Buckets returns the root folder (Google Drive doesn't have buckets)
func (fs *GoogleDriveFileSysClient) Buckets() (paths []string, err error) {
	// Google Drive doesn't have buckets, return root
	paths = append(paths, "gdrive://")
	return
}

// getFileID gets the file ID from a path
func (fs *GoogleDriveFileSysClient) getFileID(path string) (fileID string, err error) {
	// If a specific file ID is set, return it directly (for single file access)
	if fs.fileID != "" && (path == "" || path == "/" || path == fs.fileID) {
		return fs.fileID, nil
	}

	if path == "" || path == "/" {
		// If rootFolderID is set, use it; otherwise use "root"
		if fs.rootFolderID != "" {
			return fs.rootFolderID, nil
		}
		return "root", nil
	}

	// Clean the path
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimSuffix(path, "/")

	// Split path into parts
	parts := strings.Split(path, "/")

	// Start from root or specified folder
	parentID := "root"
	if fs.rootFolderID != "" {
		parentID = fs.rootFolderID
	}

	// Traverse the path
	for i, part := range parts {
		if part == "" {
			continue
		}

		g.Trace("Looking for '%s' in parent '%s' (part %d of %d)", part, parentID, i+1, len(parts))

		// Search for the part in the current parent
		query := fmt.Sprintf("name='%s' and '%s' in parents and trashed=false", part, parentID)
		result, err := fs.client.Files.List().Q(query).Fields("files(id, name, mimeType)").IncludeItemsFromAllDrives(true).SupportsAllDrives(true).Do()
		if err != nil {
			return "", g.Error(err, "Could not search for file: "+part)
		}

		if len(result.Files) == 0 {
			return "", g.Error("File not found: %s", path)
		}

		g.Trace("Found '%s' with ID '%s'", result.Files[0].Name, result.Files[0].Id)
		// Use the first match
		parentID = result.Files[0].Id
	}

	return parentID, nil
}

// Write writes data to Google Drive
func (fs *GoogleDriveFileSysClient) Write(uri string, reader io.Reader) (bw int64, err error) {
	// If fileID is set, don't allow writes (it's for direct file access only)
	if fs.fileID != "" {
		return 0, g.Error("Cannot write when FILE_ID is set. FILE_ID is for direct file access only.")
	}

	path, err := fs.GetPath(uri)
	if err != nil {
		return
	}

	// Clean the path
	path = strings.TrimPrefix(path, "/")
	dir, name := splitPath(path)

	// Get parent folder ID
	parentID := "root"
	if fs.rootFolderID != "" {
		parentID = fs.rootFolderID
	}
	if dir != "" {
		parentID, err = fs.getOrCreateFolder(dir)
		if err != nil {
			return 0, g.Error(err, "Could not get/create parent folder")
		}
	}

	// Check if file already exists
	query := fmt.Sprintf("name='%s' and '%s' in parents and trashed=false", name, parentID)
	result, err := fs.client.Files.List().Q(query).Fields("files(id)").IncludeItemsFromAllDrives(true).SupportsAllDrives(true).Do()
	if err != nil {
		return 0, g.Error(err, "Could not check if file exists")
	}

	// Create a pipe to count bytes
	pr, pw := io.Pipe()
	doneChan := make(chan error, 1)

	go func() {
		defer pw.Close()
		n, err := io.Copy(pw, reader)
		bw = n
		doneChan <- err
	}()

	if len(result.Files) > 0 {
		// Update existing file
		file := &drive.File{
			Name: name,
		}
		_, err = fs.client.Files.Update(result.Files[0].Id, file).Media(pr).Do()
	} else {
		// Create new file
		file := &drive.File{
			Name:     name,
			Parents:  []string{parentID},
			MimeType: "application/octet-stream",
		}
		_, err = fs.client.Files.Create(file).Media(pr).Do()
	}

	if err != nil {
		return 0, g.Error(err, "Could not upload file to Google Drive")
	}

	// Wait for copy to complete
	if copyErr := <-doneChan; copyErr != nil {
		return bw, g.Error(copyErr, "Error copying data")
	}

	return
}

// GetReader returns a reader for the given path
func (fs *GoogleDriveFileSysClient) GetReader(uri string) (reader io.Reader, err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		return
	}

	// If fileID is set and path matches, use it directly
	fileID := ""
	if fs.fileID != "" && (path == "" || path == "/" || path == fs.fileID) {
		fileID = fs.fileID
	} else {
		fileID, err = fs.getFileID(path)
		if err != nil {
			return nil, g.Error(err, "Could not get file ID for: "+path)
		}
	}

	resp, err := fs.client.Files.Get(fileID).SupportsAllDrives(true).Download()
	if err != nil {
		return nil, g.Error(err, "Could not download file: "+path)
	}

	return resp.Body, nil
}

// List returns the list of objects
func (fs *GoogleDriveFileSysClient) List(uri string) (nodes FileNodes, err error) {
	// If fileID is set, listing is not supported
	if fs.fileID != "" {
		return nodes, g.Error("Listing is not supported when FILE_ID is set. FILE_ID is for direct file access only.")
	}

	path, err := fs.GetPath(uri)
	if err != nil {
		return
	}

	g.Trace("uri='%s', path='%s'", uri, path)

	pattern, err := makeGlob(NormalizeURI(fs, uri))
	if err != nil {
		err = g.Error(err, "Error parsing url pattern: "+uri)
		return
	}

	// Variables to track the folder we'll list
	var folderID string
	var fileID string

	// Check if path exists and whether it's a file or folder
	if path != "" && path != "/" {
		fileID, err = fs.getFileID(path)
		if err != nil {
			// If not found, return empty list
			if strings.Contains(err.Error(), "not found") {
				return nodes, nil
			}
			return
		}

		g.Trace("Got fileID '%s' for path '%s'", fileID, path)

		// Get file info to check if it's a file or folder
		var file *drive.File
		file, err = fs.client.Files.Get(fileID).Fields("id, name, size, createdTime, modifiedTime, mimeType, owners").SupportsAllDrives(true).Do()
		if err != nil {
			// If file doesn't exist (404), return empty list
			if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == 404 {
				g.Trace("File ID '%s' not found (404), returning empty list", fileID)
				return nodes, nil
			}
			return nodes, g.Error(err, "Could not get file info")
		}

		isDir := file.MimeType == "application/vnd.google-apps.folder"

		// If path doesn't end with "/" and it's either a file or a folder,
		// return just this single item
		if !strings.HasSuffix(uri, "/") {
			node := FileNode{
				URI:   fs.Prefix() + path,
				IsDir: isDir,
				Size:  uint64(file.Size),
			}

			if isDir {
				node.URI += "/"
			}

			if file.CreatedTime != "" {
				if t, err := time.Parse(time.RFC3339, file.CreatedTime); err == nil {
					node.Created = t.Unix()
				}
			}

			if file.ModifiedTime != "" {
				if t, err := time.Parse(time.RFC3339, file.ModifiedTime); err == nil {
					node.Updated = t.Unix()
				}
			}

			if len(file.Owners) > 0 {
				node.Owner = file.Owners[0].DisplayName
			}

			nodes.Add(node)
			return nodes, nil
		}

		// If it's not a folder, return empty (can't list a file's contents)
		if !isDir {
			return nodes, nil
		}

		// Path ends with "/" and is a folder, list its contents
		// Use the fileID we validated above
		folderID = fileID
	} else {
		// Root path or empty path
		folderID = "root"
		if fs.rootFolderID != "" {
			folderID = fs.rootFolderID
		}
	}

	// List files in folder
	if folderID == "" {
		g.Warn("folderID is empty, cannot list files")
		return nodes, nil
	}

	g.Trace("listing contents of folder ID: %s for path: %s (rootFolderID: %s)", folderID, path, fs.rootFolderID)
	query := fmt.Sprintf("'%s' in parents and trashed=false", folderID)
	g.Trace("query => %s", query)
	fields := googleapi.Field("files(id, name, size, createdTime, modifiedTime, mimeType, owners)")

	pageToken := ""
	for {
		call := fs.client.Files.List().Q(query).Fields(fields).PageSize(1000).IncludeItemsFromAllDrives(true).SupportsAllDrives(true)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}

		result, err := call.Do()
		if err != nil {
			return nodes, g.Error(err, "Could not list files")
		}

		for _, file := range result.Files {
			isDir := file.MimeType == "application/vnd.google-apps.folder"

			nodePath := path
			if nodePath == "" || nodePath == "/" {
				nodePath = "/" + file.Name
			} else {
				nodePath = strings.TrimSuffix(path, "/") + "/" + file.Name
			}

			if isDir {
				nodePath += "/"
			}

			node := FileNode{
				URI:   fs.Prefix() + nodePath,
				IsDir: isDir,
				Size:  uint64(file.Size),
			}

			if file.CreatedTime != "" {
				if t, err := time.Parse(time.RFC3339, file.CreatedTime); err == nil {
					node.Created = t.Unix()
				}
			}

			if file.ModifiedTime != "" {
				if t, err := time.Parse(time.RFC3339, file.ModifiedTime); err == nil {
					node.Updated = t.Unix()
				}
			}

			if len(file.Owners) > 0 {
				node.Owner = file.Owners[0].DisplayName
			}

			nodes.AddWhere(pattern, 0, node)
		}

		pageToken = result.NextPageToken
		if pageToken == "" {
			break
		}
	}

	return
}

// ListRecursive returns the list of objects recursively
func (fs *GoogleDriveFileSysClient) ListRecursive(uri string) (nodes FileNodes, err error) {
	// If fileID is set, listing is not supported
	if fs.fileID != "" {
		return nodes, g.Error("Listing is not supported when FILE_ID is set. FILE_ID is for direct file access only.")
	}

	path, err := fs.GetPath(uri)
	if err != nil {
		return
	}

	pattern, err := makeGlob(NormalizeURI(fs, uri))
	if err != nil {
		err = g.Error(err, "Error parsing url pattern: "+uri)
		return
	}

	ts := fs.GetRefTs().Unix()
	maxItems := lo.Ternary(recursiveLimit == 0, 10000, recursiveLimit)

	// Variables to track the folder we'll list
	var folderID string
	var fileID string

	// Check if path exists and whether it's a file or folder
	if path != "" && path != "/" {
		fileID, err = fs.getFileID(path)
		if err != nil {
			// If not found, return empty list
			if strings.Contains(err.Error(), "not found") {
				return nodes, nil
			}
			return
		}

		g.Trace("Got fileID '%s' for path '%s'", fileID, path)

		// Get file info to check if it's a file or folder
		var file *drive.File
		file, err = fs.client.Files.Get(fileID).Fields("id, name, size, createdTime, modifiedTime, mimeType, owners").SupportsAllDrives(true).Do()
		if err != nil {
			// If file doesn't exist (404), return empty list
			if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == 404 {
				g.Trace("File ID '%s' not found (404), returning empty list", fileID)
				return nodes, nil
			}
			return nodes, g.Error(err, "Could not get file info")
		}

		isDir := file.MimeType == "application/vnd.google-apps.folder"

		// If it's a file (not a directory), return just this single file
		if !isDir {
			node := FileNode{
				URI:   fs.Prefix() + path,
				IsDir: false,
				Size:  uint64(file.Size),
			}

			if file.CreatedTime != "" {
				if t, err := time.Parse(time.RFC3339, file.CreatedTime); err == nil {
					node.Created = t.Unix()
				}
			}

			if file.ModifiedTime != "" {
				if t, err := time.Parse(time.RFC3339, file.ModifiedTime); err == nil {
					node.Updated = t.Unix()
				}
			}

			if len(file.Owners) > 0 {
				node.Owner = file.Owners[0].DisplayName
			}

			nodes.Add(node)
			return nodes, nil
		}

		// It's a directory, proceed with recursive listing
		folderID = fileID
	} else {
		// Root path or empty path
		folderID = "root"
		if fs.rootFolderID != "" {
			folderID = fs.rootFolderID
		}
	}

	// Use a stack for iterative traversal
	type folderItem struct {
		id   string
		path string
	}

	stack := []folderItem{{id: folderID, path: path}}

	processedCount := 0

	for len(stack) > 0 && processedCount < maxItems {
		// Pop from stack
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// List files in current folder
		query := fmt.Sprintf("'%s' in parents and trashed=false", current.id)
		fields := googleapi.Field("files(id, name, size, createdTime, modifiedTime, mimeType, owners)")

		pageToken := ""
		for processedCount < maxItems {
			call := fs.client.Files.List().Q(query).Fields(fields).PageSize(100).IncludeItemsFromAllDrives(true).SupportsAllDrives(true)
			if pageToken != "" {
				call = call.PageToken(pageToken)
			}

			result, err := call.Do()
			if err != nil {
				return nodes, g.Error(err, "Could not list files")
			}

			for _, file := range result.Files {
				if processedCount >= maxItems {
					g.Warn("Google Drive list limited at %d items. Set SLING_RECURSIVE_LIMIT to increase.", maxItems)
					return nodes, nil
				}

				isDir := file.MimeType == "application/vnd.google-apps.folder"

				nodePath := current.path
				if nodePath == "" || nodePath == "/" {
					nodePath = "/" + file.Name
				} else {
					nodePath = strings.TrimSuffix(current.path, "/") + "/" + file.Name
				}

				if isDir {
					// Add to stack for traversal
					stack = append(stack, folderItem{id: file.Id, path: nodePath})
					nodePath += "/"
				}

				node := FileNode{
					URI:   fs.Prefix() + nodePath,
					IsDir: isDir,
					Size:  uint64(file.Size),
				}

				if file.CreatedTime != "" {
					if t, terr := time.Parse(time.RFC3339, file.CreatedTime); terr == nil {
						node.Created = t.Unix()
					}
				}

				if file.ModifiedTime != "" {
					if t, terr := time.Parse(time.RFC3339, file.ModifiedTime); terr == nil {
						node.Updated = t.Unix()
					}
				}

				if len(file.Owners) > 0 {
					node.Owner = file.Owners[0].DisplayName
				}

				nodes.AddWhere(pattern, ts, node)
				processedCount++
			}

			pageToken = result.NextPageToken
			if pageToken == "" {
				break
			}
		}
	}

	return
}

// delete removes the specified path
func (fs *GoogleDriveFileSysClient) delete(uri string) (err error) {
	path, err := fs.GetPath(uri)
	if err != nil {
		return
	}

	fileID, err := fs.getFileID(path)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			// File doesn't exist, not an error
			return nil
		}
		return g.Error(err, "Could not get file ID for deletion")
	}

	err = fs.client.Files.Delete(fileID).Do()
	if err != nil {
		return g.Error(err, "Could not delete file: "+path)
	}

	return nil
}

// MkdirAll creates a directory path and all parents that don't exist
func (fs *GoogleDriveFileSysClient) MkdirAll(path string) (err error) {
	if path == "" || path == "/" {
		return nil
	}

	_, err = fs.getOrCreateFolder(path)
	return err
}

// getOrCreateFolder gets or creates a folder by path
func (fs *GoogleDriveFileSysClient) getOrCreateFolder(path string) (folderID string, err error) {
	path = strings.TrimPrefix(path, "/")
	path = strings.TrimSuffix(path, "/")

	if path == "" {
		if fs.rootFolderID != "" {
			return fs.rootFolderID, nil
		}
		return "root", nil
	}

	parts := strings.Split(path, "/")
	parentID := "root"
	if fs.rootFolderID != "" {
		parentID = fs.rootFolderID
	}

	for _, part := range parts {
		if part == "" {
			continue
		}

		// Check if folder exists
		query := fmt.Sprintf("name='%s' and '%s' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false", part, parentID)
		result, err := fs.client.Files.List().Q(query).Fields("files(id)").IncludeItemsFromAllDrives(true).SupportsAllDrives(true).Do()
		if err != nil {
			return "", g.Error(err, "Could not search for folder: "+part)
		}

		if len(result.Files) > 0 {
			// Folder exists
			parentID = result.Files[0].Id
		} else {
			// Create folder
			folder := &drive.File{
				Name:     part,
				MimeType: "application/vnd.google-apps.folder",
				Parents:  []string{parentID},
			}

			created, err := fs.client.Files.Create(folder).Fields("id").Do()
			if err != nil {
				return "", g.Error(err, "Could not create folder: "+part)
			}

			parentID = created.Id
		}
	}

	return parentID, nil
}

// splitPath splits a path into directory and filename
func splitPath(p string) (dir, file string) {
	p = strings.TrimPrefix(p, "/")
	p = strings.TrimSuffix(p, "/")

	if p == "" {
		return "", ""
	}

	lastSlash := strings.LastIndex(p, "/")
	if lastSlash == -1 {
		return "", p
	}

	return p[:lastSlash], p[lastSlash+1:]
}
