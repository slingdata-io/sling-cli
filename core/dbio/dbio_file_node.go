package dbio

import (
	"sort"
	"strings"
	"time"

	"github.com/flarco/g"
	"github.com/gobwas/glob"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/spf13/cast"
)

// FileNode represents a file node
type FileNode struct {
	URI      string      `json:"uri"`
	IsDir    bool        `json:"is_dir"`
	Size     uint64      `json:"size,omitempty"`
	Created  int64       `json:"created,omitempty"`
	Updated  int64       `json:"updated,omitempty"`
	Owner    string      `json:"owner,omitempty"`
	Columns  iop.Columns `json:"columns,omitempty"`
	Children FileNodes   `json:"children,omitempty"`

	typ  Type   // cached type
	path string // cached path
}

func (fn *FileNode) Name() string {
	parts := strings.Split(fn.Path(), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

// Type return the file type
func (fn *FileNode) Type() Type {

	if fn.typ.String() != "" {
		return fn.typ
	}

	fType, _, _, err := ParseURL(fn.URI)
	if g.LogError(err) {
		return ""
	}

	fn.typ = fType

	return fn.typ
}

// Path returns the path without the prefix
func (fn *FileNode) Path() string {

	if fn.path != "" {
		return fn.path
	}

	fType, _, path, err := ParseURL(fn.URI)
	if g.LogError(err) {
		return ""
	}

	if fType == TypeFileAzure {
		pathContainer := strings.Split(path, "/")[0]

		// remove container
		path = strings.TrimPrefix(path, pathContainer+"/")
	}

	fn.path = path
	fn.typ = fType

	return fn.path
}

// FileNodes represent file nodes
type FileNodes []FileNode

// AddWhere adds a new node to list if pattern matches name and after timestamp
func (fns *FileNodes) AddWhere(pattern *glob.Glob, after int64, ns ...FileNode) {
	nodes := *fns
	for i, n := range ns {
		if strings.HasSuffix(n.URI, "/") {
			ns[i].IsDir = true
		} else if ns[i].IsDir && !strings.HasSuffix(n.URI, "/") {
			ns[i].URI = n.URI + "/"
		}
		if pattern == nil || (*pattern).Match(n.Path()) {
			if after == 0 || ns[i].Updated == 0 || ns[i].Updated > after {
				nodes = append(nodes, ns[i])
			}
		}
	}
	*fns = nodes
}

// Add adds a new node to list
func (fns *FileNodes) Add(ns ...FileNode) {
	nodes := *fns
	for i, n := range ns {
		if strings.HasSuffix(n.URI, "/") {
			ns[i].IsDir = true
		} else if ns[i].IsDir && !strings.HasSuffix(n.URI, "/") {
			ns[i].URI = n.URI + "/"
		}
	}
	nodes = append(nodes, ns...)
	*fns = nodes
}

// URIs give a list of recursive uris
func (fns FileNodes) URIs() (uris []string) {
	for _, p := range fns {
		uris = append(uris, p.URI)
		if p.IsDir {
			uris = append(uris, p.Children.URIs()...)
		}
	}
	return uris
}

// Paths give a list of recursive paths
func (fns FileNodes) Paths() (paths []string) {
	for _, p := range fns {
		paths = append(paths, p.Path())
		if p.IsDir {
			paths = append(paths, p.Children.Paths()...)
		}
	}
	return paths
}

// Sort sorts the nodes
func (fns FileNodes) Sort() {
	sort.Slice(fns, func(i, j int) bool {
		val := func(n FileNode) string {
			return cast.ToString(!n.IsDir) + n.URI
		}
		return val(fns[i]) < val(fns[j])
	})
}

// After returns the nodes modified after provided time
func (fns FileNodes) After(ts time.Time) (nodes FileNodes) {
	for _, fn := range fns {
		lastModified := time.Unix(fn.Updated, 0)
		if ts.IsZero() || lastModified.IsZero() || lastModified.After(ts) {
			nodes = append(nodes, fn)
		}
	}
	return
}

// Columns returns a column map
func (fns FileNodes) Columns() map[string]iop.Column {
	columns := map[string]iop.Column{}
	for _, node := range fns {
		for _, column := range node.Columns {
			key := g.F("%s|%s", node.URI, strings.ToLower(column.Name))
			column.FileURI = node.URI
			columns[key] = column
		}
	}
	return columns
}
