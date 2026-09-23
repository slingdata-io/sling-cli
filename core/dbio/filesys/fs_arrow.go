package filesys

import (
	"context"
	"io"
	"os"
	"path"
	"runtime/debug"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	parquetfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/slingdata-io/sling-cli/core/env"
)

// This file reads parquet and arrow files as Arrow records, for the arrow
// lane. One file is one datastream, and the dataflow carries one datastream
// per file, so MergeDataflow is never called on the lane: the ADBC ingest and
// the staged loaders iterate df.StreamCh.

// ListFileNodes lists the files ReadDataflow reads for url: one node when the
// config names a table or a query, the selected prefixes, or the recursive
// listing of the path.
func ListFileNodes(fs FileSysClient, url string, cfg iop.FileStreamConfig) (nodes FileNodes, err error) {
	if g.In(cfg.Format, dbio.FileTypeIceberg, dbio.FileTypeDelta) || cfg.SQL != "" {
		return FileNodes{FileNode{URI: url}}, nil
	}

	if prefixes := cfg.FileSelect; len(prefixes) > 0 {
		// Check if any FileSelect entries are full URIs with scheme prefix.
		// If so, they may reference different buckets (multi-bucket access).
		fullURIPrefixes := []string{}
		relativePrefixes := []string{}

		for _, prefix := range prefixes {
			if strings.Contains(prefix, "://") {
				fullURIPrefixes = append(fullURIPrefixes, prefix)
			} else {
				relativePrefixes = append(relativePrefixes, prefix)
			}
		}

		// Handle full URI prefixes (may be from different buckets)
		for _, uri := range fullURIPrefixes {
			g.Trace("listing path (full URI): %s", uri)
			uriNodes, err := fs.Self().ListRecursive(uri)
			if err != nil {
				return nil, g.Error(err, "Error getting paths for %s", uri)
			}
			nodes = append(nodes, uriNodes...)
		}

		// Handle relative prefixes (original behavior)
		if len(relativePrefixes) > 0 {
			rootPath := GetDeepestPartitionParent(url)
			g.Trace("listing path: %s", rootPath)
			pathNodes, err := fs.Self().ListRecursive(rootPath)
			if err != nil {
				return nil, g.Error(err, "Error getting paths")
			}
			// select only prefixes
			nodes = append(nodes, pathNodes.SelectWithPrefix(relativePrefixes...)...)
		}

		return nodes, nil
	}

	g.Trace("listing path: %s", url)
	nodes, err = fs.Self().ListRecursive(url)
	if err != nil {
		return nil, g.Error(err, "Error getting paths")
	}

	return nodes, nil
}

// ArrowFileSet is the parquet or arrow files of one source stream, with the
// footer schema they all share. A remote file is copied to a local temp file
// once, so the up-front footer check and the read share one copy.
type ArrowFileSet struct {
	fs     FileSysClient
	format dbio.FileType
	uris   []string
	paths  []string
	temps  []string // the temp copy of each remote file, "" for a local file
	schema *arrow.Schema
}

// NewArrowFileSet lists the files of url, reads every file's footer schema,
// and checks that all of them match the first. The check runs before any
// record is read, so a schema drift declines the whole stream before a write.
// A select that names a column no file carries fails here too, before the
// dataflow starts.
//
// It returns a reason (not an error) when the files do not share one schema:
// the caller logs its decline line and takes the row path.
func NewArrowFileSet(fs FileSysClient, url string, cfg iop.FileStreamConfig) (set *ArrowFileSet, reason string, err error) {
	nodes, err := ListFileNodes(fs, url, cfg)
	if err != nil {
		return nil, "", err
	}

	format := cfg.Format
	if format == dbio.FileTypeNone {
		format = nodes.InferFormat()
	}

	if len(nodes.Files()) == 0 {
		return nil, "", g.Error("Provided 0 files for: %#v", nodes)
	}

	set = &ArrowFileSet{fs: fs, format: format}

	for _, node := range nodes.Files() {
		path, temp, err := set.materialize(node.URI)
		if err != nil {
			set.RemoveTemps()
			return nil, "", err
		}
		set.uris = append(set.uris, node.URI)
		set.paths = append(set.paths, path)
		set.temps = append(set.temps, temp)

		schema, err := arrowFileSchema(path, format)
		if err != nil {
			set.RemoveTemps()
			return nil, "", g.Error(err, "could not read the schema of %s", node.URI)
		}

		if set.schema == nil {
			set.schema = schema
		} else if !arrowSchemaEqual(set.schema, schema) {
			set.RemoveTemps()
			return nil, g.F("the arrow files do not share one schema: %s differs from the first file", node.URI), nil
		}
	}

	if _, _, _, err := arrowSelect(set.schema, cfg.Select); err != nil {
		set.RemoveTemps()
		return nil, "", err
	}

	return set, "", nil
}

// Schema returns the footer schema every file of the set shares.
func (s *ArrowFileSet) Schema() *arrow.Schema {
	return s.schema
}

// RemoveTemps removes the local copies of the remote files. The caller keeps
// them until the dataflow closes: the datastreams read from them.
func (s *ArrowFileSet) RemoveTemps() {
	for _, temp := range s.temps {
		if temp != "" {
			_ = os.Remove(temp)
		}
	}
	s.temps = nil
}

// Dataflow pushes one Arrow datastream per file into one dataflow. The records
// are pruned to cfg.Select and every stream stops after cfg.Limit rows. The
// column named maxKey is tracked for the incremental state, when it is read.
func (s *ArrowFileSet) Dataflow(lane iop.ArrowLane, cfg iop.FileStreamConfig, maxKey string) (df *iop.Dataflow, err error) {
	gctx := s.fs.Context()
	df = iop.NewDataflowContext(gctx.Ctx, cfg.Limit)
	dsCh := make(chan *iop.Datastream)

	s.fs.setDf(df)

	go func() {
		defer close(dsCh)

		for i, localPath := range s.paths {
			if df.Context.Ctx.Err() != nil {
				return
			}

			file, err := os.Open(localPath)
			if err != nil {
				df.Context.CaptureErr(g.Error(err, "could not open %s", localPath))
				return
			}

			recs, err := newArrowRecordReader(gctx.Ctx, file, s.format)
			if err != nil {
				file.Close()
				df.Context.CaptureErr(g.Error(err, "could not read the arrow file %s", s.uris[i]))
				return
			}

			ds, read, err := arrowFileDatastream(gctx, recs, lane, cfg.Select, cfg.Limit, maxKey)
			if err != nil {
				recs.release()
				df.Context.CaptureErr(g.Error(err, "could not read the arrow file %s", s.uris[i]))
				return
			}

			ds.SetMetadata(s.fs.GetProp("METADATA"))
			ds.Metadata.StreamURL.Value = s.uris[i]
			ds.SetConfig(s.fs.Props())

			if err := ds.Start(); err != nil {
				df.Context.CaptureErr(g.Error(err, "could not start the arrow stream of %s", s.uris[i]))
				return
			}

			select {
			case dsCh <- ds:
			case <-df.Context.Ctx.Done():
				ds.Close()
				return
			}

			// a local file is read one at a time, like the row path: the next
			// file opens once this one is read
			if s.fs.FsType() == dbio.TypeFileLocal {
				<-read
			}
		}
	}()

	go df.PushStreamChan(dsCh)

	if err = df.WaitReady(); err != nil {
		return df, g.Error(err)
	}

	return df, nil
}

// materialize returns the local path of a file: a local file is used as it is,
// a remote file is copied to one temp file, which the read then uses.
func (s *ArrowFileSet) materialize(uri string) (localPath string, temp string, err error) {
	if s.fs.FsType() == dbio.TypeFileLocal {
		localPath, err = s.fs.Self().GetPath(uri)
		return localPath, "", err
	}

	reader, err := s.fs.Self().GetReader(uri)
	if err != nil {
		return "", "", err
	}
	if closer, ok := reader.(io.Closer); ok {
		defer closer.Close()
	}

	file, err := os.CreateTemp(env.GetTempFolder(), "sling_arrow_src_*"+path.Ext(uri))
	if err != nil {
		return "", "", g.Error(err, "could not create a temp file for %s", uri)
	}

	if _, err = io.Copy(file, reader); err != nil {
		file.Close()
		os.Remove(file.Name())
		return "", "", g.Error(err, "could not copy %s locally", uri)
	}

	if err = file.Close(); err != nil {
		os.Remove(file.Name())
		return "", "", g.Error(err, "could not write %s locally", uri)
	}

	return file.Name(), file.Name(), nil
}

// arrowSchemaEqual reports whether two file schemas carry the same fields, in
// the same order, with the same types. The file metadata does not matter.
func arrowSchemaEqual(a, b *arrow.Schema) bool {
	if len(a.Fields()) != len(b.Fields()) {
		return false
	}

	for i, field := range a.Fields() {
		other := b.Field(i)
		if !strings.EqualFold(field.Name, other.Name) || !arrow.TypeEqual(field.Type, other.Type) {
			return false
		}
	}

	return true
}

// arrowFileSchema reads the footer schema of one parquet or arrow file.
func arrowFileSchema(path string, format dbio.FileType) (schema *arrow.Schema, err error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	switch format {
	case dbio.FileTypeParquet:
		pqFile, err := parquetfile.NewParquetReader(file)
		if err != nil {
			return nil, g.Error(err, "could not read parquet file")
		}
		defer pqFile.Close()

		reader, err := pqarrow.NewFileReader(pqFile, pqarrow.ArrowReadProperties{Parallel: true}, memory.NewGoAllocator())
		if err != nil {
			return nil, g.Error(err, "could not read the parquet footer")
		}

		return reader.Schema()
	case dbio.FileTypeArrow:
		return iop.ArrowIPCFileSchema(file)
	}

	return nil, g.Error("unsupported arrow file format: %s", format)
}

// arrowRecordReader yields the records of one open Arrow file. It owns the
// reader and the file: release closes both.
type arrowRecordReader struct {
	schema  *arrow.Schema
	next    func() (arrow.RecordBatch, error) // io.EOF at the end
	release func()
}

// newArrowRecordReader opens the record reader of one parquet or arrow file.
func newArrowRecordReader(ctx context.Context, file *os.File, format dbio.FileType) (r *arrowRecordReader, err error) {
	mem := memory.NewGoAllocator()

	switch format {
	case dbio.FileTypeParquet:
		pqFile, err := parquetfile.NewParquetReader(file)
		if err != nil {
			return nil, g.Error(err, "could not read parquet file")
		}

		pqReader, err := pqarrow.NewFileReader(pqFile, pqarrow.ArrowReadProperties{Parallel: true}, mem)
		if err != nil {
			return nil, g.Error(err, "could not open the parquet reader")
		}

		schema, err := pqReader.Schema()
		if err != nil {
			return nil, g.Error(err, "could not read the parquet schema")
		}

		recs, err := pqReader.GetRecordReader(ctx, nil, nil)
		if err != nil {
			return nil, g.Error(err, "could not read the parquet records")
		}

		return &arrowRecordReader{
			schema: schema,
			next: func() (arrow.RecordBatch, error) {
				// the parquet reader releases its own record on the next Read,
				// so the caller gets its own reference
				rec, err := recs.Read()
				if err != nil {
					return nil, err
				}
				rec.Retain()
				return rec, nil
			},
			release: func() { recs.Release(); pqFile.Close(); file.Close() },
		}, nil
	case dbio.FileTypeArrow:
		// The file format carries an ARROW1 footer; sling writes the stream
		// format because its writer pipes into a non-seekable sink. Both read
		// back the same records, so try the file format first.
		fileReader, fileErr := ipc.NewFileReader(file)
		if fileErr == nil {
			i := 0
			return &arrowRecordReader{
				schema: fileReader.Schema(),
				next: func() (arrow.RecordBatch, error) {
					if i >= fileReader.NumRecords() {
						return nil, io.EOF
					}
					rec, err := fileReader.RecordBatchAt(i)
					i++
					return rec, err
				},
				release: func() { fileReader.Close(); file.Close() },
			}, nil
		}

		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return nil, g.Error(err, "could not rewind arrow file")
		}

		streamReader, err := ipc.NewReader(file)
		if err != nil {
			return nil, g.Error(fileErr, "could not read arrow file")
		}

		return &arrowRecordReader{
			schema: streamReader.Schema(),
			next: func() (arrow.RecordBatch, error) {
				if !streamReader.Next() {
					if err := streamReader.Err(); err != nil {
						return nil, err
					}
					return nil, io.EOF
				}
				// the reader owns the record until the next Next, and the
				// stream releases what it takes
				rec := streamReader.RecordBatch()
				rec.Retain()
				return rec, nil
			},
			release: func() { streamReader.Release(); file.Close() },
		}, nil
	}

	return nil, g.Error("unsupported arrow file format: %s", format)
}

// arrowFileDatastream builds an Arrow datastream from one file's records. The
// reader is released when the datastream closes. The records are pruned to the
// selected columns and the stream stops after limit rows (0 is no limit).
//
// read is closed once every record of the file has been pushed: the caller uses
// it to read the next local file, the way the row path reads one file at a
// time.
func arrowFileDatastream(gctx *g.Context, recs *arrowRecordReader, lane iop.ArrowLane, selected []string, limit int, maxKey string) (ds *iop.Datastream, read <-chan struct{}, err error) {
	schema, cols, keep, err := arrowSelect(recs.schema, selected)
	if err != nil {
		return nil, nil, err
	}

	rs := iop.NewRecordStream(gctx, lane, schema, iop.ArrowLaneBuffer)
	ds = iop.NewDatastreamArrow(gctx.Ctx, cols, rs)
	ds.Defer(recs.release)

	// closed by the producer, after the last push, so the signal is race-free
	readCh := make(chan struct{})

	if maxKey != "" {
		for i, field := range schema.Fields() {
			if strings.EqualFold(field.Name, maxKey) {
				rs.TrackMax(i)
				break
			}
		}
	}

	go func() {
		defer close(readCh)
		defer func() {
			if r := recover(); r != nil {
				rs.Close(g.Error("panic occurred! %#v\n%s", r, string(debug.Stack())))
			}
		}()

		remaining := limit

		for {
			rec, err := recs.next()
			if err == io.EOF {
				break
			} else if err != nil {
				rs.Close(g.Error(err, "could not read an arrow record"))
				return
			}

			if len(keep) > 0 && len(keep) < int(rec.NumCols()) {
				rec = prunedArrowRecord(rec, keep)
			}

			if limit > 0 {
				if remaining <= 0 {
					rec.Release()
					break
				}
				if rec.NumRows() > int64(remaining) {
					sliced := rec.NewSlice(0, int64(remaining))
					rec.Release()
					rec = sliced
					remaining = 0
				} else {
					remaining -= int(rec.NumRows())
				}
			}

			// Push takes the record: it releases it when the stream is closed
			if err := rs.Push(rec); err != nil {
				rs.Close(err)
				return
			}
		}

		rs.Close(nil)
	}()

	return ds, readCh, nil
}

// arrowSelect returns the stream schema, the Sling columns and the source
// column indices of the selected column names, in the given order. No names
// keeps every column, in file order.
func arrowSelect(schema *arrow.Schema, selected []string) (out *arrow.Schema, cols iop.Columns, keep []int, err error) {
	cols = iop.ArrowSchemaToColumns(schema)
	if len(selected) == 0 {
		return schema, cols, nil, nil
	}

	index := map[string]int{}
	for i, col := range cols {
		index[strings.ToLower(col.Name)] = i
	}

	fields := make([]arrow.Field, 0, len(selected))
	kept := make(iop.Columns, 0, len(selected))
	for _, name := range selected {
		i, ok := index[strings.ToLower(name)]
		if !ok {
			return nil, nil, nil, g.Error("selected column '%s' not found", name)
		}
		keep = append(keep, i)
		fields = append(fields, schema.Field(i))
		kept = append(kept, cols[i])
	}

	return arrow.NewSchema(fields, nil), kept, keep, nil
}

// prunedArrowRecord returns a record with only the given columns, in the given
// order. The input record is released: the new record holds the columns.
func prunedArrowRecord(rec arrow.RecordBatch, keep []int) arrow.RecordBatch {
	fields := make([]arrow.Field, len(keep))
	arrays := make([]arrow.Array, len(keep))
	for i, idx := range keep {
		fields[i] = rec.Schema().Field(idx)
		arrays[i] = rec.Column(idx)
	}

	out := array.NewRecord(arrow.NewSchema(fields, nil), arrays, rec.NumRows())
	rec.Release()

	return out
}
