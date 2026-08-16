package filesys

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/flarco/g"
	"github.com/slingdata-io/sling-cli/core/dbio"
	"github.com/slingdata-io/sling-cli/core/dbio/iop"
	"github.com/stretchr/testify/assert"
)

// idleTrackingReader emulates an S3 body: it records the idle time before the
// first read, and fails like a reset connection once that exceeds the window.
type idleTrackingReader struct {
	data        []byte
	pos         int
	uri         string
	openedAt    time.Time
	idleWindow  time.Duration
	firstRead   bool
	dead        bool
	closed      bool
	forceClosed bool
	dieAt       int // >0: die like a reset connection after this many bytes
	fs          *idleSensitiveFS
}

func (r *idleTrackingReader) close() {
	if r.closed {
		return
	}
	r.closed = true

	r.fs.mu.Lock()
	r.fs.curOpen--
	r.fs.mu.Unlock()
}

func (r *idleTrackingReader) Close() error {
	r.forceClosed = true
	r.close()
	return nil
}

func (r *idleTrackingReader) Read(p []byte) (int, error) {
	// Close() on an SFTP pipe makes a later Read fail this way.
	// EOF cleanup must not; Peek reads to EOF then reads again.
	if r.forceClosed {
		return 0, io.ErrClosedPipe
	}

	if !r.firstRead {
		r.firstRead = true
		idle := time.Since(r.openedAt)

		r.fs.mu.Lock()
		if idle > r.fs.maxIdle {
			r.fs.maxIdle = idle
			r.fs.maxIdleURI = r.uri
		}
		r.fs.mu.Unlock()

		if idle > r.idleWindow {
			r.dead = true
		}

		// pause after the first read so a body that was opened too early
		// sits idle while this file is consumed (Start() also hits this).
		if r.fs.readPause > 0 {
			time.Sleep(r.fs.readPause)
		}
	}

	if r.dead {
		return 0, fmt.Errorf("read tcp 10.0.0.1:54567->52.216.0.1:443: read: connection reset by peer")
	}
	if r.dieAt > 0 && r.pos >= r.dieAt {
		r.close()
		return 0, fmt.Errorf("read tcp 10.0.0.1:54567->52.216.0.1:443: read: connection reset by peer")
	}
	if r.pos >= len(r.data) {
		r.close()
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// idleSensitiveFS hands back bodies that go stale if not consumed promptly.
type idleSensitiveFS struct {
	LocalFileSysClient
	contents      map[string][]byte
	idleWindow    time.Duration
	readPause     time.Duration
	openPause     time.Duration   // latency per GetReader, like a remote store
	dieAfterBytes int             // >0: the first body per URI dies mid-stream
	killed        map[string]bool // URIs whose first body already died

	mu         sync.Mutex
	openCount  int
	curOpen    int
	maxOpen    int
	maxIdle    time.Duration
	maxIdleURI string
}

func (fs *idleSensitiveFS) Init(ctx context.Context) (err error) {
	var instance FileSysClient = fs
	fs.BaseFileSysClient.instance = &instance
	fs.BaseFileSysClient.context = g.NewContext(ctx)
	fs.BaseFileSysClient.fsType = dbio.TypeFileS3
	fs.BaseFileSysClient.properties = map[string]string{}
	return
}

func (fs *idleSensitiveFS) GetReader(uri string) (io.Reader, error) {
	data, ok := fs.contents[uri]
	if !ok {
		return nil, fmt.Errorf("no such object: %s", uri)
	}

	if fs.openPause > 0 {
		time.Sleep(fs.openPause)
	}

	fs.mu.Lock()
	fs.openCount++
	fs.curOpen++
	if fs.curOpen > fs.maxOpen {
		fs.maxOpen = fs.curOpen
	}
	dieAt := 0
	if fs.dieAfterBytes > 0 && !fs.killed[uri] {
		if fs.killed == nil {
			fs.killed = map[string]bool{}
		}
		fs.killed[uri] = true
		dieAt = fs.dieAfterBytes
	}
	fs.mu.Unlock()

	return &idleTrackingReader{
		data:       data,
		uri:        uri,
		openedAt:   time.Now(),
		idleWindow: fs.idleWindow,
		dieAt:      dieAt,
		fs:         fs,
	}, nil
}

type idleFileKind int

const (
	idleCSV idleFileKind = iota
	idleJSONL
	idleXML
)

func newIdleSensitiveFS(t *testing.T, numFiles, rowsPerFile int, idleWindow time.Duration, kind idleFileKind) (*idleSensitiveFS, FileNodes) {
	t.Helper()

	contents := map[string][]byte{}
	nodes := FileNodes{}
	for i := 0; i < numFiles; i++ {
		var uri string
		var buf bytes.Buffer
		switch kind {
		case idleJSONL:
			uri = fmt.Sprintf("s3://test-bucket/unload/u01-%04d_part_00.jsonl", i)
			for r := 0; r < rowsPerFile; r++ {
				fmt.Fprintf(&buf, "{\"id\":%d,\"name\":\"name_%d_%d\"}\n", i*rowsPerFile+r, i, r)
			}
		case idleXML:
			uri = fmt.Sprintf("s3://test-bucket/unload/u01-%04d_part_00.xml", i)
			buf.WriteString("<root>\n")
			for r := 0; r < rowsPerFile; r++ {
				fmt.Fprintf(&buf, "<row><id>%d</id><name>name_%d_%d</name></row>\n", i*rowsPerFile+r, i, r)
			}
			buf.WriteString("</root>\n")
		default:
			uri = fmt.Sprintf("s3://test-bucket/unload/u01-%04d_part_00.csv", i)
			buf.WriteString("id,name\n")
			for r := 0; r < rowsPerFile; r++ {
				fmt.Fprintf(&buf, "%d,name_%d_%d\n", i*rowsPerFile+r, i, r)
			}
		}

		contents[uri] = buf.Bytes()
		nodes = append(nodes, FileNode{URI: uri, Size: uint64(buf.Len())})
	}

	fs := &idleSensitiveFS{contents: contents, idleWindow: idleWindow}
	if err := fs.Init(context.Background()); err != nil {
		t.Fatalf("init: %v", err)
	}
	fs.SetProp("url", "s3://test-bucket/unload")

	return fs, nodes
}

func readAll(t *testing.T, fs *idleSensitiveFS, nodes FileNodes, fileType dbio.FileType) (count int, err error) {
	t.Helper()

	ds, err := MergeReaders(fs, fileType, nodes, iop.FileStreamConfig{})
	if err != nil {
		t.Fatalf("MergeReaders: %v", err)
	}

	for range ds.Rows() {
		count++
	}

	return count, ds.Err()
}

func (fs *idleSensitiveFS) snapshot() (maxOpen int, maxIdle time.Duration, maxIdleURI string, curOpen int) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.maxOpen, fs.maxIdle, fs.maxIdleURI, fs.curOpen
}

// mergeConcurrency mirrors the concurrency pick in MergeReaders for the
// mock FS (S3 type, small node counts).
const mergeConcurrency = 10

// TestMergeReadersOpenBoundIssue789 guards issue #789's resource side.
// Look-ahead is allowed, but the open-body count must stay bounded by the
// prefetch window, not grow with the file count.
func TestMergeReadersOpenBoundIssue789(t *testing.T) {
	const numFiles = 40
	const rowsPerFile = 200

	fs, nodes := newIdleSensitiveFS(t, numFiles, rowsPerFile, time.Hour, idleCSV)

	count, err := readAll(t, fs, nodes, dbio.FileTypeCsv)
	assert.NoError(t, err)
	assert.Equal(t, numFiles*rowsPerFile, count)

	maxOpen, maxIdle, maxIdleURI, curOpen := fs.snapshot()
	t.Logf("max open at once: %d, longest idle: %v (%s)", maxOpen, maxIdle.Round(time.Millisecond), maxIdleURI)

	// workers + channel buffer + consumer transition; 40 open bodies is the old bug
	assert.LessOrEqual(t, maxOpen, 2*mergeConcurrency+4, "too many bodies open at once: %d", maxOpen)
	assert.Equal(t, 0, curOpen, "bodies must be closed after use")
}

// TestMergeReadersOpensArePipelined guards the staging timeout regression
// (exec 3HuEiSRkALXQERuORc7KDWC5XAn): with per-open latency, opens must
// overlap. A serial open of 30 files at 50ms each takes 1.5s+.
func TestMergeReadersOpensArePipelined(t *testing.T) {
	const numFiles = 30
	const rowsPerFile = 50
	const openPause = 50 * time.Millisecond

	fs, nodes := newIdleSensitiveFS(t, numFiles, rowsPerFile, time.Hour, idleCSV)
	fs.openPause = openPause

	started := time.Now()
	count, err := readAll(t, fs, nodes, dbio.FileTypeCsv)
	elapsed := time.Since(started)

	assert.NoError(t, err)
	assert.Equal(t, numFiles*rowsPerFile, count)

	serial := time.Duration(numFiles) * openPause
	t.Logf("elapsed: %v (serial would be %v+)", elapsed.Round(time.Millisecond), serial)
	assert.Less(t, elapsed, serial/2, "opens are not pipelined: %v", elapsed.Round(time.Millisecond))
}

// TestMergeReadersResumesAfterMidStreamReset covers the reopen path with a
// non-zero offset: a body that dies mid-file is reopened and skipped to the
// bytes already delivered, with no loss and no duplicates.
func TestMergeReadersResumesAfterMidStreamReset(t *testing.T) {
	const numFiles = 3
	const rowsPerFile = 500

	fs, nodes := newIdleSensitiveFS(t, numFiles, rowsPerFile, time.Hour, idleCSV)
	fs.dieAfterBytes = 1000 // dies mid-file; each file is ~8KB

	count, err := readAll(t, fs, nodes, dbio.FileTypeCsv)

	assert.NoError(t, err, "stream must resume after a mid-stream reset")
	assert.Equal(t, numFiles*rowsPerFile, count, "rows must not be lost or duplicated")
}

// TestMergeReadersSurvivesIdleReset is the end-to-end guard: with a window
// that a reset would trip, the stream must still complete.
func TestMergeReadersSurvivesIdleReset(t *testing.T) {
	const numFiles = 40
	const rowsPerFile = 200

	fs, nodes := newIdleSensitiveFS(t, numFiles, rowsPerFile, time.Second, idleCSV)
	fs.readPause = 100 * time.Millisecond

	count, err := readAll(t, fs, nodes, dbio.FileTypeCsv)

	assert.NoError(t, err, "stream must not die from a reset idle body")
	assert.Equal(t, numFiles*rowsPerFile, count, "all rows should be read")
}

// TestMergeReadersSlowConsumerRecoversResetLookahead: a slow consumer (2s
// per file) lets prefetched bodies idle past the 1s reset window. The
// stream must recover each reset body by a reopen at first read.
func TestMergeReadersSlowConsumerRecoversResetLookahead(t *testing.T) {
	const numFiles = 4
	const rowsPerFile = 200
	const pause = 2 * time.Second
	const idleWindow = time.Second

	fs, nodes := newIdleSensitiveFS(t, numFiles, rowsPerFile, idleWindow, idleCSV)
	fs.readPause = pause

	count, err := readAll(t, fs, nodes, dbio.FileTypeCsv)

	assert.NoError(t, err, "a look-ahead body that sat idle for %v must be reopened", pause)
	assert.Equal(t, numFiles*rowsPerFile, count)
}

func TestMergeReadersJsonLinesResetRecovered(t *testing.T) {
	const numFiles = 10
	const rowsPerFile = 50

	fs, nodes := newIdleSensitiveFS(t, numFiles, rowsPerFile, time.Second, idleJSONL)

	count, err := readAll(t, fs, nodes, dbio.FileTypeJsonLines)
	assert.NoError(t, err)
	assert.Equal(t, numFiles*rowsPerFile, count)

	maxOpen, _, _, curOpen := fs.snapshot()
	assert.LessOrEqual(t, maxOpen, 2*mergeConcurrency+4, "too many JSONL bodies open at once: %d", maxOpen)
	assert.Equal(t, 0, curOpen, "JSONL bodies must be closed after use")
}

// TestMergeReadersXmlOpenBound covers the pipe path: open bodies stay
// bounded by the prefetch window.
func TestMergeReadersXmlOpenBound(t *testing.T) {
	const numFiles = 20
	const rowsPerFile = 10

	fs, nodes := newIdleSensitiveFS(t, numFiles, rowsPerFile, time.Hour, idleXML)

	_, err := readAll(t, fs, nodes, dbio.FileTypeXml)
	assert.NoError(t, err)

	maxOpen, _, _, _ := fs.snapshot()
	t.Logf("xml max open at once: %d (openCount=%d)", maxOpen, func() int {
		fs.mu.Lock()
		defer fs.mu.Unlock()
		return fs.openCount
	}())

	assert.LessOrEqual(t, maxOpen, 2*mergeConcurrency+4, "XML pipe path opened %d bodies at once", maxOpen)
}
