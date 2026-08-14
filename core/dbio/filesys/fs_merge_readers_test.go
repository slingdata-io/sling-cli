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
	contents   map[string][]byte
	idleWindow time.Duration
	readPause  time.Duration

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

	fs.mu.Lock()
	fs.openCount++
	fs.curOpen++
	if fs.curOpen > fs.maxOpen {
		fs.maxOpen = fs.curOpen
	}
	fs.mu.Unlock()

	return &idleTrackingReader{
		data:       data,
		uri:        uri,
		openedAt:   time.Now(),
		idleWindow: fs.idleWindow,
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

// TestMergeReadersIdleBoundedIssue789 guards issue #789. MergeReaders used to
// open every file's body up front while the consumer read one at a time.
func TestMergeReadersIdleBoundedIssue789(t *testing.T) {
	const numFiles = 40
	const rowsPerFile = 200

	fs, nodes := newIdleSensitiveFS(t, numFiles, rowsPerFile, time.Hour, idleCSV)

	count, err := readAll(t, fs, nodes, dbio.FileTypeCsv)
	assert.NoError(t, err)
	assert.Equal(t, numFiles*rowsPerFile, count)

	maxOpen, maxIdle, maxIdleURI, curOpen := fs.snapshot()
	t.Logf("max open at once: %d, longest idle: %v (%s)", maxOpen, maxIdle.Round(time.Millisecond), maxIdleURI)

	// current file plus at most one transition. 20 idle bodies is the old bug.
	assert.LessOrEqual(t, maxOpen, 2, "too many bodies open at once: %d", maxOpen)
	assert.Less(t, maxIdle, time.Second,
		"a body idled %v before its first read; open the body at first read",
		maxIdle.Round(time.Millisecond))
	assert.Equal(t, 0, curOpen, "bodies must be closed after use")
}

// TestMergeReadersIdleDoesNotGrowWithFileCount is the core of the #789 fix:
// a larger unload must not mean a longer idle wait.
func TestMergeReadersIdleDoesNotGrowWithFileCount(t *testing.T) {
	measure := func(numFiles int) time.Duration {
		fs, nodes := newIdleSensitiveFS(t, numFiles, 200, time.Hour, idleCSV)

		count, err := readAll(t, fs, nodes, dbio.FileTypeCsv)
		assert.NoError(t, err)
		assert.Equal(t, numFiles*200, count)

		_, maxIdle, _, _ := fs.snapshot()
		return maxIdle
	}

	small := measure(10)
	large := measure(40)

	t.Logf("longest idle: 10 files=%v, 40 files=%v",
		small.Round(time.Millisecond), large.Round(time.Millisecond))

	// both must stay far below a reset window; do not ratio two near-zero times
	assert.Less(t, small, 200*time.Millisecond, "10-file idle %v is not bounded", small)
	assert.Less(t, large, 200*time.Millisecond, "40-file idle %v is not bounded", large)
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

// TestMergeReadersSlowConsumerDoesNotResetLookahead fails if the next body
// is opened while the current file is still being read. Prefetch of live
// bodies sits idle for the pause (2s) and trips the 1s window.
func TestMergeReadersSlowConsumerDoesNotResetLookahead(t *testing.T) {
	const numFiles = 4
	const rowsPerFile = 200
	const pause = 2 * time.Second
	const idleWindow = time.Second

	fs, nodes := newIdleSensitiveFS(t, numFiles, rowsPerFile, idleWindow, idleCSV)
	fs.readPause = pause

	count, err := readAll(t, fs, nodes, dbio.FileTypeCsv)

	assert.NoError(t, err, "look-ahead opened a body that sat idle for %v", pause)
	assert.Equal(t, numFiles*rowsPerFile, count)

	maxOpen, maxIdle, maxIdleURI, _ := fs.snapshot()
	t.Logf("max open at once: %d, longest idle: %v (%s)", maxOpen, maxIdle.Round(time.Millisecond), maxIdleURI)

	assert.LessOrEqual(t, maxOpen, 2, "look-ahead opened extra bodies: maxOpen=%d", maxOpen)
	assert.Less(t, maxIdle, idleWindow,
		"a body idled %v; open the body at first read", maxIdle.Round(time.Millisecond))
}

func TestMergeReadersJsonLinesIdleBounded(t *testing.T) {
	const numFiles = 10
	const rowsPerFile = 50

	fs, nodes := newIdleSensitiveFS(t, numFiles, rowsPerFile, time.Second, idleJSONL)

	count, err := readAll(t, fs, nodes, dbio.FileTypeJsonLines)
	assert.NoError(t, err)
	assert.Equal(t, numFiles*rowsPerFile, count)

	maxOpen, maxIdle, _, curOpen := fs.snapshot()
	assert.LessOrEqual(t, maxOpen, 2, "too many JSONL bodies open at once: %d", maxOpen)
	assert.Less(t, maxIdle, time.Second, "JSONL body idled %v", maxIdle.Round(time.Millisecond))
	assert.Equal(t, 0, curOpen, "JSONL bodies must be closed after use")
}

// TestMergeReadersXmlOpensNearPointOfUse covers the pipe path. Producers used
// to call GetReader for every file, then copy one by one.
func TestMergeReadersXmlOpensNearPointOfUse(t *testing.T) {
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

	assert.LessOrEqual(t, maxOpen, 2, "XML pipe path opened %d bodies at once", maxOpen)
}
