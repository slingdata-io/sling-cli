package iop

import (
	"bufio"
	"io"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/flarco/g"
	"github.com/flarco/g/json"
	cmap "github.com/orcaman/concurrent-map/v2"
)

type Queue struct {
	Path    string        `json:"path"`
	File    *os.File      `json:"-"`
	Reader  *bufio.Reader `json:"-"`
	Writer  *bufio.Writer `json:"-"`
	mu      sync.Mutex    // protect concurrent access
	count   int
	reading bool // whether queue is in reading mode
	writing bool // whether queue is in writing mode
	keep    bool
}

var queues = cmap.New[*Queue]()

// NewQueue creates a new queue with a temporary file
func NewQueue(name string) (q *Queue, err error) {
	// make temp folder
	var tmpFile *os.File
	var keep bool
	if folder := os.Getenv("SLING_QUEUE_FOLDER"); folder != "" {
		keep = true // keep file, will be deleted by parent process
		if err := os.MkdirAll(folder, 0755); err != nil {
			return nil, g.Error(err, "could not create queue folder")
		}
		tmpFilePath := path.Join(folder, name+".queue")
		if g.PathExists(tmpFilePath) {
			if tmpFile, err = os.OpenFile(tmpFilePath, os.O_RDWR, 0755); err != nil {
				return nil, g.Error(err, "could not open queue file: %s", tmpFilePath)
			}
		} else {
			if tmpFile, err = os.OpenFile(tmpFilePath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0755); err != nil {
				return nil, g.Error(err, "could not create queue file: %s", tmpFilePath)
			}
		}
	} else {
		tmpFile, err = os.CreateTemp("", name+"_*.queue")
		if err != nil {
			return nil, g.Error(err, "failed to create temp file for queue")
		}
	}

	q = &Queue{
		Path:    tmpFile.Name(),
		File:    tmpFile,
		writing: true, // start in writing mode
		reading: false,
		keep:    keep,
	}

	q.Writer = bufio.NewWriter(tmpFile)

	g.Trace("using queue `%s` at %s", name, q.Path)

	queues.Set(name, q)
	return q, nil
}

// Append writes a line to the queue
func (q *Queue) Append(data any) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Cannot write if in reading mode
	if q.reading {
		return g.Error("queue is in reading mode, cannot write")
	}

	// Ensure we're in writing mode
	if !q.writing {
		if err := q.startWriting(); err != nil {
			return g.Error(err, "failed to start writing mode")
		}
	}

	// Always JSON encode the data to handle special characters and complex types properly
	encoded := g.Marshal(data)

	// Add a newline for record separation
	if !strings.HasSuffix(encoded, "\n") {
		encoded += "\n"
	}

	_, err := q.Writer.WriteString(encoded)
	if err != nil {
		return g.Error(err, "failed to write to queue")
	}
	q.count++

	// Flush after each write to ensure data is written to disk immediately
	return q.Writer.Flush()
}

// startWriting prepares the queue for writing
func (q *Queue) startWriting() error {
	// Cannot start writing if in reading mode
	if q.reading {
		return g.Error("queue is in reading mode, cannot switch to writing mode")
	}

	// Close existing file if open
	if q.File != nil {
		if q.Writer != nil {
			q.Writer.Flush() // Ensure data is flushed before closing
		}
		q.Writer = nil

		if err := q.File.Close(); err != nil {
			return g.Error(err, "failed to close file before starting writing")
		}
		q.File = nil
	}

	// Open file for writing (append mode)
	file, err := os.OpenFile(q.Path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return g.Error(err, "failed to open file for writing")
	}

	q.File = file
	q.Writer = bufio.NewWriter(file)
	q.writing = true

	return nil
}

// finishWriting completes the writing phase
func (q *Queue) finishWriting() error {
	if !q.writing {
		return nil // Already finished writing
	}

	if q.Writer != nil {
		if err := q.Writer.Flush(); err != nil {
			return g.Error(err, "failed to flush writer")
		}
		q.Writer = nil
	}

	if q.File != nil {
		// Make sure all data is synced to disk
		if err := q.File.Sync(); err != nil {
			return g.Error(err, "failed to sync file")
		}

		if err := q.File.Close(); err != nil {
			return g.Error(err, "failed to close file after writing")
		}
		q.File = nil
	}

	q.writing = false
	return nil
}

// startReading prepares the queue for reading
func (q *Queue) startReading() error {
	// Finish writing phase if active
	if q.writing {
		if err := q.finishWriting(); err != nil {
			return g.Error(err, "failed to finish writing before reading")
		}
	}

	// Close existing file if open
	if q.File != nil {
		q.Reader = nil
		if err := q.File.Close(); err != nil {
			return g.Error(err, "failed to close file before starting reading")
		}
		q.File = nil
	}

	// Open file for reading
	file, err := os.OpenFile(q.Path, os.O_RDONLY, 0644)
	if err != nil {
		return g.Error(err, "failed to open file for reading")
	}

	q.File = file
	q.Reader = bufio.NewReader(file)
	q.reading = true
	q.writing = false // Cannot write after reading starts

	return nil
}

// Next reads the next line from the queue
// Returns the line, a boolean indicating if there are more lines, and any error
func (q *Queue) Next() (any, bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Ensure we're in reading mode
	if !q.reading {
		if err := q.startReading(); err != nil {
			return nil, false, g.Error(err, "failed to start reading mode")
		}
	}

	// Read the next line
	line, err := q.Reader.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			// Check if there's any content before EOF
			if len(line) > 0 {
				// Process the last line without newline
				return decodeJSONLine(line), true, nil
			}
			return nil, false, nil
		}
		return nil, false, g.Error(err, "failed to read from queue")
	}

	// Remove trailing newline and decode
	return decodeJSONLine(line), true, nil
}

// decodeJSONLine processes a JSON encoded line from the queue
func decodeJSONLine(line string) any {
	// Remove trailing newline
	line = strings.TrimSuffix(line, "\n")

	// Decode the JSON
	var result any
	if err := json.Unmarshal([]byte(line), &result); err != nil {
		// If not valid JSON, return as string
		g.Debug("Failed to decode JSON: %v, raw: %s", err, line)
		return line
	}
	return result
}

// Reset positions the reader at the beginning of the file
func (q *Queue) Reset() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Ensure we're in reading mode
	if !q.reading {
		if err := q.startReading(); err != nil {
			return g.Error(err, "failed to start reading mode")
		}
	}

	// Seek to beginning of file
	_, err := q.File.Seek(0, io.SeekStart)
	if err != nil {
		return g.Error(err, "failed to reset queue position")
	}

	// Re-initialize the reader
	q.Reader = bufio.NewReader(q.File)
	return nil
}

// Close closes and optionally removes the queue file
func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Clean up resources
	if q.Writer != nil {
		q.Writer.Flush()
		q.Writer = nil
	}

	if q.Reader != nil {
		q.Reader = nil
	}

	if q.File != nil {
		path := q.File.Name()

		// Close the file
		if err := q.File.Close(); err != nil {
			return err
		}
		q.File = nil

		// Remove the file
		if !q.keep {
			if err := os.Remove(path); err != nil {
				return err
			}
		}

		q.reading = false
		q.writing = false

	}

	return nil
}

func CloseQueues() {
	for _, q := range queues.Items() {
		q.Close()
	}
}
