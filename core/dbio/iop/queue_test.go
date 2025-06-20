package iop

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueue_Creation(t *testing.T) {
	// Create a new queue
	q, err := NewQueue("test-queue")
	assert.NoError(t, err)
	defer q.Close()

	// Check initial state
	assert.True(t, q.writing)
	assert.False(t, q.reading)
	assert.NotNil(t, q.File)
	assert.NotNil(t, q.Writer)
	assert.Nil(t, q.Reader)
	assert.FileExists(t, q.Path)
}

func TestQueue_WriteAndRead(t *testing.T) {
	// Create a new queue
	q, err := NewQueue("test-queue")
	assert.NoError(t, err)
	defer q.Close()

	// Test writing string data
	err = q.Append("test string data")
	assert.NoError(t, err)

	// Test writing json-serializable data
	testData := map[string]interface{}{
		"name":  "test",
		"value": 123,
		"nested": map[string]interface{}{
			"key": "value",
		},
	}
	err = q.Append(testData)
	assert.NoError(t, err)

	// Should still be in writing mode
	assert.True(t, q.writing)
	assert.False(t, q.reading)

	// Test reading - should transition to reading mode
	item, hasMore, err := q.Next()
	assert.NoError(t, err)
	assert.True(t, hasMore)
	assert.Equal(t, "test string data", item)

	// Should now be in reading mode
	assert.False(t, q.writing)
	assert.True(t, q.reading)

	// Read second item
	item, hasMore, err = q.Next()
	assert.NoError(t, err)
	assert.True(t, hasMore)

	// Should be our map data
	mapItem, ok := item.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "test", mapItem["name"])
	assert.Equal(t, float64(123), mapItem["value"]) // JSON numbers are floats

	// Check nested map
	nestedMap, ok := mapItem["nested"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "value", nestedMap["key"])

	// Should be no more items
	_, hasMore, err = q.Next()
	assert.NoError(t, err)
	assert.False(t, hasMore)
}

func TestQueue_Reset(t *testing.T) {
	// Create a new queue
	q, err := NewQueue("test-queue")
	assert.NoError(t, err)
	defer q.Close()

	// Add data
	for i := 0; i < 3; i++ {
		err = q.Append(i)
		assert.NoError(t, err)
	}

	// Read first item
	item, hasMore, err := q.Next()
	assert.NoError(t, err)
	assert.True(t, hasMore)
	assert.Equal(t, float64(0), item)

	// Read second item
	item, hasMore, err = q.Next()
	assert.NoError(t, err)
	assert.True(t, hasMore)
	assert.Equal(t, float64(1), item)

	// Reset and read again
	err = q.Reset()
	assert.NoError(t, err)

	// Read first item again
	item, hasMore, err = q.Next()
	assert.NoError(t, err)
	assert.True(t, hasMore)
	assert.Equal(t, float64(0), item)
}

func TestQueue_WriteThenRead(t *testing.T) {
	// Create a new queue
	q, err := NewQueue("test-queue")
	assert.NoError(t, err)
	defer q.Close()

	// Add some data
	for i := 0; i < 5; i++ {
		err = q.Append(i)
		assert.NoError(t, err)
	}

	// Explicitly finish writing
	err = q.finishWriting()
	assert.NoError(t, err)
	assert.False(t, q.writing)

	// Start reading - should automatically transition to reading mode
	err = q.Reset()
	assert.NoError(t, err)
	assert.True(t, q.reading)
	assert.False(t, q.writing)

	// Read all items
	count := 0
	for {
		_, hasMore, err := q.Next()
		assert.NoError(t, err)
		if !hasMore {
			break
		}
		count++
	}
	assert.Equal(t, 5, count)
}

func TestQueue_ReadAfterWrite(t *testing.T) {
	// Create a new queue
	q, err := NewQueue("test-queue")
	assert.NoError(t, err)
	defer q.Close()

	// Add some data
	err = q.Append("data1")
	assert.NoError(t, err)
	err = q.Append("data2")
	assert.NoError(t, err)

	// Read first item - should transition to reading mode
	item, hasMore, err := q.Next()
	assert.NoError(t, err)
	assert.True(t, hasMore)
	assert.Equal(t, "data1", item)
	assert.True(t, q.reading)
	assert.False(t, q.writing)

	// Try to write after reading - should fail
	err = q.Append("data3")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue is in reading mode, cannot write")
}

func TestQueue_Close(t *testing.T) {
	// Create a new queue
	q, err := NewQueue("test-queue")
	assert.NoError(t, err)

	// Get the file path
	path := q.Path
	assert.FileExists(t, path)

	// Add some data and close
	err = q.Append("test")
	assert.NoError(t, err)
	err = q.Close()
	assert.NoError(t, err)

	// File should be removed
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))

	// Queue should be in clean state
	assert.False(t, q.reading)
	assert.False(t, q.writing)
	assert.Nil(t, q.File)
	assert.Nil(t, q.Reader)
	assert.Nil(t, q.Writer)
}

func TestQueue_StateTransitions(t *testing.T) {
	// Create a new queue
	q, err := NewQueue("test-queue")
	assert.NoError(t, err)
	defer q.Close()

	// Initial state
	assert.True(t, q.writing)
	assert.False(t, q.reading)

	// Finish writing
	err = q.finishWriting()
	assert.NoError(t, err)
	assert.False(t, q.writing)
	assert.False(t, q.reading)

	// Try to start writing again
	err = q.startWriting()
	assert.NoError(t, err) // Should be allowed as not in reading mode yet
	assert.True(t, q.writing)
	assert.False(t, q.reading)

	// Start reading - should transition from writing to reading
	err = q.startReading()
	assert.NoError(t, err)
	assert.False(t, q.writing)
	assert.True(t, q.reading)

	// Try to start writing again - should fail
	err = q.startWriting()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "queue is in reading mode, cannot switch to writing mode")
	assert.False(t, q.writing)
	assert.True(t, q.reading)
}

func TestQueue_LargeDataVolume(t *testing.T) {
	// Create a new queue
	q, err := NewQueue("test-queue-large")
	assert.NoError(t, err)
	defer q.Close()

	// Generate a larger dataset
	const itemCount = 1000
	for i := 0; i < itemCount; i++ {
		data := map[string]interface{}{
			"id":    i,
			"value": "This is test value " + string(rune(i%26+65)), // A-Z repeating
		}
		err = q.Append(data)
		assert.NoError(t, err)
	}

	// Transition to reading mode
	err = q.Reset()
	assert.NoError(t, err)

	// Read and verify all items
	readCount := 0
	for {
		item, hasMore, err := q.Next()
		assert.NoError(t, err)
		if !hasMore {
			break
		}

		mapItem, ok := item.(map[string]interface{})
		assert.True(t, ok)

		id := int(mapItem["id"].(float64))
		assert.Equal(t, readCount, id)

		expectedValue := "This is test value " + string(rune(id%26+65))
		assert.Equal(t, expectedValue, mapItem["value"])

		readCount++
	}

	assert.Equal(t, itemCount, readCount)
}

func TestQueue_NonJsonData(t *testing.T) {
	// Create a new queue
	q, err := NewQueue("test-queue-nonjson")
	assert.NoError(t, err)
	defer q.Close()

	// Add some strings with special characters
	testStrings := []string{
		"Just a simple string",
		"A string with special chars: @#$%^&*",
		"String with newlines\nand\ttabs",
		`String with "quotes" inside`,
		"123456", // Looks like a number but stored as string
	}

	for _, s := range testStrings {
		err = q.Append(s)
		assert.NoError(t, err)
	}

	// Read back and verify
	err = q.Reset()
	assert.NoError(t, err)

	for i, expected := range testStrings {
		item, hasMore, err := q.Next()
		assert.NoError(t, err)
		assert.True(t, hasMore)
		// Since we're JSON encoding, the item will be properly decoded back to a string
		assert.Equal(t, expected, item, "Item %d doesn't match expected value", i)
	}

	// No more items
	_, hasMore, err := q.Next()
	assert.NoError(t, err)
	assert.False(t, hasMore)
}

// Add a specific test for newlines and special characters
func TestQueue_SpecialCharacters(t *testing.T) {
	// Create a new queue
	q, err := NewQueue("test-queue-special")
	assert.NoError(t, err)
	defer q.Close()

	// Test data with various special characters
	testData := []string{
		"Line with \n newline",
		"Line with \t tab",
		"Line with \r carriage return",
		"Line with \\ backslash",
		"Line with \"quotes\"",
		"Line with multiple\nnewlines\nand\ttabs",
		"Line with unicode: 你好, 🚀, ñ",
	}

	// Write all data
	for _, data := range testData {
		err = q.Append(data)
		assert.NoError(t, err)
	}

	// Read and verify
	err = q.Reset()
	assert.NoError(t, err)

	for i, expected := range testData {
		item, hasMore, err := q.Next()
		assert.NoError(t, err)
		assert.True(t, hasMore)
		assert.Equal(t, expected, item, "Special character item %d doesn't match", i)
	}
}

func TestQueue_MultipleResets(t *testing.T) {
	// Create a new queue
	q, err := NewQueue("test-queue-resets")
	assert.NoError(t, err)
	defer q.Close()

	// Add some data
	testData := []int{1, 2, 3, 4, 5}
	for _, val := range testData {
		err = q.Append(val)
		assert.NoError(t, err)
	}

	// First read cycle
	err = q.Reset()
	assert.NoError(t, err)

	for i := 0; i < 3; i++ { // Read only first 3 items
		item, hasMore, err := q.Next()
		assert.NoError(t, err)
		assert.True(t, hasMore)
		assert.Equal(t, float64(testData[i]), item)
	}

	// Reset again and read all items
	err = q.Reset()
	assert.NoError(t, err)

	for i, expected := range testData {
		item, hasMore, err := q.Next()
		assert.NoError(t, err)
		assert.True(t, hasMore)
		assert.Equal(t, float64(expected), item, "Item %d doesn't match", i)
	}

	// One more reset and partial read
	err = q.Reset()
	assert.NoError(t, err)

	item, hasMore, err := q.Next()
	assert.NoError(t, err)
	assert.True(t, hasMore)
	assert.Equal(t, float64(1), item)
}

func TestQueue_EmptyQueue(t *testing.T) {
	// Create a new queue without writing anything
	q, err := NewQueue("test-queue-empty")
	assert.NoError(t, err)
	defer q.Close()

	// Try to read from empty queue
	err = q.Reset()
	assert.NoError(t, err)

	_, hasMore, err := q.Next()
	assert.NoError(t, err)
	assert.False(t, hasMore, "Empty queue should report no more items")

	// Now add something and verify it works
	err = q.startWriting() // Need to switch back to writing mode
	assert.Error(t, err, "Should not be able to switch to writing after reading")
}

func TestQueue_CloseAndReopenScenario(t *testing.T) {
	// This test simulates an application closing and reopening a queue
	// by creating a new queue with the same file

	var path string

	// First session: create and write
	{
		q, err := NewQueue("test-queue-session")
		assert.NoError(t, err)
		path = q.Path // Save path for later

		err = q.Append("session 1 data")
		assert.NoError(t, err)

		// Just close the file without deleting (simulate application exit)
		if q.Writer != nil {
			q.Writer.Flush()
		}
		if q.File != nil {
			q.File.Close()
			q.File = nil
		}
	}

	// In a real scenario, we'd use a persistent path rather than the temp file
	// which would be deleted. This test just demonstrates the concept.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Skip("Temp file was removed, skipping multi-session test")
	}

	// Second session: reopen and read
	{
		// Create a queue with the existing file
		q := &Queue{
			Path: path,
		}

		// Start in reading mode
		err := q.startReading()
		assert.NoError(t, err)
		defer q.Close() // Now actually delete the file

		// Read the data from first session
		item, hasMore, err := q.Next()
		assert.NoError(t, err)
		assert.True(t, hasMore)
		assert.Equal(t, "session 1 data", item)
	}
}

func TestChunkFunction(t *testing.T) {
	// Create function instance
	fns := functions{}

	// Test array chunking
	t.Run("Array Chunking", func(t *testing.T) {
		// Create a test array
		testArray := []any{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

		// Chunk with size 3
		result, err := fns.chunk(testArray, 3)
		assert.NoError(t, err)

		// Verify result is a channel
		chunkedChan, ok := result.(chan []any)
		assert.True(t, ok, "Result should be a channel")

		// Add timeout to prevent test hanging
		chunks := [][]any{}
		timeout := time.After(3 * time.Second)

		done := make(chan bool)
		go func() {
			for chunk := range chunkedChan {
				chunks = append(chunks, chunk)
			}
			done <- true
		}()

		// Wait for either completion or timeout
		select {
		case <-done:
			// Success - continue with assertions
		case <-timeout:
			t.Fatal("Test timed out waiting for chunks")
		}

		// Verify chunks
		assert.Equal(t, 4, len(chunks), "Should have 4 chunks")
		assert.Equal(t, []any{1, 2, 3}, chunks[0])
		assert.Equal(t, []any{4, 5, 6}, chunks[1])
		assert.Equal(t, []any{7, 8, 9}, chunks[2])
		assert.Equal(t, []any{10}, chunks[3], "Last chunk should have remaining elements")
	})

	t.Run("Queue Chunking", func(t *testing.T) {
		// Create a test queue
		q, err := NewQueue("test-chunk-queue")
		assert.NoError(t, err)
		defer q.Close()

		// Add test data
		for i := 1; i <= 10; i++ {
			err = q.Append(i)
			assert.NoError(t, err)
		}

		// Chunk with size 4
		result, err := fns.chunk(q, 4)
		assert.NoError(t, err)

		// Verify result is a channel
		chunkedChan, ok := result.(chan []any)
		assert.True(t, ok, "Result should be a channel")

		// Read all chunks
		chunks := [][]any{}
		for chunk := range chunkedChan {
			chunks = append(chunks, chunk)
		}

		// Verify chunks
		assert.Equal(t, 3, len(chunks), "Should have 3 chunks")
		assert.Equal(t, 4, len(chunks[0]), "First chunk should have 4 elements")
		assert.Equal(t, 4, len(chunks[1]), "Second chunk should have 4 elements")
		assert.Equal(t, 2, len(chunks[2]), "Third chunk should have 2 elements")

		// Check values (accounting for JSON numbers as float64)
		assert.Equal(t, float64(1), chunks[0][0])
		assert.Equal(t, float64(5), chunks[1][0])
		assert.Equal(t, float64(9), chunks[2][0])
	})

	t.Run("Invalid Arguments", func(t *testing.T) {
		// Test with wrong number of arguments
		_, err := fns.chunk([]any{1, 2, 3})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "expects at 2 arguments")

		// Test with invalid chunk size
		_, err = fns.chunk([]any{1, 2, 3}, "not-a-number")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid chunk_size")

		// Test with negative chunk size
		_, err = fns.chunk([]any{1, 2, 3}, -1)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "must be greater than 0")

		// Test with invalid object type
		_, err = fns.chunk("not-an-array-or-queue", 2)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid object for chunking")
	})

	t.Run("Empty Array", func(t *testing.T) {
		// Test with empty array
		result, err := fns.chunk([]any{}, 3)
		assert.NoError(t, err)

		chunkedChan, ok := result.(chan []any)
		assert.True(t, ok, "Result should be a channel")

		// Channel should close without sending any chunks
		chunks := [][]any{}
		for chunk := range chunkedChan {
			chunks = append(chunks, chunk)
		}

		assert.Equal(t, 0, len(chunks), "Should have no chunks for empty array")
	})

	t.Run("Empty Queue", func(t *testing.T) {
		// Create an empty queue
		q, err := NewQueue("test-empty-queue")
		assert.NoError(t, err)
		defer q.Close()

		// Chunk the empty queue
		result, err := fns.chunk(q, 3)
		assert.NoError(t, err)

		chunkedChan, ok := result.(chan []any)
		assert.True(t, ok, "Result should be a channel")

		// Channel should close without sending any chunks
		chunks := [][]any{}
		for chunk := range chunkedChan {
			chunks = append(chunks, chunk)
		}

		assert.Equal(t, 0, len(chunks), "Should have no chunks for empty queue")
	})
}
