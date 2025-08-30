package storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/types"
)

// XADD adds a new entry to a stream
func (s *Store) XADD(key string, id *StreamID, fields map[string]string) (StreamID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.data[key]

	// Check if this is a new or empty stream
	isNewOrEmpty := entry.Type == types.TypeNil
	if !isNewOrEmpty && entry.Type == types.TypeStream {
		if stream := entry.Value.(*Stream); stream.Len() == 0 {
			isNewOrEmpty = true
		}
	}

	if entry.Type == types.TypeNil {
		// Create new stream
		stream := NewStream()
		newID, err := stream.Add(fields, id)
		if err != nil {
			return StreamID{}, err
		}

		s.data[key] = Entry{
			Type:  types.TypeStream,
			Value: stream,
		}
		return newID, nil
	}

	if entry.Type != types.TypeStream {
		return StreamID{}, fmt.Errorf("key %s is not a stream", key)
	}

	// Get existing stream
	stream := entry.Value.(*Stream)
	return stream.Add(fields, id)
}

// XRANGE retrieves a range of entries from a stream
func (s *Store) XRANGE(key string, start, end string) ([]StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry := s.data[key]
	if entry.Type == types.TypeNil {
		return nil, nil // Empty stream
	}

	if entry.Type != types.TypeStream {
		return nil, fmt.Errorf("key %s is not a stream", key)
	}

	stream := entry.Value.(*Stream)

	var startID, endID StreamID
	var err error

	if start == "-" {
		// Special start marker: use lowest possible ID
		startID = StreamID{Time: 0, Sequence: 0}
	} else {
		startID, err = ParseStreamID(start)
		if err != nil {
			return nil, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
	}

	if end == "+" {
		// Special end marker: Redis uses max int64 for both timestamp and sequence
		endID = StreamID{Time: 1<<63 - 1, Sequence: 1<<63 - 1}
	} else if strings.Contains(end, "*") {
		// Reject any ID containing * in the end position
		return nil, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
	} else {
		endID, err = ParseStreamID(end)
		if err != nil {
			return nil, fmt.Errorf("ERR Invalid stream ID specified as stream command argument")
		}
	}

	return stream.Range(startID, endID), nil
}

// XREAD reads from one or more streams, optionally blocking until new data arrives
func (s *Store) XREAD(streams []string, ids []string, block bool, timeoutMs int64) (map[string][]StreamEntry, error) {
	if len(streams) != len(ids) {
		return nil, fmt.Errorf("number of streams and IDs must match")
	}

	result := make(map[string][]StreamEntry)

	// First try to read without locking
	hasData := false
	for i, stream := range streams {
		entries, err := s.readStream(stream, ids[i])
		if err != nil {
			return nil, err
		}
		if len(entries) > 0 {
			hasData = true
			result[stream] = entries
		}
	}

	if hasData || !block {
		return result, nil
	}

	// Block and wait for new data. For timeoutMs == 0, block indefinitely.
	var deadline time.Time
	if timeoutMs > 0 {
		deadline = time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	}
	for {
		// Small sleep to avoid busy waiting
		time.Sleep(10 * time.Millisecond)

		// Re-check for new data
		hasData = false
		for i, stream := range streams {
			entries, err := s.readStream(stream, ids[i])
			if err != nil {
				return nil, err
			}
			if len(entries) > 0 {
				hasData = true
				result[stream] = entries
			}
		}
		if hasData {
			return result, nil
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			// Timed out
			return result, nil
		}
	}
}

func (s *Store) readStream(key string, id string) ([]StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry := s.data[key]
	if entry.Type == types.TypeNil {
		return []StreamEntry{}, nil
	}

	if entry.Type != types.TypeStream {
		return nil, fmt.Errorf("key %s is not a stream", key)
	}

	stream := entry.Value.(*Stream)

	if id == "$" {
		// Return only new entries after current last ID
		return nil, nil
	}

	// Special case: if ID is "0-0", return all entries
	if id == "0-0" {
		// XREAD is exclusive, so start strictly after 0-0 => 0-1
		return stream.Range(StreamID{Time: 0, Sequence: 1}, StreamID{Time: 1<<63 - 1, Sequence: 1<<63 - 1}), nil
	}

	startID, err := ParseStreamID(id)
	if err != nil {
		// Be tolerant: treat invalid ID as no new entries instead of erroring out.
		// This prevents XREAD from failing in blocking scenarios.
		return []StreamEntry{}, nil
	}

	// Return entries with ID greater than startID (exclusive)
	return stream.Range(StreamID{Time: startID.Time, Sequence: startID.Sequence + 1}, StreamID{Time: 1<<63 - 1, Sequence: 1<<63 - 1}), nil
}

// XLEN returns the number of entries in a stream
func (s *Store) XLEN(key string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry := s.data[key]
	if entry.Type == types.TypeNil {
		return 0, nil
	}

	if entry.Type != types.TypeStream {
		return 0, fmt.Errorf("key %s is not a stream", key)
	}

	stream := entry.Value.(*Stream)
	return int64(stream.Len()), nil
}
