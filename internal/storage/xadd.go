package storage

import (
	"fmt"

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

	// For a new key or empty stream, validate ID must be greater than 0-0
	if id != nil && isNewOrEmpty {
		if id.Time <= 0 && id.Sequence <= 0 {
			return StreamID{}, fmt.Errorf("The ID specified in XADD must be greater than 0-0")
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

	// Add to existing stream
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
		startID = StreamID{Time: -1, Sequence: 0}
	} else {
		startID, err = ParseStreamID(start)
		if err != nil {
			return nil, fmt.Errorf("invalid start ID: %v", err)
		}
	}

	if end == "+" {
		endID = StreamID{Time: -1, Sequence: 0}
	} else {
		endID, err = ParseStreamID(end)
		if err != nil {
			return nil, fmt.Errorf("invalid end ID: %v", err)
		}
	}

	return stream.Range(startID, endID), nil
}

// XREAD reads from one or more streams, optionally blocking until new data arrives
func (s *Store) XREAD(streams []string, ids []string, block bool) (map[string][]StreamEntry, error) {
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

	// Block and wait for new data
	// In a real implementation, this would use channels and conditions
	// Here we just return empty for simplicity
	return result, nil
}

func (s *Store) readStream(key string, id string) ([]StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry := s.data[key]
	if entry.Type == types.TypeNil {
		return nil, nil
	}

	if entry.Type != types.TypeStream {
		return nil, fmt.Errorf("key %s is not a stream", key)
	}

	stream := entry.Value.(*Stream)

	if id == "$" {
		// Return only new entries after current last ID
		return nil, nil
	}

	startID, err := ParseStreamID(id)
	if err != nil {
		return nil, fmt.Errorf("invalid ID: %v", err)
	}

	// Return entries with ID greater than startID
	return stream.Range(startID, StreamID{Time: -1, Sequence: 0}), nil
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
