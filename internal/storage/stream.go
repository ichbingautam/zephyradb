// Package storage provides the storage functionality for Redis data types.
package storage

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// StreamID represents a unique identifier for a stream entry.
// Format: <millisecondsTime>-<sequenceNumber>
type StreamID struct {
	Time     int64 // Unix timestamp in milliseconds
	Sequence int64 // Sequence number for entries in the same millisecond
}

// String formats the StreamID as a Redis stream ID string
func (id StreamID) String() string {
	return fmt.Sprintf("%d-%d", id.Time, id.Sequence)
}

// ParseStreamID parses a Redis stream ID string into a StreamID struct
func ParseStreamID(id string) (StreamID, error) {
	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return StreamID{}, fmt.Errorf("invalid stream ID format: %s", id)
	}

	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return StreamID{}, fmt.Errorf("invalid timestamp in stream ID: %s", id)
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return StreamID{}, fmt.Errorf("invalid sequence in stream ID: %s", id)
	}

	return StreamID{Time: ts, Sequence: seq}, nil
}

// IsSpecial checks if the ID is a special value like "$" or "-"
func (id StreamID) IsSpecial() bool {
	return id.Time == -1 && id.Sequence == -1 // Special ID for "$"
}

// StreamEntry represents a single entry in a Redis stream
type StreamEntry struct {
	ID     StreamID          // Unique identifier
	Fields map[string]string // Field-value pairs
}

// Stream represents a Redis stream data structure
type Stream struct {
	mu      sync.RWMutex
	entries []StreamEntry
	lastID  StreamID
}

// NewStream creates a new empty stream
func NewStream() *Stream {
	return &Stream{
		entries: make([]StreamEntry, 0),
		lastID:  StreamID{Time: 0, Sequence: 0},
	}
}

// Add adds a new entry to the stream
func (s *Stream) Add(fields map[string]string, requestedID *StreamID) (StreamID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var newID StreamID

	if requestedID == nil {
		// Auto-generate ID
		now := time.Now().UnixMilli()
		if now <= s.lastID.Time {
			// Same millisecond, increment sequence
			newID = StreamID{Time: s.lastID.Time, Sequence: s.lastID.Sequence + 1}
		} else {
			// New millisecond
			newID = StreamID{Time: now, Sequence: 0}
		}
	} else {
		// Validate requested ID
		if len(s.entries) == 0 {
			// For new streams, ID must be greater than 0-0
			if requestedID.Time <= 0 && requestedID.Sequence <= 0 {
				return StreamID{}, fmt.Errorf("The ID specified in XADD must be greater than 0-0")
			}
		} else {
			// For existing streams, ID must be greater than last ID
			if requestedID.Time < s.lastID.Time ||
				(requestedID.Time == s.lastID.Time && requestedID.Sequence <= s.lastID.Sequence) {
				return StreamID{}, fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
			}
		}
		newID = *requestedID
	}

	entry := StreamEntry{
		ID:     newID,
		Fields: fields,
	}

	s.entries = append(s.entries, entry)
	s.lastID = newID

	return newID, nil
}

// Range returns entries within the specified ID range
func (s *Stream) Range(start, end StreamID) []StreamEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []StreamEntry
	for _, entry := range s.entries {
		if (start.Time == -1 || entry.ID.Time >= start.Time) && // handle "-" special case
			(end.Time == -1 || entry.ID.Time <= end.Time) { // handle "+" special case
			result = append(result, entry)
		}
	}

	return result
}

// LastID returns the ID of the last entry in the stream
func (s *Stream) LastID() StreamID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastID
}

// Len returns the number of entries in the stream
func (s *Stream) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}
