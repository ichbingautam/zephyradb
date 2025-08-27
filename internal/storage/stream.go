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
	// Handle full ID format (timestamp-sequence)
	if strings.Contains(id, "-") {
		parts := strings.Split(id, "-")
		if len(parts) != 2 {
			return StreamID{}, fmt.Errorf("Invalid stream ID format")
		}

		// Parse timestamp part
		ts, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return StreamID{}, fmt.Errorf("Invalid stream ID format")
		}

		// Handle auto-sequence marker
		if parts[1] == "*" {
			return StreamID{Time: ts, Sequence: -1}, nil // -1 indicates auto-sequence
		}

		// Parse explicit sequence number
		seq, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return StreamID{}, fmt.Errorf("Invalid stream ID format")
		}

		return StreamID{Time: ts, Sequence: seq}, nil
	}

	// Handle timestamp-only format (defaults to sequence 0)
	ts, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return StreamID{}, fmt.Errorf("Invalid stream ID format")
	}

	return StreamID{Time: ts, Sequence: 0}, nil
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
		// Handle auto-sequence case (sequence = -1)
		if requestedID.Sequence == -1 {
			// For time 0, start sequence at 1
			if requestedID.Time == 0 {
				newID = StreamID{Time: 0, Sequence: 1}
			} else if requestedID.Time == s.lastID.Time {
				// Same timestamp as last entry, increment sequence
				newID = StreamID{Time: requestedID.Time, Sequence: s.lastID.Sequence + 1}
			} else if requestedID.Time > s.lastID.Time {
				// New timestamp, start sequence at 0
				newID = StreamID{Time: requestedID.Time, Sequence: 0}
			} else {
				// Time is less than last entry
				return StreamID{}, fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
			}
		} else {
			// Handle explicit sequence number
			if requestedID.Time <= 0 && requestedID.Sequence <= 0 {
				return StreamID{}, fmt.Errorf("The ID specified in XADD must be greater than 0-0")
			}
			if requestedID.Time < s.lastID.Time ||
				(requestedID.Time == s.lastID.Time && requestedID.Sequence <= s.lastID.Sequence) {
				return StreamID{}, fmt.Errorf("The ID specified in XADD is equal or smaller than the target stream top item")
			}
			newID = *requestedID
		}
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
		// Entry is in range if:
		// 1. Its timestamp is greater than start timestamp, OR
		// 2. Same timestamp as start AND sequence >= start sequence
		// AND
		// 1. Its timestamp is less than end timestamp, OR
		// 2. Same timestamp as end AND sequence <= end sequence
		if (entry.ID.Time > start.Time ||
			(entry.ID.Time == start.Time && entry.ID.Sequence >= start.Sequence)) &&
			(entry.ID.Time < end.Time ||
				(entry.ID.Time == end.Time && entry.ID.Sequence <= end.Sequence)) {
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
