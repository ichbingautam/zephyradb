package storage

import (
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/types"
)

// Entry represents a value in the store with optional expiry
type Entry struct {
	Type      types.DataType
	Value     string   // for string values
	List      []string // for list values
	ExpiresAt int64    // unix ms, 0 means no expiry
}

// Store represents a thread-safe key-value store with expiry support
type Store struct {
	mu   sync.RWMutex
	data map[string]Entry
}

// New creates a new Store instance
func New() *Store {
	return &Store{
		data: make(map[string]Entry),
	}
}

// Set stores a string value with optional expiry
func (s *Store) Set(key, value string, expiryMs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt int64
	if expiryMs > 0 {
		expiresAt = time.Now().UnixMilli() + expiryMs
	}

	s.data[key] = Entry{
		Type:      types.TypeString,
		Value:     value,
		ExpiresAt: expiresAt,
	}
}

// Get retrieves a value by key
func (s *Store) Get(key string) (*Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]
	if !exists {
		return nil, false
	}

	// Check expiry
	if entry.ExpiresAt > 0 && time.Now().UnixMilli() > entry.ExpiresAt {
		// Cleanup expired key
		s.mu.RUnlock()
		s.mu.Lock()
		delete(s.data, key)
		s.mu.Unlock()
		s.mu.RLock()
		return nil, false
	}

	return &entry, true
}

// GetString retrieves a string value by key
func (s *Store) GetString(key string) (string, bool) {
	entry, exists := s.Get(key)
	if !exists {
		return "", false
	}
	if entry.Type != types.TypeString {
		return "", false
	}
	return entry.Value, true
}

// RPush appends values to a list and returns the new length
func (s *Store) RPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists || entry.Type != types.TypeList {
		// Create new list if doesn't exist or isn't a list
		s.data[key] = Entry{
			Type: types.TypeList,
			List: values,
		}
		return len(values)
	}

	// Append to existing list
	entry.List = append(entry.List, values...)
	s.data[key] = entry
	return len(entry.List)
}
