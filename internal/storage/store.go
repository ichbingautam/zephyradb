package storage

import (
	"sync"
	"time"
)

// Entry represents a value in the store with optional expiry
type Entry struct {
	Value     string
	ExpiresAt int64 // unix ms, 0 means no expiry
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

// Set stores a value with optional expiry
func (s *Store) Set(key, value string, expiryMs int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt int64
	if expiryMs > 0 {
		expiresAt = time.Now().UnixMilli() + expiryMs
	}

	s.data[key] = Entry{
		Value:     value,
		ExpiresAt: expiresAt,
	}
}

// Get retrieves a value, considering expiry
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, exists := s.data[key]
	if !exists {
		return "", false
	}

	// Check expiry
	if entry.ExpiresAt > 0 && time.Now().UnixMilli() > entry.ExpiresAt {
		return "", false
	}

	return entry.Value, true
}
