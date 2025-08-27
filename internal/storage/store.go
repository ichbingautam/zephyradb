package storage

import (
	"context"
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

// GetList returns a list by key
func (s *Store) GetList(key string) ([]string, bool) {
	entry, exists := s.Get(key)
	if !exists {
		return nil, false
	}
	if entry.Type != types.TypeList {
		return nil, false
	}
	return entry.List, true
}

// LLen returns the length of a list
func (s *Store) LLen(key string) int64 {
	list, exists := s.GetList(key)
	if !exists {
		return 0
	}
	return int64(len(list))
}

// LPush prepends values to a list and returns the new length
func (s *Store) LPush(key string, values ...string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists || entry.Type != types.TypeList {
		// Create new list
		reversed := make([]string, len(values))
		for i, v := range values {
			reversed[len(values)-1-i] = v
		}
		s.data[key] = Entry{
			Type: types.TypeList,
			List: reversed,
		}
		return int64(len(values))
	}

	// Prepend to existing list
	newList := make([]string, len(values)+len(entry.List))
	copy(newList[len(values):], entry.List)
	for i, v := range values {
		newList[len(values)-1-i] = v
	}
	entry.List = newList
	s.data[key] = entry
	return int64(len(newList))
}

// LRange returns a range of elements from a list
func (s *Store) LRange(key string, start, stop int64) []string {
	list, exists := s.GetList(key)
	if !exists {
		return []string{}
	}

	length := int64(len(list))

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Boundary checks
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return []string{}
	}

	return list[start : stop+1]
}

// LRem removes elements equal to value from the list
func (s *Store) LRem(key string, count int64, value string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists || entry.Type != types.TypeList {
		return 0
	}

	list := entry.List
	var removed int64
	var newList []string

	if count > 0 {
		// Remove count elements from head to tail
		for _, v := range list {
			if v == value && removed < count {
				removed++
				continue
			}
			newList = append(newList, v)
		}
	} else if count < 0 {
		// Remove |count| elements from tail to head
		count = -count
		for i := len(list) - 1; i >= 0; i-- {
			if list[i] == value && removed < count {
				removed++
				continue
			}
			newList = append([]string{list[i]}, newList...)
		}
	} else {
		// Remove all elements equal to value
		for _, v := range list {
			if v == value {
				removed++
				continue
			}
			newList = append(newList, v)
		}
	}

	entry.List = newList
	s.data[key] = entry
	return removed
}

// BLPop blocks until an element is available in one of the lists
func (s *Store) BLPop(ctx context.Context, timeout float64, keys ...string) (key string, value string, ok bool) {
	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))

	for {
		s.mu.Lock()
		// Try each list
		for _, k := range keys {
			entry, exists := s.data[k]
			if exists && entry.Type == types.TypeList && len(entry.List) > 0 {
				// Get first element
				value := entry.List[0]
				// Remove first element
				entry.List = entry.List[1:]
				s.data[k] = entry
				s.mu.Unlock()
				return k, value, true
			}
		}
		s.mu.Unlock()

		// Check timeout
		if timeout == 0 {
			select {
			case <-ctx.Done():
				return "", "", false
			case <-time.After(100 * time.Millisecond):
				continue
			}
		} else if time.Now().After(deadline) {
			return "", "", false
		}

		// Wait before next try
		select {
		case <-ctx.Done():
			return "", "", false
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// RPush appends values to a list and returns the new length
func (s *Store) RPush(key string, values ...string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists || entry.Type != types.TypeList {
		// Create new list if doesn't exist or isn't a list
		s.data[key] = Entry{
			Type: types.TypeList,
			List: values,
		}
		return int64(len(values))
	}

	// Append to existing list
	entry.List = append(entry.List, values...)
	s.data[key] = entry
	return int64(len(entry.List))
}
