// Package storage implements Redis's data storage and manipulation functionality.
// It provides thread-safe operations for storing and retrieving different data types
// with support for expiry, atomic operations, and blocking commands.
package storage

import (
	"context"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/types"
)

// Entry represents a value stored in Redis with optional expiry.
// It can hold different types of values (strings, lists, streams) and tracks their expiry time.
type Entry struct {
	// Type indicates the kind of value stored (string, list, stream, etc.)
	Type types.DataType

	// Value holds the actual data for this entry.
	// The concrete type depends on the Type field:
	// - For TypeString: string
	// - For TypeList: []string
	// - For TypeStream: *Stream
	Value any

	// ExpiresAt is the Unix timestamp in milliseconds when this entry expires.
	// A value of 0 means the entry never expires.
	ExpiresAt int64
}

// Store implements a thread-safe key-value store that mimics Redis's data storage.
// It supports multiple data types, key expiry, and concurrent access.
type Store struct {
	// mu protects the data map from concurrent access
	mu sync.RWMutex

	// data stores all key-value pairs
	data map[string]Entry
}

// New creates and initializes a new Store instance.
// It initializes an empty thread-safe map for storing key-value pairs.
func New() *Store {
	return &Store{
		data: make(map[string]Entry),
	}
}

// KeysAll returns all non-expired keys currently in the store.
func (s *Store) KeysAll() []string {
	nowMs := time.Now().UnixMilli()
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.data))
	for k, e := range s.data {
		if e.ExpiresAt > 0 && nowMs > e.ExpiresAt {
			delete(s.data, k)
			continue
		}
		keys = append(keys, k)
	}
	return keys
}

// Set stores or updates a string value with optional expiry.
// Parameters:
//   - key: The key under which to store the value
//   - value: The string value to store
//   - expiryMs: Expiry time in milliseconds. 0 means no expiry.
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
	value, ok := entry.Value.(string)
	if !ok {
		return "", false
	}
	return value, true
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
	list, ok := entry.Value.([]string)
	if !ok {
		return nil, false
	}
	return list, true
}

// LLen returns the length of a list stored at the specified key.
// If the key does not exist or is not a list, returns 0.
// Parameters:
//   - key: The key identifying the list
//
// Returns:
//   - The number of elements in the list, or 0 if key doesn't exist
func (s *Store) LLen(key string) int64 {
	list, exists := s.GetList(key)
	if !exists {
		return 0
	}
	return int64(len(list))
}

// LPush inserts a new element at the beginning (left side) of a list.
// If the key does not exist, it is created as an empty list before the operation.
// If the key exists but is not a list, it returns an error.
// Parameters:
//   - key: The key identifying the list
//   - value: The value to insert at the beginning of the list
//
// Returns:
//   - The length of the list after the push operation
//
// Parameters:
//   - key: The key identifying the list
//   - values: One or more values to prepend to the list
//
// Returns:
//   - The length of the list after the push operation
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
			Type:  types.TypeList,
			Value: reversed,
		}
		return int64(len(values))
	}

	// Prepend to existing list
	existingList, _ := entry.Value.([]string)
	newList := make([]string, len(values)+len(existingList))
	copy(newList[len(values):], existingList)
	for i, v := range values {
		newList[len(values)-1-i] = v
	}
	entry.Value = newList
	s.data[key] = entry
	return int64(len(newList))
}

// LRange returns a subset of elements from a list stored at the specified key.
// Parameters:
//   - key: The key identifying the list
//   - start: Starting index (0-based). If negative, counts from end (-1 = last element)
//   - stop: Ending index (inclusive). If negative, counts from end (-1 = last element)
//
// Returns:
//   - A slice of values from the specified range. Returns empty slice if key doesn't exist
//   - Out of range indexes are handled by limiting to valid range
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

// LRem removes elements equal to the specified value from the list.
// Parameters:
//   - key: The key identifying the list
//   - count: The number of elements to remove and direction:
//   - count > 0: Remove up to count elements from head to tail
//   - count < 0: Remove up to count elements from tail to head
//   - count = 0: Remove all elements equal to value
//   - value: The value to match for removal
//
// Returns:
//   - The number of removed elements
//
// If key does not exist, it is treated as an empty list and the command returns zero.
// Parameters:
//   - key: The key identifying the list
//   - count: The number of elements to remove and direction
//   - value: The value to remove from the list
//
// Returns:
//   - The number of elements removed
func (s *Store) LRem(key string, count int64, value string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists || entry.Type != types.TypeList {
		return 0
	}

	list, _ := entry.Value.([]string)
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

	entry.Value = newList
	s.data[key] = entry
	return removed
}

// BLPop is a blocking list pop operation that waits for data in multiple lists.
// It removes and returns an element from the first non-empty list found.
// The operation blocks until one of the following conditions is met:
//   - An element becomes available in one of the lists
//   - The specified timeout is reached
//   - The context is cancelled
//
// Parameters:
//   - ctx: Context for cancellation
//   - timeout: Maximum time to block in seconds (0 means block indefinitely)
//   - keys: One or more list keys to monitor
//
// Returns:
//   - key: The key from which the element was popped
//   - value: The popped element
//   - ok: True if an element was popped, false if timeout/cancelled
func (s *Store) BLPop(ctx context.Context, timeout float64, keys ...string) (key string, value string, ok bool) {
	deadline := time.Now().Add(time.Duration(timeout * float64(time.Second)))

	for {
		s.mu.Lock()
		// Try each list
		for _, k := range keys {
			entry, exists := s.data[k]
			if exists && entry.Type == types.TypeList {
				list, ok := entry.Value.([]string)
				if !ok || len(list) == 0 {
					continue
				}
				// Get first element
				value := list[0]
				// Remove first element
				entry.Value = list[1:]
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

// RPush inserts a new element at the end (right side) of a list.
// If the key does not exist, it is created as an empty list before the operation.
// If the key exists but is not a list, it returns an error.
// Parameters:
//   - key: The key identifying the list
//   - value: The value to append to the end of the list
//
// Returns:
//   - The length of the list after the push operation
//
// Parameters:
//   - key: The key identifying the list
//   - values: One or more values to append to the list
//
// Returns:
//   - The length of the list after the push operation
func (s *Store) RPush(key string, values ...string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists || entry.Type != types.TypeList {
		// Create new list if doesn't exist or isn't a list
		s.data[key] = Entry{
			Type:  types.TypeList,
			Value: values,
		}
		return int64(len(values))
	}

	// Append to existing list
	list, _ := entry.Value.([]string)
	newList := append(list, values...)
	entry.Value = newList
	s.data[key] = entry
	return int64(len(newList))
}
