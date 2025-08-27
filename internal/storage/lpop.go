package storage

import (
	"github.com/codecrafters-io/redis-starter-go/internal/types"
)

// LPop removes and returns count elements from the start of a list
func (s *Store) LPop(key string, count int64) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.data[key]
	if !exists || entry.Type != types.TypeList {
		return nil, false
	}

	listLen := len(entry.List)
	if listLen == 0 {
		return nil, false
	}

	// If count is 0 or negative, return empty array
	if count <= 0 {
		return []string{}, true
	}

	// If count is greater than list length, return all elements
	if count > int64(listLen) {
		count = int64(listLen)
	}

	// Get the first count elements and update the list
	values := make([]string, count)
	copy(values, entry.List[:count])

	if count == int64(listLen) {
		delete(s.data, key)
	} else {
		entry.List = entry.List[count:]
		s.data[key] = entry
	}

	return values, true
}
