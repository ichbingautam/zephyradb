package storage

import (
    "github.com/codecrafters-io/redis-starter-go/internal/types"
)

// ZSet represents a Redis sorted set. For this stage we only need to
// support adding members with scores and reporting how many new members
// were added. We store scores in a simple map for O(1) lookups/updates.
// Ordering is not required yet by the tests.
type ZSet struct {
    // scores maps member -> score
    scores map[string]float64
}

// ensureZSetLocked ensures that the key exists and holds a ZSet under s.mu.
// Returns the entry and a flag indicating whether it existed before.
func (s *Store) ensureZSetLocked(key string) (Entry, bool) {
    entry, exists := s.data[key]
    if !exists || entry.Type != types.TypeZSet {
        // Initialize fresh ZSet
        entry = Entry{
            Type:  types.TypeZSet,
            Value: &ZSet{scores: make(map[string]float64)},
        }
        s.data[key] = entry
        return entry, false
    }
    return entry, true
}

// ZAdd adds the specified member with the given score to the sorted set stored at key.
// If key does not exist, a new sorted set is created. If the member already exists,
// its score is updated. The return value is the number of new members added to the
// sorted set (0 if updated existing member, 1 if added new one).
func (s *Store) ZAdd(key string, score float64, member string) int64 {
    s.mu.Lock()
    defer s.mu.Unlock()

    entry, _ := s.ensureZSetLocked(key)
    zs, _ := entry.Value.(*ZSet)

    _, existed := zs.scores[member]
    zs.scores[member] = score
    if existed {
        return 0
    }
    return 1
}

// ZRank returns the rank (0-based) of member in the sorted set stored at key.
// Ordering is by increasing score; ties are broken lexicographically by member.
// If the key or member doesn't exist, returns (0, false).
func (s *Store) ZRank(key, member string) (int64, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    entry, exists := s.data[key]
    if !exists || entry.Type != types.TypeZSet {
        return 0, false
    }
    zs, _ := entry.Value.(*ZSet)
    score, ok := zs.scores[member]
    if !ok {
        return 0, false
    }
    var rank int64
    for m, sc := range zs.scores {
        if sc < score {
            rank++
        } else if sc == score && m < member {
            rank++
        }
    }
    return rank, true
}
