package storage

import (
    "sort"
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

// ZScore returns the score of a member in the sorted set at key.
// Returns (0, false) if key/member are missing or wrong type.
func (s *Store) ZScore(key, member string) (float64, bool) {
    s.mu.RLock()
    defer s.mu.RUnlock()

    entry, exists := s.data[key]
    if !exists || entry.Type != types.TypeZSet {
        return 0, false
    }
    zs, _ := entry.Value.(*ZSet)
    sc, ok := zs.scores[member]
    if !ok {
        return 0, false
    }
    return sc, true
}

// ZRem removes a member from the sorted set at key. Returns 1 if the member
// existed and was removed, or 0 if the key/member was missing.
func (s *Store) ZRem(key, member string) int64 {
    s.mu.Lock()
    defer s.mu.Unlock()

    entry, exists := s.data[key]
    if !exists || entry.Type != types.TypeZSet {
        return 0
    }
    zs, _ := entry.Value.(*ZSet)
    if _, ok := zs.scores[member]; ok {
        delete(zs.scores, member)
        return 1
    }
    return 0
}

// ZCard returns the cardinality of the sorted set stored at key.
// Returns 0 if the key doesn't exist or doesn't hold a zset.
func (s *Store) ZCard(key string) int64 {
    s.mu.RLock()
    defer s.mu.RUnlock()

    entry, exists := s.data[key]
    if !exists || entry.Type != types.TypeZSet {
        return 0
    }
    zs, _ := entry.Value.(*ZSet)
    return int64(len(zs.scores))
}

// ZRange returns members in the sorted set stored at key between indexes
// start and stop (inclusive), ordered by increasing score and lexicographically
// for ties. If key doesn't exist or is not a zset, returns empty slice.
func (s *Store) ZRange(key string, start, stop int64) []string {
    s.mu.RLock()
    defer s.mu.RUnlock()

    entry, exists := s.data[key]
    if !exists || entry.Type != types.TypeZSet {
        return []string{}
    }
    zs, _ := entry.Value.(*ZSet)
    n := len(zs.scores)
    if n == 0 {
        return []string{}
    }
    // Materialize and sort
    items := make([]struct{ m string; s float64 }, 0, n)
    for m, sc := range zs.scores {
        items = append(items, struct{ m string; s float64 }{m: m, s: sc})
    }
    sort.Slice(items, func(i, j int) bool {
        if items[i].s == items[j].s {
            return items[i].m < items[j].m
        }
        return items[i].s < items[j].s
    })

    // Translate negatives relative to end and clamp
    iStart := start
    iStop := stop
    if iStart < 0 {
        iStart = int64(n) + iStart
    }
    if iStop < 0 {
        iStop = int64(n) + iStop
    }
    if iStart < 0 {
        iStart = 0
    }
    if iStop < 0 {
        iStop = 0
    }
    if iStart >= int64(n) {
        return []string{}
    }
    if iStop >= int64(n) {
        iStop = int64(n) - 1
    }
    if iStart > iStop {
        return []string{}
    }

    res := make([]string, 0, iStop-iStart+1)
    for i := iStart; i <= iStop; i++ {
        res = append(res, items[i].m)
    }
    return res
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
