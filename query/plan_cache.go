package query

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// defaultCacheSize is the default maximum number of cached query plans.
const defaultCacheSize = 1000

// planCacheEntry holds a cached query plan with its expiry.
type planCacheEntry struct {
	optimizedRelations []RelationPattern
	estimatedCount     int
	expiresAt          time.Time
	lastAccess         time.Time
}

// QueryPlanCache caches optimized query plans (CBO-optimized join orders,
// filter strategies) keyed by query signature. Plans expire after a TTL
// and are evicted by LRU when the cache is full.
type QueryPlanCache struct {
	mu       sync.RWMutex
	entries  map[uint64]*planCacheEntry
	maxSize  int
	ttl      time.Duration
	hitCount int64
	missCount int64
	// lru list for O(1) eviction ordering (not just LFU).
	lruList []uint64
}

// NewQueryPlanCache creates a new query plan cache with the given max size
// and TTL. Default: 1000 entries, 5 minute TTL.
func NewQueryPlanCache(maxSize int, ttl time.Duration) *QueryPlanCache {
	if maxSize <= 0 {
		maxSize = defaultCacheSize
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &QueryPlanCache{
		entries: make(map[uint64]*planCacheEntry),
		maxSize: maxSize,
		ttl:     ttl,
	}
}

// DefaultQueryPlanCache creates a query plan cache with default settings.
func DefaultQueryPlanCache() *QueryPlanCache {
	return NewQueryPlanCache(1000, 5*time.Minute)
}

// planKey computes a 64-bit hash key from a set of relation patterns.
// The key is deterministic: same relation patterns produce the same key.
func planKey(relations []RelationPattern, resultVars []string) uint64 {
	// Build a canonical representation: sort relations by their prefix + bound positions
	type canonRel struct {
		prefix            byte
		boundPositions    map[int]uint64
		variablePositions map[int]string
	}
	canon := make([]canonRel, len(relations))
	for i, rel := range relations {
		canon[i] = canonRel{
			prefix:            rel.Prefix,
			boundPositions:    rel.BoundPositions,
			variablePositions: rel.VariablePositions,
		}
	}

	// Canonicalize: sort relations by their tuple representation
	sort.Slice(canon, func(i, j int) bool {
		if canon[i].prefix != canon[j].prefix {
			return canon[i].prefix < canon[j].prefix
		}
		// Compare by total bound + variable positions
		iTotal := len(canon[i].boundPositions) + len(canon[i].variablePositions)
		jTotal := len(canon[j].boundPositions) + len(canon[j].variablePositions)
		return iTotal < jTotal
	})

	// Hash the canonical representation
	h := fnv.New64a()
	for _, rel := range canon {
		h.Write([]byte{rel.prefix})
		// Encode bound positions deterministically
		posKeys := make([]int, 0, len(rel.boundPositions))
		for p := range rel.boundPositions {
			posKeys = append(posKeys, p)
		}
		sort.Ints(posKeys)
		for _, p := range posKeys {
			binary.Write(h, binary.BigEndian, int64(p))
			binary.Write(h, binary.BigEndian, rel.boundPositions[p])
		}
		// Encode variable positions deterministically
		varKeys := make([]int, 0, len(rel.variablePositions))
		for p := range rel.variablePositions {
			varKeys = append(varKeys, p)
		}
		sort.Ints(varKeys)
		for _, p := range varKeys {
			binary.Write(h, binary.BigEndian, int64(p))
			h.Write([]byte(rel.variablePositions[p]))
			h.Write([]byte{0})
		}
	}
	// Include resultVars in the key so plans with different projections
	// don't share a cached optimization intended for a different output set.
	for _, v := range resultVars {
		h.Write([]byte(v))
		h.Write([]byte{0})
	}
	return h.Sum64()
}

// Get retrieves a cached plan. Returns nil if not found or expired.
func (c *QueryPlanCache) Get(key uint64) ([]RelationPattern, int, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		c.mu.Lock()
		c.missCount++
		c.mu.Unlock()
		return nil, 0, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check expiry
	if time.Now().After(entry.expiresAt) {
		delete(c.entries, key)
		c.removeFromLRU(key)
		c.missCount++
		return nil, 0, false
	}

	entry.lastAccess = time.Now()
	c.promoteInLRU(key)
	c.hitCount++

	slog.Debug("plan cache hit",
		"key", key,
		"hitCount", c.hitCount,
	)

	return entry.optimizedRelations, entry.estimatedCount, true
}

// promoteInLRU moves key to the front (most recently used).
func (c *QueryPlanCache) promoteInLRU(key uint64) {
	for i, k := range c.lruList {
		if k == key {
			c.lruList = append(c.lruList[:i], c.lruList[i+1:]...)
			break
		}
	}
	c.lruList = append(c.lruList, key)
}

// removeFromLRU removes key from the LRU list.
func (c *QueryPlanCache) removeFromLRU(key uint64) {
	for i, k := range c.lruList {
		if k == key {
			c.lruList = append(c.lruList[:i], c.lruList[i+1:]...)
			return
		}
	}
}

// Set stores an optimized plan in the cache. Evicts the least-recently-used
// entry (LRU) if the cache is full.
func (c *QueryPlanCache) Set(key uint64, relations []RelationPattern, estimatedCount int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict if full — LRU: the first entry in lruList is the oldest.
	for len(c.entries) >= c.maxSize && len(c.lruList) > 0 {
		oldest := c.lruList[0]
		c.lruList = c.lruList[1:]
		delete(c.entries, oldest)
		slog.Debug("plan cache evicted LRU",
			"key", oldest,
			"cacheSize", len(c.entries),
		)
	}

	c.entries[key] = &planCacheEntry{
		optimizedRelations: relations,
		estimatedCount:     estimatedCount,
		expiresAt:          time.Now().Add(c.ttl),
		lastAccess:         time.Now(),
	}
	c.lruList = append(c.lruList, key)
}

// Invalidate removes a specific plan from the cache.
func (c *QueryPlanCache) Invalidate(key uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
	c.removeFromLRU(key)
}

// Clear removes all cached plans.
func (c *QueryPlanCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[uint64]*planCacheEntry)
	c.lruList = c.lruList[:0]
	c.hitCount = 0
	c.missCount = 0
}

// Stats returns cache performance statistics.
func (c *QueryPlanCache) Stats() (hits, misses int64, size, maxSize int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hitCount, c.missCount, len(c.entries), c.maxSize
}

// OptimizeRelationsWithCache uses the plan cache to avoid re-optimizing
// the same relation patterns. If a cached plan exists, it is returned
// immediately. Otherwise, the engine optimizes the relations, caches them,
// and returns the optimized result.
func (e *LFTJEngine) OptimizeRelationsWithCache(ctx context.Context, relations []RelationPattern, boundVars map[string]uint64) ([]RelationPattern, int) {
	// Lazy-init plan cache per engine instance.
	if e.planCache == nil {
		e.planCache = DefaultQueryPlanCache()
	}

	key := planKey(relations, nil)

	// Check cache first
	if cached, estimatedCount, ok := e.planCache.Get(key); ok {
		return cached, estimatedCount
	}

	// Cache miss: optimize and store
	optimized, estimatedCount := e.OptimizeRelations(ctx, relations, boundVars)
	if estimatedCount > 0 {
		e.planCache.Set(key, optimized, estimatedCount)
	}

	return optimized, estimatedCount
}
