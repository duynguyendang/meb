package query

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"time"
)

// QueryCache provides TTL-based caching for query results.
type QueryCache struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	maxSize int
	ttl     time.Duration
	enabled bool
}

type cacheEntry struct {
	results   []map[string]any
	expiresAt time.Time
	createdAt time.Time
}

// NewQueryCache creates a new query cache instance.
func NewQueryCache(ttl time.Duration, maxSize int, enabled bool) *QueryCache {
	return &QueryCache{
		entries: make(map[string]*cacheEntry),
		maxSize: maxSize,
		ttl:     ttl,
		enabled: enabled,
	}
}

// Get retrieves cached results for a key if still valid.
func (c *QueryCache) Get(key string) ([]map[string]any, bool) {
	if c == nil || !c.enabled {
		return nil, false
	}
	c.mu.RLock()
	entry, ok := c.entries[key]
	if !ok {
		c.mu.RUnlock()
		return nil, false
	}
	if time.Now().After(entry.expiresAt) {
		c.mu.RUnlock()
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		return nil, false
	}
	res := entry.results
	c.mu.RUnlock()
	return res, true
}

// Set stores query results with TTL.
func (c *QueryCache) Set(key string, results []map[string]any) {
	if c == nil || !c.enabled {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// Lazy cleanup of expired entries if reaching capacity
	if len(c.entries) >= c.maxSize {
		now := time.Now()
		for k, v := range c.entries {
			if now.After(v.expiresAt) {
				delete(c.entries, k)
			}
		}
	}

	if len(c.entries) >= c.maxSize {
		c.evictOldest()
	}
	c.entries[key] = &cacheEntry{
		results:   results,
		expiresAt: time.Now().Add(c.ttl),
		createdAt: time.Now(),
	}
}

// HashKey computes a short hex SHA-256 hash of a query string.
func (c *QueryCache) HashKey(query string) string {
	h := sha256.Sum256([]byte(query))
	return fmt.Sprintf("%x", h[:8])
}

func (c *QueryCache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time
	first := true
	for key, entry := range c.entries {
		if first || entry.createdAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.createdAt
			first = false
		}
	}
	if oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}
