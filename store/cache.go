package store

import (
	"sync"
	"time"
)

// discoveryCache provides an in-memory TTL cache for infrequently changing
// discovery queries (services, operations, dependencies).
type discoveryCache struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	ttl     time.Duration
}

type cacheEntry struct {
	data      interface{}
	expiresAt time.Time
}

func newDiscoveryCache(ttl time.Duration) *discoveryCache {
	return &discoveryCache{
		entries: make(map[string]*cacheEntry),
		ttl:     ttl,
	}
}

// get returns cached data for key if it exists and hasn't expired.
func (c *discoveryCache) get(key string) (interface{}, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok || time.Now().After(entry.expiresAt) {
		return nil, false
	}
	return entry.data, true
}

// set stores data for key with the configured TTL.
func (c *discoveryCache) set(key string, data interface{}) {
	c.mu.Lock()
	c.entries[key] = &cacheEntry{
		data:      data,
		expiresAt: time.Now().Add(c.ttl),
	}
	c.mu.Unlock()
}

const dependencyCacheKey = "dependencies"

// maxDependencyLookback caps the time window for dependency queries to avoid OOM on large datasets.
const maxDependencyLookback = 2 * time.Hour
