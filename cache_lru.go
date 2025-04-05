package entcache

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
)

// LRU provides an LRU cache that implements the AddGetter interface.
type LRU struct {
	mu sync.Mutex
	*lru.Cache
}

// NewLRU creates a new Cache.
// If maxEntries is zero, the cache has no limit.
func NewLRU(maxEntries int) *LRU {
	return &LRU{
		Cache: lru.New(maxEntries),
	}
}

// Add adds the entry to the cache.
func (l *LRU) Add(_ context.Context, k Key, e *Entry, ttl time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	ne := &Entry{}
	if err := ne.UnmarshalBinary(buf); err != nil {
		return err
	}
	if ttl == 0 {
		l.Cache.Add(k, ne)
	} else {
		l.Cache.Add(k, &entry{Entry: ne, expiry: time.Now().Add(ttl)})
	}
	return nil
}

// Get gets an entry from the cache.
func (l *LRU) Get(_ context.Context, k Key) (*Entry, error) {
	l.mu.Lock()
	e, ok := l.Cache.Get(k)
	l.mu.Unlock()
	if !ok {
		return nil, ErrNotFound
	}
	switch e := e.(type) {
	case *Entry:
		return e, nil
	case *entry:
		if time.Now().Before(e.expiry) {
			return e.Entry, nil
		}
		l.mu.Lock()
		l.Cache.Remove(k)
		l.mu.Unlock()
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("entcache: unexpected entry type: %T", e)
	}
}

// Del deletes an entry from the cache.
func (l *LRU) Del(_ context.Context, k Key) error {
	l.mu.Lock()
	l.Cache.Remove(k)
	l.mu.Unlock()
	return nil
}
