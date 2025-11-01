package entcache

import (
	"context"
	"time"
)

type ctxKey struct{}

// NewContext returns a new Context that carries a cache.
func NewContext(ctx context.Context, levels ...AddGetDeleter) context.Context {
	var cache AddGetDeleter
	switch len(levels) {
	case 0:
		cache = NewLRU(0)
	case 1:
		cache = levels[0]
	default:
		cache = &multiLevel{levels: levels}
	}
	return context.WithValue(ctx, ctxKey{}, cache)
}

// FromContext returns the cache value stored in ctx, if any.
func FromContext(ctx context.Context) (AddGetDeleter, bool) {
	c, ok := ctx.Value(ctxKey{}).(AddGetDeleter)
	return c, ok
}

// ctxOptions allows injecting runtime options.
type ctxOptions struct {
	cache     bool          // i.e. cache entry.
	evict     bool          // i.e. cache and invalidate entry.
	cacheOnly bool          // i.e. skip database execution, cache-only operation.
	key       Key           // entry key.
	ttl       time.Duration // entry duration.
}

var ctxOptionsKey ctxOptions

// QueryOption configures cache behavior for a query.
type QueryOption func(*ctxOptions)

// CacheOnly configures the driver to skip database execution.
// When used alone, reads from cache and returns empty result if not cached.
// When combined with Evict(), invalidates the cache without executing the query.
//
//	// Read from cache only (no DB fallback)
//	users, err := client.User.Query().All(entcache.Cache(ctx, CacheOnly()))
//
//	// Invalidate without executing query
//	_, err := client.User.Query().Where(...).All(entcache.Cache(ctx, CacheOnly(), Evict()))
func CacheOnly() QueryOption {
	return func(o *ctxOptions) {
		o.cacheOnly = true
	}
}

// Evict invalidates the cache entry after determining its key.
// When used alone, executes the query and invalidates the cached result.
// When combined with CacheOnly(), invalidates without executing.
//
//	// Execute and invalidate
//	users, err := client.User.Query().All(entcache.Cache(ctx, Evict()))
//
//	// Invalidate only (no execution)
//	_, err := client.User.Query().Where(...).All(entcache.Cache(ctx, CacheOnly(), Evict()))
func Evict() QueryOption {
	return func(o *ctxOptions) {
		o.evict = true
	}
}

// WithKey sets a custom cache key instead of generating one from the query.
// Note that this option should not be used if the ent.Client query involves
// more than 1 SQL query (e.g., eager loading).
//
//	users, err := client.User.Query().All(entcache.Cache(ctx, WithKey("my-key")))
func WithKey(key Key) QueryOption {
	return func(o *ctxOptions) {
		o.key = key
	}
}

// WithTTL sets a custom TTL for this cache entry.
//
//	users, err := client.User.Query().All(entcache.Cache(ctx, WithTTL(time.Hour)))
func WithTTL(ttl time.Duration) QueryOption {
	return func(o *ctxOptions) {
		o.ttl = ttl
	}
}

// Cache returns a context that enables caching for the query.
// Accepts optional configuration via functional options.
//
// Examples:
//
//	// Basic caching
//	ctx := entcache.Cache(ctx)
//
//	// With custom TTL
//	ctx := entcache.Cache(ctx, WithTTL(time.Hour))
//
//	// Cache-only read (no DB fallback)
//	ctx := entcache.Cache(ctx, CacheOnly())
//
//	// Invalidate without execution
//	ctx := entcache.Cache(ctx, CacheOnly(), Evict())
//
//	// Execute, cache, and set custom key
//	ctx := entcache.Cache(ctx, WithKey("my-key"), WithTTL(time.Minute))
func Cache(ctx context.Context, opts ...QueryOption) context.Context {
	o := &ctxOptions{cache: true}
	for _, opt := range opts {
		opt(o)
	}
	return context.WithValue(ctx, ctxOptionsKey, o)
}
