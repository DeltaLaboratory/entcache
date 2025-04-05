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
	cache bool          // i.e. cache entry.
	evict bool          // i.e. cache and invalidate entry.
	key   Key           // entry key.
	ttl   time.Duration // entry duration.
}

var ctxOptionsKey ctxOptions

// Cache returns a new Context that tells the Driver
// to cache the cache entry on Query.
//
//	client.T.Query().All(entcache.Cache(ctx))
func Cache(ctx context.Context) context.Context {
	c, ok := ctx.Value(ctxOptionsKey).(*ctxOptions)
	if !ok {
		return context.WithValue(ctx, ctxOptionsKey, &ctxOptions{cache: true})
	}
	c.cache = true
	return ctx
}

// Evict returns a new Context that tells the Driver
// to cache and invalidate the cache entry on Query.
//
//	client.T.Query().All(entcache.Evict(ctx))
func Evict(ctx context.Context) context.Context {
	c, ok := ctx.Value(ctxOptionsKey).(*ctxOptions)
	if !ok {
		return context.WithValue(ctx, ctxOptionsKey, &ctxOptions{cache: true, evict: true})
	}
	c.cache = true
	c.evict = true
	return ctx
}

// WithKey returns a new Context that carries the Key for the cache entry.
// Note that this option should not be used if the ent.Client query involves
// more than 1 SQL query (e.g., eager loading).
//
//	client.T.Query().All(entcache.WithKey(ctx, "key"))
func WithKey(ctx context.Context, key Key) context.Context {
	c, ok := ctx.Value(ctxOptionsKey).(*ctxOptions)
	if !ok {
		return context.WithValue(ctx, ctxOptionsKey, &ctxOptions{key: key})
	}
	c.key = key
	return ctx
}

// WithTTL returns a new Context that carries the TTL for the cache entry.
//
//	client.T.Query().All(entcache.WithTTL(ctx, time.Second))
func WithTTL(ctx context.Context, ttl time.Duration) context.Context {
	c, ok := ctx.Value(ctxOptionsKey).(*ctxOptions)
	if !ok {
		return context.WithValue(ctx, ctxOptionsKey, &ctxOptions{ttl: ttl})
	}
	c.ttl = ttl
	return ctx
}
