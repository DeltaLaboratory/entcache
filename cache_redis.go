package entcache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/rueidis"
)

// Redis provides a remote cache backed by Redis
// and implements the SetGetter interface.
type Redis struct {
	c rueidis.Client
}

// NewRedis returns a new Redis cache level from the given Redis connection.
func NewRedis(c rueidis.Client) *Redis {
	return &Redis{c: c}
}

// Add adds the entry to the cache.
func (r *Redis) Add(ctx context.Context, k Key, e *Entry, ttl time.Duration) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	buf, err := e.MarshalBinary()
	if err != nil {
		return err
	}
	return r.c.Do(ctx, r.c.B().Set().Key(key).Value(rueidis.BinaryString(buf)).Ex(ttl).Build()).Error()
}

// Get gets an entry from the cache.
func (r *Redis) Get(ctx context.Context, k Key) (*Entry, error) {
	key := fmt.Sprint(k)
	if key == "" {
		return nil, ErrNotFound
	}
	buf, err := r.c.Do(ctx, r.c.B().Get().Key(key).Build()).AsBytes()
	if err != nil || len(buf) == 0 {
		return nil, ErrNotFound
	}
	e := &Entry{}
	if err := e.UnmarshalBinary(buf); err != nil {
		return nil, err
	}
	return e, nil
}

// Del deletes an entry from the cache.
func (r *Redis) Del(ctx context.Context, k Key) error {
	key := fmt.Sprint(k)
	if key == "" {
		return nil
	}
	return r.c.Do(ctx, r.c.B().Del().Key(key).Build()).Error()
}
