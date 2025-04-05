package entcache

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/gob"
	"errors"
	"time"
)

type (
	// A Key defines a comparable Go value.
	// See http://golang.org/ref/spec#Comparison_operators
	Key any

	// AddGetDeleter defines the interface for getting,
	// adding and deleting entries from the cache.
	AddGetDeleter interface {
		Del(context.Context, Key) error
		Add(context.Context, Key, *Entry, time.Duration) error
		Get(context.Context, Key) (*Entry, error)
	}
)

type Entry struct {
	Columns []string         `cbor:"0,keyasint" json:"c" bson:"c"`
	Values  [][]driver.Value `cbor:"1,keyasint" json:"v" bson:"v"`
}

// MarshalBinary implements the encoding.BinaryMarshaler interface.
//
//goland:noinspection GoMixedReceiverTypes
func (e Entry) MarshalBinary() ([]byte, error) {
	entry := struct {
		C []string
		V [][]driver.Value
	}{
		C: e.Columns,
		V: e.Values,
	}
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(entry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface.
//
//goland:noinspection GoMixedReceiverTypes
func (e *Entry) UnmarshalBinary(buf []byte) error {
	var entry struct {
		C []string
		V [][]driver.Value
	}
	if err := gob.NewDecoder(bytes.NewBuffer(buf)).Decode(&entry); err != nil {
		return err
	}
	e.Values = entry.V
	e.Columns = entry.C
	return nil
}

// ErrNotFound returned by Get when and Entry does not exist in the cache.
var ErrNotFound = errors.New("entcache: entry was not found")

type (
	// entry wraps the Entry with additional expiry information.
	entry struct {
		*Entry
		expiry time.Time
	}
)

// multiLevel provides a multi-level cache implementation.
type multiLevel struct {
	levels []AddGetDeleter
}

// Add adds the entry to the cache.
func (m *multiLevel) Add(ctx context.Context, k Key, e *Entry, ttl time.Duration) error {
	for i := range m.levels {
		if err := m.levels[i].Add(ctx, k, e, ttl); err != nil {
			return err
		}
	}
	return nil
}

// Get gets an entry from the cache.
func (m *multiLevel) Get(ctx context.Context, k Key) (*Entry, error) {
	for i := range m.levels {
		switch e, err := m.levels[i].Get(ctx, k); {
		case err == nil:
			return e, nil
		case !errors.Is(err, ErrNotFound):
			return nil, err
		}
	}
	return nil, ErrNotFound
}

// Del deletes an entry from the cache.
func (m *multiLevel) Del(ctx context.Context, k Key) error {
	for i := range m.levels {
		if err := m.levels[i].Del(ctx, k); err != nil {
			return err
		}
	}
	return nil
}

// contextLevel provides a context/request level cache implementation.
type contextLevel struct{}

// Get gets an entry from the cache.
func (*contextLevel) Get(ctx context.Context, k Key) (*Entry, error) {
	c, ok := FromContext(ctx)
	if !ok {
		return nil, ErrNotFound
	}
	return c.Get(ctx, k)
}

// Add adds the entry to the cache.
func (*contextLevel) Add(ctx context.Context, k Key, e *Entry, ttl time.Duration) error {
	c, ok := FromContext(ctx)
	if !ok {
		return nil
	}
	return c.Add(ctx, k, e, ttl)
}

// Del deletes an entry from the cache.
func (*contextLevel) Del(ctx context.Context, k Key) error {
	c, ok := FromContext(ctx)
	if !ok {
		return nil
	}
	return c.Del(ctx, k)
}
