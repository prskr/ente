package cache

import (
	"context"
	"path"
	"time"

	"github.com/patrickmn/go-cache"
)

var _ KeyValueCache = (*InMemoryKeyValue)(nil)

func NewInMemoryKeyValue(defaultExpiration, cleanupInterval time.Duration) *InMemoryKeyValue {
	return &InMemoryKeyValue{
		Cache: cache.New(defaultExpiration, cleanupInterval),
	}
}

type InMemoryKeyValue struct {
	prefix string
	Cache  *cache.Cache
}

func (i InMemoryKeyValue) WithPrefix(prefix string) KeyValueCache {
	return InMemoryKeyValue{
		prefix: path.Join(i.prefix, prefix),
		Cache:  i.Cache,
	}
}

func (i InMemoryKeyValue) Get(_ context.Context, key string) ([]byte, error) {
	val, found := i.Cache.Get(i.cacheKey(key))
	if !found || val == nil {
		return nil, ErrCacheMiss
	}

	data, ok := val.([]byte)
	if !ok {
		return nil, ErrCacheMiss
	}

	return data, nil
}

func (i InMemoryKeyValue) Set(_ context.Context, key string, value []byte) error {
	i.Cache.Set(i.cacheKey(key), value, cache.DefaultExpiration)
	return nil
}

func (i InMemoryKeyValue) Unset(_ context.Context, key string) error {
	i.Cache.Delete(i.cacheKey(key))
	return nil
}

func (r InMemoryKeyValue) cacheKey(key string) string {
	return path.Join(r.prefix, key)
}
