package cache

import (
	"context"
	"encoding/json"
)

func NewTypedKeyValueCache[V any](cache KeyValueCache) TypedKeyValueCache[V] {
	return TypedKeyValueCache[V]{
		Cache: cache,
	}
}

type TypedKeyValueCache[V any] struct {
	Cache KeyValueCache
}

func (c TypedKeyValueCache[V]) Get(ctx context.Context, key string) (val V, err error) {
	raw, err := c.Cache.Get(ctx, key)
	if err != nil {
		return val, err
	}

	if err = json.Unmarshal(raw, val); err != nil {
		return val, err
	}

	return val, nil
}

func (c TypedKeyValueCache[V]) Set(ctx context.Context, key string, val V) error {
	raw, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return c.Cache.Set(ctx, key, raw)
}

func (c TypedKeyValueCache[V]) Unset(ctx context.Context, key string) error {
	return c.Cache.Unset(ctx, key)
}
