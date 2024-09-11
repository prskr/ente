package cache

import (
	"context"
	"errors"
)

var ErrCacheMiss = errors.New("cache miss")

type KeyValueCache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte) error
	Unset(ctx context.Context, key string) error
	WithPrefix(prefix string) KeyValueCache
}
