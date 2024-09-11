package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	_ KeyValueCache = (*RedisKeyValue)(nil)
	_ io.Closer     = (*RedisKeyValue)(nil)
)

func NewRedisKeyValue(defaultExpiration time.Duration, connString string) (*RedisKeyValue, error) {
	opts, err := redis.ParseURL(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare Redis cache: %w", err)
	}

	return &RedisKeyValue{
		TTL:    defaultExpiration,
		Client: redis.NewClient(opts),
	}, nil
}

type RedisKeyValue struct {
	prefix string
	Client *redis.Client
	TTL    time.Duration
}

// Get implements KeyValueCache.
func (r *RedisKeyValue) Get(ctx context.Context, key string) ([]byte, error) {
	cmd := r.Client.GetEx(ctx, r.cacheKey(key), r.TTL)

	data, err := cmd.Bytes()
	if err != nil && errors.Is(err, redis.Nil) {
		return nil, ErrCacheMiss
	}

	return data, nil
}

// Set implements KeyValueCache.
func (r *RedisKeyValue) Set(ctx context.Context, key string, value []byte) error {
	cmd := r.Client.SetEx(ctx, r.cacheKey(key), value, r.TTL)

	return cmd.Err()
}

// Unset implements KeyValueCache.
func (r *RedisKeyValue) Unset(ctx context.Context, key string) error {
	cmd := r.Client.Del(ctx, r.cacheKey(key))
	return cmd.Err()
}

// WithPrefix implements KeyValueCache.
func (r *RedisKeyValue) WithPrefix(prefix string) KeyValueCache {
	return &RedisKeyValue{
		prefix: path.Join(r.prefix, prefix),
		Client: r.Client,
		TTL:    r.TTL,
	}
}

func (r *RedisKeyValue) Close() error {
	return r.Client.Close()
}

func (r *RedisKeyValue) cacheKey(key string) string {
	return path.Join(r.prefix, key)
}
