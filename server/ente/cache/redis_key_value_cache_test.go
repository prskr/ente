package cache_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ente-io/museum/ente/cache"
)

func Test_RedisKeyValue_Get(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		seed    map[string][]byte
		key     string
		want    []byte
		wantErr func(tb testing.TB, err error) error
	}{
		{
			name:    "Cache empty",
			key:     "hello",
			wantErr: ignoreCacheMissError,
		},
		{
			name: "No cache miss",
			key:  "hello",
			seed: map[string][]byte{
				"hello": []byte("world"),
			},
			want:    []byte("world"),
			wantErr: wantNoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := testContext(t)
			connString, err := valkeyContainer.ConnectionString(ctx)
			if err != nil {
				t.Fatalf("failed to get Redis connection string: %v", err)
			}

			redisKV, err := cache.NewRedisKeyValue(10*time.Minute, connString)
			if err != nil {
				t.Fatalf("failed to create Redis KV: %v", err)
			}

			t.Cleanup(func() {
				if err := redisKV.Close(); err != nil {
					t.Errorf("failed to close Redis connection: %v", err)
				}
			})

			kv := redisKV.WithPrefix(t.Name())

			if tt.seed != nil {
				for k, v := range tt.seed {
					kv.Set(ctx, k, v)
				}
			}

			raw, err := kv.Get(ctx, tt.key)
			if err := tt.wantErr(t, err); err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if !bytes.Equal(tt.want, raw) {
				t.Errorf("value %x, expected %x", raw, tt.want)
			}
		})
	}
}

func Test_RedisKeyValue_Set(t *testing.T) {
	t.Parallel()

	type args struct {
		key   string
		value []byte
	}

	tests := []struct {
		name    string
		args    args
		seed    map[string][]byte
		want    map[string][]byte
		wantErr func(tb testing.TB, err error) error
	}{
		{
			name: "Empty cache",
			args: args{
				key:   "hello",
				value: []byte("world"),
			},
			want: map[string][]byte{
				"hello": []byte("world"),
			},
			wantErr: wantNoError,
		},
		{
			name: "Override existing  value",
			args: args{
				key:   "hello",
				value: []byte("world"),
			},
			seed: map[string][]byte{
				"hello": []byte("go"),
			},
			want: map[string][]byte{
				"hello": []byte("world"),
			},
			wantErr: wantNoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := testContext(t)
			connString, err := valkeyContainer.ConnectionString(ctx)
			if err != nil {
				t.Fatalf("failed to get Redis connection string: %v", err)
			}

			redisKV, err := cache.NewRedisKeyValue(10*time.Minute, connString)
			if err != nil {
				t.Fatalf("failed to create Redis KV: %v", err)
			}

			t.Cleanup(func() {
				if err := redisKV.Close(); err != nil {
					t.Errorf("failed to close Redis connection: %v", err)
				}
			})

			kv := redisKV.WithPrefix(t.Name())

			if tt.seed != nil {
				for k, v := range tt.seed {
					kv.Set(ctx, k, v)
				}
			}

			if err := tt.wantErr(t, kv.Set(ctx, tt.args.key, tt.args.value)); err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			for k, v := range tt.want {
				val, err := kv.Get(ctx, k)
				if err := tt.wantErr(t, err); err != nil {
					t.Errorf("unexpected error: %v", err)
					return
				}

				if !bytes.Equal(v, val) {
					t.Errorf("want %x, got %x", v, val)
				}
			}
		})
	}
}

func wantNoError(tb testing.TB, err error) error {
	tb.Helper()
	return err
}

func ignoreCacheMissError(tb testing.TB, err error) error {
	tb.Helper()
	if err != nil && !errors.Is(err, cache.ErrCacheMiss) {
		return err
	}

	return nil
}

func testContext(t contextableTest) context.Context {
	t.Helper()

	deadline, ok := t.Deadline()
	if !ok {
		deadline = time.Now().Add(30 * time.Second)
	}
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	t.Cleanup(cancel)

	return ctx
}

type contextableTest interface {
	Helper()
	Deadline() (time.Time, bool)
	Cleanup(func())
}
