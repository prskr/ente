package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/valkey"
)

var (
	valkeyContainer *valkey.ValkeyContainer
)

func TestMain(m *testing.M) {
	deadline := time.Now().Add(60 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)

	var err error
	valkeyContainer, err = valkey.Run(ctx,
		"docker.io/valkey/valkey:8.0-alpine",
		valkey.WithLogLevel(valkey.LogLevelNotice),
	)
	if err != nil {
		panic(err)
	}

	cancel()

	m.Run()

	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	valkeyContainer.Terminate(ctx)
	cancel()
}
