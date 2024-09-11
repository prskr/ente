package cache

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ente-io/museum/ente"
	"github.com/ente-io/museum/ente/storagebonus"
)

// UserCache struct holds can be used to fileCount various entities for user.
type UserCache struct {
	fileCache  TypedKeyValueCache[*FileCountCache]
	bonusCache TypedKeyValueCache[*storagebonus.ActiveStorageBonus]
}

type FileCountCache struct {
	Count          int64
	TrashUpdatedAt int64
	Usage          int64
}

// NewUserCache creates a new instance of the UserCache struct.
func NewUserCache(cache KeyValueCache) *UserCache {
	return &UserCache{
		fileCache:  NewTypedKeyValueCache[*FileCountCache](cache.WithPrefix("fileCount/")),
		bonusCache: NewTypedKeyValueCache[*storagebonus.ActiveStorageBonus](cache.WithPrefix("bonus/")),
	}
}

// SetFileCount updates the fileCount with the given userID and fileCount.
func (c *UserCache) SetFileCount(ctx context.Context, userID int64, fileCount *FileCountCache, app ente.App) error {
	return c.fileCache.Set(ctx, cacheKey(userID, app), fileCount)
}

func (c *UserCache) SetBonus(ctx context.Context, userID int64, bonus *storagebonus.ActiveStorageBonus) error {
	return c.bonusCache.Set(ctx, strconv.FormatInt(userID, 10), bonus)
}

func (c *UserCache) GetBonus(ctx context.Context, userID int64) (*storagebonus.ActiveStorageBonus, bool) {
	val, err := c.bonusCache.Get(ctx, strconv.FormatInt(userID, 10))
	if err != nil {
		return nil, false
	}

	return val, true
}

// GetFileCount retrieves the file count from the fileCount for the given userID.
// It returns the file count and a boolean indicating if the value was found.
func (c *UserCache) GetFileCount(ctx context.Context, userID int64, app ente.App) (*FileCountCache, bool) {
	val, err := c.fileCache.Get(ctx, cacheKey(userID, app))
	if err != nil {
		return nil, false
	}

	return val, true
}

func cacheKey(userID int64, app ente.App) string {
	return fmt.Sprintf("%d-%s", userID, app)
}
