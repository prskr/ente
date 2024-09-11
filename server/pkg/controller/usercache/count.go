package usercache

import (
	"context"

	"github.com/ente-io/stacktrace"
	"github.com/sirupsen/logrus"

	"github.com/ente-io/museum/ente"
	"github.com/ente-io/museum/ente/cache"
)

func (c *Controller) GetUserFileCountWithCache(ctx context.Context, userID int64, app ente.App) (int64, error) {
	// Check if the value is present in the cache
	if count, ok := c.UserCache.GetFileCount(ctx, userID, app); ok {
		// Cache hit, update the cache asynchronously
		go func() {
			_, _ = c.getUserCountAndUpdateCache(ctx, userID, app, count)
		}()
		return count.Count, nil
	}
	return c.getUserCountAndUpdateCache(ctx, userID, app, nil)
}

func (c *Controller) getUserCountAndUpdateCache(ctx context.Context, userID int64, app ente.App, oldCache *cache.FileCountCache) (int64, error) {
	usage, err := c.UsageRepo.GetUsage(userID)
	if err != nil {
		return 0, stacktrace.Propagate(err, "")
	}
	trashUpdatedAt, err := c.TrashRepo.GetTrashUpdatedAt(userID)
	if err != nil {
		return 0, stacktrace.Propagate(err, "")
	}
	if oldCache != nil && oldCache.Usage == usage && oldCache.TrashUpdatedAt == trashUpdatedAt {
		logrus.Debugf("Cache hit for user %d", userID)
		return oldCache.Count, nil
	}
	count, err := c.FileRepo.GetFileCountForUser(userID, app)
	if err != nil {
		return 0, stacktrace.Propagate(err, "")
	}
	cntCache := &cache.FileCountCache{
		Count:          count,
		Usage:          usage,
		TrashUpdatedAt: trashUpdatedAt,
	}

	return count, c.UserCache.SetFileCount(ctx, userID, cntCache, app)
}
