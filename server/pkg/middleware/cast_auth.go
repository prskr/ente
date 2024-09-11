package middleware

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	cache2 "github.com/ente-io/museum/ente/cache"
	"github.com/ente-io/museum/ente/cast"
	castCtrl "github.com/ente-io/museum/pkg/controller/cast"
	"github.com/ente-io/museum/pkg/utils/auth"
)

// CastMiddleware intercepts and authenticates incoming requests
type CastMiddleware struct {
	Cache    cache2.TypedKeyValueCache[*cast.AuthContext]
	CastCtrl *castCtrl.Controller
}

// CastAuthMiddleware returns a middle ware that extracts the `X-AuthToken`
// within the header of a request and uses it to authenticate and insert the
// authenticated user to the request's `X-Auth-User-ID` field.
// If isJWT is true we use JWT token validation
func (m *CastMiddleware) CastAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		token := auth.GetCastToken(c)
		if token == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "cast access token missing"})
			return
		}
		app := auth.GetApp(c)
		cacheKey := fmt.Sprintf("%s:%s:%s", app, token, "cast")
		cachedCastCtx, err := m.Cache.Get(c, cacheKey)
		if err != nil {
			castCtx, err := m.CastCtrl.GetCollectionAndCasterIDForToken(c, token)
			if err != nil {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid token"})
				return
			}
			c.Set(auth.CastContext, castCtx)
			_ = m.Cache.Set(c, cacheKey, castCtx)
			c.Set(auth.CastContext, *castCtx)
		} else {
			c.Set(auth.CastContext, cachedCastCtx)
			// validate async validate that the token is still active
			go func() {
				_, err := m.CastCtrl.GetCollectionAndCasterIDForToken(c, token)
				if err != nil {
					_ = m.Cache.Unset(c, cacheKey)
				}
			}()
		}
		c.Next()
	}
}
