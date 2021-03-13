package middleware

import (
	"net/http"
	"os"
	"strings"
	"time"
    
	"github.com/k8-proxy/k8-go-api/utils"

	"github.com/justinas/alice"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"
)

// AuthMiddleware to check authorization
func AuthMiddleware(next http.Handler) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		errauth := "you don't have  valid authoriaztion token"
		erremptyauth := "you didn't provide authoriaztion token"

		logf := zerolog.Ctx(r.Context())
		failauth := func() {
			logf.UpdateContext(func(c zerolog.Context) zerolog.Context {
				return c.Str("Auth", "Fail")

			})
		}

		//Authorization: Bearer
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			utils.ResponseWithError(w, http.StatusUnauthorized, erremptyauth)
			failauth()
			return
		}

		authHeaderParts := strings.Fields(authHeader)
		if len(authHeaderParts) != 2 || authHeaderParts[0] != "Bearer" {
			utils.ResponseWithError(w, http.StatusUnauthorized, errauth)
			failauth()
			return
		}

		if authHeaderParts[1] != "mysecrettoken" {
			utils.ResponseWithError(w, http.StatusUnauthorized, errauth)
			failauth()
			return
		}

		logf.UpdateContext(func(c zerolog.Context) zerolog.Context {
			return c.Str("Auth", "Success")

		})
		next.ServeHTTP(w, r)
	})
}

func LogMiddleware(next http.Handler) http.Handler {

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log := zerolog.New(os.Stdout).With().
			Timestamp().
			Str("role", "my-service").
			Str("host", "host").
			Logger()

		c := alice.New()

		// Install the logger handler with default output on the console
		c = c.Append(hlog.NewHandler(log))

		c = c.Append(hlog.AccessHandler(func(r *http.Request, status, size int, duration time.Duration) {
			hlog.FromRequest(r).Info().
				Str("method", r.Method).
				Stringer("url", r.URL).
				Int("status", status).
				Int("size", size).
				Dur("duration", duration).
				Msg("")
		}))
		c = c.Append(hlog.RemoteAddrHandler("ip"))
		c = c.Append(hlog.UserAgentHandler("user_agent"))
		c = c.Append(hlog.RefererHandler("referer"))
		c = c.Append(hlog.RequestIDHandler("req_id", "ZlogRequest-Id"))

		h := c.Then(next)
		h.ServeHTTP(w, r)

	})

}
