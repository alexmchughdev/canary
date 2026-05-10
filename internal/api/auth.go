package api

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// authed wraps a handler with bearer-token auth. Comparison is constant
// time so a brute-forcer can't time the prefix match.
func (s *Server) authed(h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ah := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(ah, prefix) {
			writeJSONError(w, http.StatusUnauthorized, "missing bearer token")
			return
		}
		if subtle.ConstantTimeCompare([]byte(ah[len(prefix):]), []byte(s.token)) != 1 {
			writeJSONError(w, http.StatusUnauthorized, "invalid token")
			return
		}
		h(w, r)
	})
}
