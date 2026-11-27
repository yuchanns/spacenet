// Package http provides Skynet's HTTP/1.1 & WebSocket libraries
// (http.httpd, http.httpc, http.websocket, http.url, http.sockethelper,
// http.internal, http.tlshelper) plus a Go net/http.Handler bridge for
// dispatching HTTP requests to Skynet Lua services.
package http

import (
	"context"
	"errors"
	"io"
	"math"
	stdhttp "net/http"
	"strings"

	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/crypt"
	"github.com/yuchanns/spacenet/contrib/socket"
	sptls "github.com/yuchanns/spacenet/contrib/tls"
	"github.com/yuchanns/spacenet/runtime/contrib"
)

const defaultMaxBodyBytes int64 = 10 << 20 // 10 MiB

// New returns the HTTP/WebSocket contrib, declaring dependencies on
// contrib/socket, contrib/crypt, and contrib/tls.
func New() *contrib.Contrib {
	return contrib.New("http",
		contrib.WithDeps(socket.New(), crypt.New(), sptls.New()),
		contrib.WithFS(subFS),
	)
}

// HandlerOption configures a net/http.Handler created by NewHandler.
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	maxBodyBytes int64
}

// WithMaxBodyBytes limits the maximum request body size read by NewHandler.
// A non-positive value disables the limit. Defaults to 10 MiB.
func WithMaxBodyBytes(n int64) HandlerOption {
	return func(c *handlerConfig) { c.maxBodyBytes = n }
}

// NewHandler returns a standard Go net/http.Handler that bridges incoming Go
// HTTP/1.1, HTTP/2, and TLS requests to a Skynet Lua service via PTYPE_LUA RPC.
//
// The target Lua service receives a single request table argument:
//
//	{ method = "GET", path = "/hello", query = "a=1", host = "...", headers = { ... }, body = "..." }
//
// and returns (status_code, body [, response_headers_table]).
// Header values may be either a string or an array of strings.
func NewHandler(node *spacenet.Node, target string, opts ...HandlerOption) stdhttp.Handler {
	cfg := handlerConfig{maxBodyBytes: defaultMaxBodyBytes}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		if node == nil || target == "" {
			stdhttp.Error(w, "spacenet http handler is not configured", stdhttp.StatusServiceUnavailable)
			return
		}
		var bodyBytes []byte
		if r.Body != nil {
			reader := io.Reader(r.Body)
			if cfg.maxBodyBytes > 0 {
				reader = stdhttp.MaxBytesReader(w, r.Body, cfg.maxBodyBytes)
			}
			var err error
			bodyBytes, err = io.ReadAll(reader)
			if err != nil {
				var maxErr *stdhttp.MaxBytesError
				if errors.As(err, &maxErr) {
					stdhttp.Error(w, "request body too large", stdhttp.StatusRequestEntityTooLarge)
					return
				}
				stdhttp.Error(w, "failed to read request body", stdhttp.StatusBadRequest)
				return
			}
		}
		hdrs := make(map[string]any, len(r.Header))
		for k, vals := range r.Header {
			lk := strings.ToLower(k)
			if lk == "cookie" {
				hdrs[lk] = strings.Join(vals, "; ")
			} else {
				hdrs[lk] = strings.Join(vals, ", ")
			}
		}
		req := map[string]any{
			"method":  r.Method,
			"path":    r.URL.Path,
			"query":   r.URL.RawQuery,
			"host":    r.Host,
			"headers": hdrs,
			"body":    string(bodyBytes),
		}

		res, err := node.Call(r.Context(), target, req)
		if err != nil {
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				stdhttp.Error(w, "upstream timeout", stdhttp.StatusGatewayTimeout)
			case errors.Is(err, context.Canceled):
				return
			default:
				stdhttp.Error(w, err.Error(), stdhttp.StatusBadGateway)
			}
			return
		}

		status := stdhttp.StatusOK
		if len(res) >= 1 {
			switch code := res[0].(type) {
			case int64:
				if code >= 100 && code <= 599 {
					status = int(code)
				}
			case float64:
				if !math.IsNaN(code) && !math.IsInf(code, 0) && math.Trunc(code) == code && code >= 100 && code <= 599 {
					status = int(code)
				}
			}
		}
		if len(res) >= 3 {
			if respHdrs, ok := res[2].(map[any]any); ok {
				for k, v := range respHdrs {
					ks, ok1 := k.(string)
					if !ok1 || ks == "" {
						continue
					}
					switch val := v.(type) {
					case string:
						w.Header().Set(ks, val)
					case []any:
						for _, elem := range val {
							if s, ok := elem.(string); ok {
								w.Header().Add(ks, s)
							}
						}
					case map[any]any:
						// Lua array tables decode to map[any]any with 1-based int64 keys.
						for i := int64(1); ; i++ {
							elem, exists := val[i]
							if !exists {
								break
							}
							if s, ok := elem.(string); ok {
								w.Header().Add(ks, s)
							}
						}
					}
				}
			}
		}
		w.WriteHeader(status)
		if len(res) >= 2 {
			if body, ok := res[1].(string); ok {
				_, _ = io.WriteString(w, body)
			}
		}
	})
}
