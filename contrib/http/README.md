# contrib/http

HTTP/1.1, HTTPS, and WebSocket contrib module for `spacenet`, providing `http.httpd`, `http.httpc`, `http.websocket`, `http.url`, `http.sockethelper`, `http.internal`, `http.tlshelper`, and a Go `net/http.Handler` bridge (`NewHandler`).

Dependencies (`contrib/socket`, `contrib/crypt`, `contrib/tls`) are resolved automatically via `contrib.WithDeps`.

## Usage

### Skynet `http.httpd` / `http.httpc` / `http.websocket`

```go
import (
	"context"

	"github.com/yuchanns/spacenet"
	sphttp "github.com/yuchanns/spacenet/contrib/http"
)

node, err := spacenet.New(
	spacenet.WithContrib(sphttp.New()),
	spacenet.WithFS(services),
	spacenet.WithStart("main"),
)
if err != nil {
	log.Fatal(err)
}
defer node.Close()
log.Fatal(node.Run(context.Background()))
```

### Integrating with Go's `net/http` Server

```go
import (
	"log"
	"net/http"

	"github.com/yuchanns/spacenet"
	sphttp "github.com/yuchanns/spacenet/contrib/http"
)

node, err := spacenet.New(
	spacenet.WithContrib(sphttp.New()),
	spacenet.WithFS(services),
	spacenet.WithStart("web"),
)
if err != nil {
	log.Fatal(err)
}
defer node.Close()
if err := node.Start(); err != nil {
	log.Fatal(err)
}

log.Fatal(http.ListenAndServe(":8080", sphttp.NewHandler(node, ".web")))
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
