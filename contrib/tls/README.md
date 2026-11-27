# contrib/tls

TLS contrib module for `spacenet` backed by Go's `crypto/tls`, providing `ltls.c` and `ltls.init.c`.

## Usage

```go
import (
	"github.com/yuchanns/spacenet"
	sptls "github.com/yuchanns/spacenet/contrib/tls"
)

node, err := spacenet.New(
	spacenet.WithContrib(sptls.New()),
	spacenet.WithEnv("enablessl", "true"),
	spacenet.WithFS(services),
	spacenet.WithStart("main"),
)
```

Use `sptls.WithBaseConfig(cfg)` to supply a custom base `*tls.Config` cloned for each TLS context.

## License

Apache-2.0 — see [LICENSE](LICENSE).
