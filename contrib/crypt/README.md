# contrib/crypt

Cryptography contrib module for `spacenet`, providing `skynet.crypt`, `client.crypt`, `md5.core`, and `md5`.

## Usage

```go
import (
	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/crypt"
)

node, err := spacenet.New(
	spacenet.WithContrib(crypt.New()),
	spacenet.WithFS(services),
	spacenet.WithStart("main"),
)
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
