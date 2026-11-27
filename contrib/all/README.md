# contrib/all

Convenience aggregator returning all official `spacenet` contrib modules (`socket`, `crypt`, `tls`, `http`, `cluster`, `share`, `sproto`, and `debug`).

## Usage

```go
import (
	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/all"
)

node, err := spacenet.New(
	spacenet.WithContrib(all.Contribs()...),
	spacenet.WithFS(services),
	spacenet.WithStart("main"),
)
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
