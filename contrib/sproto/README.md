# contrib/sproto

Sproto binary serialization and IDL parser contrib module for `spacenet`, providing `sproto.core`, `sproto`, `sprotoparser`, and `sprotoloader`.

## Usage

```go
import (
	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/sproto"
)

node, err := spacenet.New(
	spacenet.WithContrib(sproto.New()),
	spacenet.WithFS(services),
	spacenet.WithStart("main"),
)
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
