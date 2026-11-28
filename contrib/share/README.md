# contrib/share

Cross-actor data sharing and pub/sub contrib module for `spacenet`, providing `spacenet.sharetable`, `spacenet.datasheet`, `spacenet.sharedata`, `spacenet.sharemap`, `spacenet.stm`, and `spacenet.multicast`.

Declares `contrib/sproto` as a dependency for `spacenet.sharemap`.

## Usage

```go
import (
	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/share"
)

node, err := spacenet.New(
	spacenet.WithContrib(share.New()),
	spacenet.WithFS(services),
	spacenet.WithStart("main"),
)
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
