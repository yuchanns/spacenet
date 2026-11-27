# contrib/cluster

Multi-node RPC and cluster framing contrib module for `spacenet`, providing `skynet.cluster.core`, `skynet.cluster`, `clusterd`, `clusteragent`, `clustersender`, and `clusterproxy`.

Declares `contrib/socket` as a dependency.

## Usage

```go
import (
	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/cluster"
)

node, err := spacenet.New(
	spacenet.WithContrib(cluster.New()),
	spacenet.WithFS(services),
	spacenet.WithStart("main"),
)
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
