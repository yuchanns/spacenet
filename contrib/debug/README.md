# contrib/debug

Debug console, memory inspection, and remote debugging contrib module for `spacenet`, providing `spacenet.memory`, `spacenet.debugchannel`, `spacenet.inject`, `spacenet.injectcode`, `spacenet.remotedebug`, `debug_console`, `debug_agent`, `console`, `cmemory`, and `dbg`.

Declares `contrib/http` as a dependency.

## Usage

```go
import (
	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/debug"
)

node, err := spacenet.New(
	spacenet.WithContrib(debug.New()),
	spacenet.WithFS(services),
	spacenet.WithStart("main"),
)
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
