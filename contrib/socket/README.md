# contrib/socket

TCP/UDP networking contrib module for `spacenet`, providing `spacenet.socketdriver`, `spacenet.netpack`, `spacenet.socket`, `spacenet.socketchannel`, `spacenet.dns`, `snax.gateserver`, `snax.msgserver`, `snax.loginserver`, and the `gate` service.

## Usage

```go
import (
	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/socket"
)

node, err := spacenet.New(
	spacenet.WithContrib(socket.New()),
	spacenet.WithFS(services),
	spacenet.WithStart("main"),
)
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
