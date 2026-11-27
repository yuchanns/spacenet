# contrib/socket

TCP/UDP networking contrib module for `spacenet`, providing `skynet.socketdriver`, `skynet.netpack`, `skynet.socket`, `skynet.socketchannel`, `skynet.dns`, `snax.gateserver`, `snax.msgserver`, `snax.loginserver`, and the `gate` service.

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
