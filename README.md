# spacenet

[![CI](https://github.com/yuchanns/spacenet/actions/workflows/ci.yml/badge.svg)](https://github.com/yuchanns/spacenet/actions/workflows/ci.yml)

A pure-Go implementation of [cloudwu/skynet](https://github.com/cloudwu/skynet) powered by [yuchanns/moelua](https://github.com/yuchanns/moelua) (Lua 5.5.1).

## Quick Start

### Embedded Node (Lua Services)

```go
package main

import (
	"context"
	"log"
	"testing/fstest"

	"github.com/yuchanns/spacenet"
)

func main() {
	services := fstest.MapFS{
		"service/hello.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
skynet.start(function()
	skynet.error("hello from spacenet")
	skynet.exit()
end)
`)},
	}

	node, err := spacenet.New(
		spacenet.WithFS(services),
		spacenet.WithStart("hello"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()

	if err := node.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
```

### With Contrib Modules (Socket / HTTP / TLS / Sproto)

```go
node, err := spacenet.New(
	spacenet.WithThreads(8),
	spacenet.WithContrib(
		sphttp.New(),
		sptls.New(),
		sproto.New(),
	),
	spacenet.WithFS(appFS),
	spacenet.WithStart("main"),
)
```

Using Go's `net/http` directly with a Lua service:

```go
http.ListenAndServe(":8080", sphttp.NewHandler(node, ".web"))
```

### CLI

```bash
go run ./cmd/spacenet -threads 8 -start main config.lua
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
