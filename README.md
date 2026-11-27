# spacenet

[![CI](https://github.com/yuchanns/spacenet/actions/workflows/ci.yml/badge.svg)](https://github.com/yuchanns/spacenet/actions/workflows/ci.yml)

A Go implementation of [cloudwu/skynet](https://github.com/cloudwu/skynet) powered by [yuchanns/moelua](https://github.com/yuchanns/moelua) (Lua 5.5.1).

## Quick Start

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

## License

Apache-2.0 — see [LICENSE](LICENSE).
