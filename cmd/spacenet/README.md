# cmd/spacenet

Standalone CLI for running `spacenet` with a Skynet-compatible Lua configuration file and all official contrib modules enabled.

## Install

```bash
go install github.com/yuchanns/spacenet/cmd/spacenet@latest
```

## Usage

```bash
spacenet config.lua
```

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-threads` | `0` (`NumCPU`) | Number of actor worker goroutines |
| `-start` | `""` | Override the `start` Lua service launched by `bootstrap` |
| `-basedir` | `""` | Base directory for resolving relative Lua paths |
| `-jit` | `false` | Enable MoeLua's native JIT tier on supported platforms |
| `-version` | `false` | Print version information and exit |

## License

Apache-2.0 — see [LICENSE](LICENSE).
