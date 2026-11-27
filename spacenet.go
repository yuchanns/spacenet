package spacenet

import (
	"context"
	"io"
	"io/fs"
	"time"

	"github.com/yuchanns/spacenet/runtime"
)

// Contrib extends a spacenet Node with opt-in Lua modules, services, and subsystems.
type Contrib = runtime.Contrib

// Option configures a spacenet Node.
type Option func(*runtime.Config)

// WithThreads sets the number of bounded actor worker goroutines.
func WithThreads(n int) Option {
	return func(c *runtime.Config) { c.Threads = n }
}

// WithMonitorInterval configures the endless-loop monitor polling interval (default 5s).
func WithMonitorInterval(d time.Duration) Option {
	return func(c *runtime.Config) { c.MonitorInterval = d }
}

// WithHarbor sets the 8-bit harbor ID (default 0, standalone).
func WithHarbor(id uint8) Option {
	return func(c *runtime.Config) { c.Harbor = id }
}

// WithJIT enables MoeLua's optional native JIT tier on supported platforms.
func WithJIT() Option {
	return func(c *runtime.Config) { c.JIT = true }
}

// WithBaseDir sets the base directory for resolving relative OS paths.
func WithBaseDir(dir string) Option {
	return func(c *runtime.Config) { c.BaseDir = dir }
}

// WithDisableOSFS prevents Lua loaders from reading files outside mounted fs.FS layers.
func WithDisableOSFS() Option {
	return func(c *runtime.Config) { c.DisableOSFS = true }
}

// WithLogOutput redirects default logger output to w (defaults to os.Stdout).
func WithLogOutput(w io.Writer) Option {
	return func(c *runtime.Config) { c.LogOutput = w }
}

// WithLoggerFile directs the built-in logger service to append to path.
func WithLoggerFile(path string) Option {
	return func(c *runtime.Config) { c.LoggerFile = path }
}

// WithBootstrap overrides the bootstrap service command (default "snlua bootstrap").
func WithBootstrap(cmd string) Option {
	return func(c *runtime.Config) { c.Bootstrap = cmd }
}

// WithStart sets the "start" Lua service launched by bootstrap.lua (default "main").
func WithStart(serviceName string) Option {
	return WithEnv("start", serviceName)
}

// WithEnv sets a single Skynet environment variable accessible via skynet.getenv().
func WithEnv(key, value string) Option {
	return func(c *runtime.Config) {
		if c.Env == nil {
			c.Env = make(map[string]string)
		}
		c.Env[key] = value
	}
}

// WithEnvMap merges a map of Skynet environment variables.
func WithEnvMap(m map[string]string) Option {
	return func(c *runtime.Config) {
		if c.Env == nil {
			c.Env = make(map[string]string, len(m))
		}
		for k, v := range m {
			c.Env[k] = v
		}
	}
}

// WithFS mounts an io/fs.FS (e.g., embed.FS or fstest.MapFS) into the Node's
// LayeredFS ahead of plugin and core embedded assets.
func WithFS(fsys fs.FS) Option {
	return func(c *runtime.Config) {
		if fsys != nil {
			c.FS = append(c.FS, fsys)
		}
	}
}

// WithContrib enables opt-in Go contrib modules (socket, http, tls, crypt, cluster, share, sproto, debug).
func WithContrib(contribs ...Contrib) Option {
	return func(c *runtime.Config) {
		c.Contribs = append(c.Contribs, contribs...)
	}
}

// Node is a running spacenet actor runtime instance.
type Node struct {
	rt *runtime.Node
}

// New creates and initializes a spacenet Node with the given options.
// Call Start() to run bootstrap, or Launch() to spawn services directly.
func New(opts ...Option) (*Node, error) {
	var cfg runtime.Config
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	rt, err := runtime.NewNode(cfg)
	if err != nil {
		return nil, err
	}
	return &Node{rt: rt}, nil
}

// Runtime exposes the underlying *runtime.Node for plugin authors and advanced integrations.
func (n *Node) Runtime() *runtime.Node { return n.rt }

// Start launches the configured bootstrap service.
func (n *Node) Start() error { return n.rt.Start() }

// Wait blocks until all non-reserved actors exit or ctx is canceled.
func (n *Node) Wait(ctx context.Context) error { return n.rt.Wait(ctx) }

// Run starts bootstrap and blocks until all actors exit or ctx is canceled.
func (n *Node) Run(ctx context.Context) error {
	if err := n.Start(); err != nil {
		return err
	}
	return n.Wait(ctx)
}

// Launch spawns a service directly (e.g. n.Launch("snlua", "myservice arg1")).
func (n *Node) Launch(serviceName, param string) (uint32, error) {
	return n.rt.Launch(serviceName, param)
}

// Call sends a PTYPE_LUA RPC call from Go to a named (".svc") or hex (":00000005") service.
func (n *Node) Call(ctx context.Context, target string, args ...any) ([]any, error) {
	return n.rt.CallLua(ctx, target, args...)
}

// Send delivers a one-way PTYPE_LUA message from Go to a Lua service.
func (n *Node) Send(target string, args ...any) error {
	return n.rt.SendLua(target, args...)
}

// GetEnv returns a Skynet environment variable.
func (n *Node) GetEnv(key string) (string, bool) { return n.rt.GetEnv(key) }

// SetEnv sets a Skynet environment variable.
func (n *Node) SetEnv(key, val string) { n.rt.SetEnv(key, val) }

// Abort retires all actors to initiate clean shutdown.
func (n *Node) Abort() { n.rt.Abort() }

// Close terminates the Node and releases all resources.
func (n *Node) Close() error { return n.rt.Close() }
