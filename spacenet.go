package spacenet

import (
	"context"
	"io"
	"io/fs"
	"time"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/runtime"
	"github.com/yuchanns/spacenet/runtime/contrib"
)

// Contrib extends a spacenet Node with opt-in Lua modules, services, and subsystems.
type Contrib = contrib.Contrib

type nodeConfig struct {
	rt       runtime.Config
	contribs []*contrib.Contrib
}

// Option configures a spacenet Node.
type Option func(*nodeConfig)

// WithThreads sets the number of bounded actor worker goroutines.
func WithThreads(n int) Option {
	return func(c *nodeConfig) { c.rt.Threads = n }
}

// WithMonitorInterval configures the endless-loop monitor polling interval (default 5s).
func WithMonitorInterval(d time.Duration) Option {
	return func(c *nodeConfig) { c.rt.MonitorInterval = d }
}

// WithHarbor sets the 8-bit harbor ID (default 0, standalone).
func WithHarbor(id uint8) Option {
	return func(c *nodeConfig) { c.rt.Harbor = id }
}

// WithJIT enables MoeLua's optional native JIT tier on supported platforms.
func WithJIT() Option {
	return func(c *nodeConfig) { c.rt.JIT = true }
}

// WithBaseDir sets the base directory for resolving relative OS paths.
func WithBaseDir(dir string) Option {
	return func(c *nodeConfig) { c.rt.BaseDir = dir }
}

// WithDisableOSFS prevents Lua loaders from reading files outside mounted fs.FS layers.
func WithDisableOSFS() Option {
	return func(c *nodeConfig) { c.rt.DisableOSFS = true }
}

// WithLogOutput redirects default logger output to w (defaults to os.Stdout).
func WithLogOutput(w io.Writer) Option {
	return func(c *nodeConfig) { c.rt.LogOutput = w }
}

// WithLoggerFile directs the built-in logger service to append to path.
func WithLoggerFile(path string) Option {
	return func(c *nodeConfig) { c.rt.LoggerFile = path }
}

// WithBootstrap overrides the bootstrap service command (default "snlua bootstrap").
func WithBootstrap(cmd string) Option {
	return func(c *nodeConfig) { c.rt.Bootstrap = cmd }
}

// WithStart sets the "start" Lua service launched by bootstrap.lua (default "main").
func WithStart(serviceName string) Option {
	return WithEnv("start", serviceName)
}

// WithEnv sets a single Skynet environment variable accessible via skynet.getenv().
func WithEnv(key, value string) Option {
	return func(c *nodeConfig) {
		if c.rt.Env == nil {
			c.rt.Env = make(map[string]string)
		}
		c.rt.Env[key] = value
	}
}

// WithEnvMap merges a map of Skynet environment variables.
func WithEnvMap(m map[string]string) Option {
	return func(c *nodeConfig) {
		if c.rt.Env == nil {
			c.rt.Env = make(map[string]string, len(m))
		}
		for k, v := range m {
			c.rt.Env[k] = v
		}
	}
}

// WithFS mounts an io/fs.FS (e.g., embed.FS or fstest.MapFS) into the Node's
// LayeredFS ahead of contrib and core embedded assets.
func WithFS(fsys fs.FS) Option {
	return func(c *nodeConfig) {
		if fsys != nil {
			c.rt.FS = append(c.rt.FS, fsys)
		}
	}
}

// WithContrib enables opt-in Go contrib modules (socket, http, tls, crypt, cluster, share, sproto, debug).
func WithContrib(contribs ...*Contrib) Option {
	return func(c *nodeConfig) {
		c.contribs = append(c.contribs, contribs...)
	}
}

// Node is a running spacenet actor runtime instance.
type Node struct {
	rt *runtime.Node
}

// New creates and initializes a spacenet Node with the given options.
// Call Start() to run bootstrap, or Launch() to spawn services directly.
func New(opts ...Option) (*Node, error) {
	var cfg nodeConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if len(cfg.contribs) > 0 {
		ordered, err := contrib.Resolve(cfg.contribs)
		if err != nil {
			return nil, err
		}
		cfg.rt.Preloads = make(map[string]moelua.GoFunc)
		cfg.rt.Services = make(map[string]runtime.ServiceFactory)
		for _, c := range ordered {
			if cfs := c.FS(); cfs != nil {
				cfg.rt.ContribFS = append(cfg.rt.ContribFS, cfs)
			}
			for name, fn := range c.Preloads() {
				cfg.rt.Preloads[name] = fn
			}
			for name, factory := range c.Services() {
				cfg.rt.Services[name] = factory
			}
			if initFn := c.InitFunc(); initFn != nil {
				cfg.rt.InitHooks = append(cfg.rt.InitHooks, runtime.InitHook{
					Name: c.Name(),
					Fn:   initFn,
				})
			}
		}
	}
	rt, err := runtime.NewNode(cfg.rt)
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
