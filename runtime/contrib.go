package runtime

import (
	"io"
	"io/fs"

	"github.com/yuchanns/moelua"
)

// Contrib describes an opt-in extension module for a spacenet Node.
// Only name is required; dependencies, Node initialization hooks, embedded Lua
// filesystems, Lua preload loaders, and native Go services are composed via
// ContribOption functions passed to NewContrib.
type Contrib struct {
	name     string
	deps     []*Contrib
	initNode func(*Node) (io.Closer, error)
	fs       fs.FS
	preloads map[string]moelua.GoFunc
	services map[string]ServiceFactory
}

// ContribOption configures a Contrib constructed with NewContrib.
type ContribOption func(*Contrib)

// NewContrib constructs a Contrib with the given unique module name and optional capabilities.
func NewContrib(name string, opts ...ContribOption) *Contrib {
	c := &Contrib{name: name}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// WithContribDeps declares other Contribs that must be initialized before this Contrib.
func WithContribDeps(deps ...*Contrib) ContribOption {
	return func(c *Contrib) {
		for _, dep := range deps {
			if dep != nil {
				c.deps = append(c.deps, dep)
			}
		}
	}
}

// WithContribInit registers a per-Node initialization hook. If it returns a non-nil
// io.Closer, the closer is invoked in reverse dependency order during Node.Close().
func WithContribInit(fn func(*Node) (io.Closer, error)) ContribOption {
	return func(c *Contrib) {
		c.initNode = fn
	}
}

// WithContribFS attaches an embedded Lua library/service filesystem to the Node's LayeredFS.
func WithContribFS(fsys fs.FS) ContribOption {
	return func(c *Contrib) {
		c.fs = fsys
	}
}

// WithContribPreload registers a single Lua package.preload module loader.
func WithContribPreload(name string, fn moelua.GoFunc) ContribOption {
	return func(c *Contrib) {
		if name == "" || fn == nil {
			return
		}
		if c.preloads == nil {
			c.preloads = make(map[string]moelua.GoFunc)
		}
		c.preloads[name] = fn
	}
}

// WithContribPreloads registers multiple Lua package.preload module loaders.
func WithContribPreloads(m map[string]moelua.GoFunc) ContribOption {
	return func(c *Contrib) {
		if len(m) == 0 {
			return
		}
		if c.preloads == nil {
			c.preloads = make(map[string]moelua.GoFunc, len(m))
		}
		for k, v := range m {
			if k != "" && v != nil {
				c.preloads[k] = v
			}
		}
	}
}

// WithContribService registers a single native Go service factory.
func WithContribService(name string, factory ServiceFactory) ContribOption {
	return func(c *Contrib) {
		if name == "" || factory == nil {
			return
		}
		if c.services == nil {
			c.services = make(map[string]ServiceFactory)
		}
		c.services[name] = factory
	}
}

// WithContribServices registers multiple native Go service factories.
func WithContribServices(m map[string]ServiceFactory) ContribOption {
	return func(c *Contrib) {
		if len(m) == 0 {
			return
		}
		if c.services == nil {
			c.services = make(map[string]ServiceFactory, len(m))
		}
		for k, v := range m {
			if k != "" && v != nil {
				c.services[k] = v
			}
		}
	}
}

func (c *Contrib) Name() string {
	if c == nil {
		return ""
	}
	return c.name
}

func (c *Contrib) Dependencies() []*Contrib {
	if c == nil {
		return nil
	}
	return c.deps
}

func (c *Contrib) InitNode(n *Node) (io.Closer, error) {
	if c == nil || c.initNode == nil {
		return nil, nil
	}
	return c.initNode(n)
}

func (c *Contrib) FS() fs.FS {
	if c == nil {
		return nil
	}
	return c.fs
}

func (c *Contrib) Preloads() map[string]moelua.GoFunc {
	if c == nil {
		return nil
	}
	return c.preloads
}

func (c *Contrib) Services() map[string]ServiceFactory {
	if c == nil {
		return nil
	}
	return c.services
}
