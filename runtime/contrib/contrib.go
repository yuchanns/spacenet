// Package contrib provides a functional-option builder for composing opt-in
// spacenet extension modules.
package contrib

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"slices"
	"sort"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/runtime"
)

// Contrib describes an opt-in extension module for a spacenet Node.
// Only name is required; dependencies, Node initialization hooks, embedded Lua
// filesystems, Lua preload loaders, and native Go services are composed via
// Option functions passed to New.
type Contrib struct {
	name     string
	deps     []*Contrib
	initNode func(*runtime.Node) (io.Closer, error)
	fs       fs.FS
	preloads map[string]moelua.GoFunc
	services map[string]runtime.ServiceFactory
}

// Option configures a Contrib constructed with New.
type Option func(*Contrib)

// New constructs a Contrib with the given unique module name and optional capabilities.
func New(name string, opts ...Option) *Contrib {
	c := &Contrib{name: name}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c
}

// WithDeps declares other Contribs that must be initialized before this Contrib.
func WithDeps(deps ...*Contrib) Option {
	return func(c *Contrib) {
		for _, dep := range deps {
			if dep != nil {
				c.deps = append(c.deps, dep)
			}
		}
	}
}

// WithInit registers a per-Node initialization hook. If it returns a non-nil
// io.Closer, the closer is invoked in reverse dependency order during Node.Close().
func WithInit(fn func(*runtime.Node) (io.Closer, error)) Option {
	return func(c *Contrib) {
		c.initNode = fn
	}
}

// WithFS attaches an embedded Lua library/service filesystem to the Node's LayeredFS.
func WithFS(fsys fs.FS) Option {
	return func(c *Contrib) {
		c.fs = fsys
	}
}

// WithPreload registers a single Lua package.preload module loader.
func WithPreload(name string, fn moelua.GoFunc) Option {
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

// WithPreloads registers multiple Lua package.preload module loaders.
func WithPreloads(m map[string]moelua.GoFunc) Option {
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

// WithService registers a single native Go service factory.
func WithService(name string, factory runtime.ServiceFactory) Option {
	return func(c *Contrib) {
		if name == "" || factory == nil {
			return
		}
		if c.services == nil {
			c.services = make(map[string]runtime.ServiceFactory)
		}
		c.services[name] = factory
	}
}

// WithServices registers multiple native Go service factories.
func WithServices(m map[string]runtime.ServiceFactory) Option {
	return func(c *Contrib) {
		if len(m) == 0 {
			return
		}
		if c.services == nil {
			c.services = make(map[string]runtime.ServiceFactory, len(m))
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
	return slices.Clone(c.deps)
}

func (c *Contrib) InitFunc() func(*runtime.Node) (io.Closer, error) {
	if c == nil {
		return nil
	}
	return c.initNode
}

func (c *Contrib) InitNode(n *runtime.Node) (io.Closer, error) {
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
	return maps.Clone(c.preloads)
}

func (c *Contrib) Services() map[string]runtime.ServiceFactory {
	if c == nil {
		return nil
	}
	return maps.Clone(c.services)
}

// Resolve collects, deduplicates, and topologically orders contribs and their
// transitive dependencies. Direct entries take precedence over transitive
// dependencies with the same name.
func Resolve(contribs []*Contrib) ([]*Contrib, error) {
	byName := make(map[string]*Contrib)
	queue := make([]*Contrib, 0, len(contribs))
	enqueue := func(list []*Contrib) error {
		for _, c := range list {
			if c == nil {
				continue
			}
			name := c.Name()
			if name == "" {
				return errors.New("spacenet: contrib requires a non-empty name")
			}
			if _, exists := byName[name]; exists {
				continue
			}
			byName[name] = c
			queue = append(queue, c)
		}
		return nil
	}
	if err := enqueue(contribs); err != nil {
		return nil, err
	}
	for i := 0; i < len(queue); i++ {
		if err := enqueue(queue[i].deps); err != nil {
			return nil, err
		}
	}

	order := make([]string, 0, len(byName))
	for name := range byName {
		order = append(order, name)
	}
	sort.Strings(order)

	const (
		unvisited = 0
		visiting  = 1
		visited   = 2
	)
	state := make(map[string]int, len(byName))
	var sorted []*Contrib
	var visit func(name string) error
	visit = func(name string) error {
		switch state[name] {
		case visited:
			return nil
		case visiting:
			return fmt.Errorf("spacenet: cyclic contrib dependency involving %q", name)
		}
		c, ok := byName[name]
		if !ok {
			return fmt.Errorf("spacenet: missing required contrib dependency %q", name)
		}
		state[name] = visiting
		for _, dep := range c.Dependencies() {
			if dep == nil {
				continue
			}
			if err := visit(dep.Name()); err != nil {
				return err
			}
		}
		state[name] = visited
		sorted = append(sorted, c)
		return nil
	}
	for _, name := range order {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return sorted, nil
}
