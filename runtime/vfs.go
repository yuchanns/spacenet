package runtime

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/yuchanns/moelua"
)

// LayeredFS resolves Lua source files across priority-ordered layers:
//  1. Local OS disk (unless disabled)
//  2. User-provided fs.FS layers (WithFS)
//  3. Opt-in plugin embedded fs.FS layers
//  4. Core embedded Skynet Lua fs.FS
type LayeredFS struct {
	baseDir     string
	disableOSFS bool
	layers      []fs.FS
}

func NewLayeredFS(baseDir string, disableOSFS bool, layers ...fs.FS) *LayeredFS {
	out := make([]fs.FS, 0, len(layers))
	for _, l := range layers {
		if l != nil {
			out = append(out, l)
		}
	}
	return &LayeredFS{
		baseDir:     baseDir,
		disableOSFS: disableOSFS,
		layers:      out,
	}
}

func normalizeVFSPath(p string) string {
	p = strings.ReplaceAll(p, "\\", "/")
	cleaned := path.Clean(p)
	cleaned = strings.TrimPrefix(cleaned, "./")
	cleaned = strings.TrimPrefix(cleaned, "/")
	return cleaned
}

func (l *LayeredFS) resolveOSPath(name string) string {
	if filepath.IsAbs(name) || l.baseDir == "" {
		return name
	}
	return filepath.Join(l.baseDir, filepath.FromSlash(name))
}

// Exists reports whether name refers to a readable file in any layer.
func (l *LayeredFS) Exists(name string) bool {
	if !l.disableOSFS {
		osPath := l.resolveOSPath(name)
		if fi, err := os.Stat(osPath); err == nil && !fi.IsDir() {
			return true
		}
	}
	vpath := normalizeVFSPath(name)
	if vpath == "." || vpath == "" || !fs.ValidPath(vpath) {
		return false
	}
	for _, layer := range l.layers {
		if fi, err := fs.Stat(layer, vpath); err == nil && !fi.IsDir() {
			return true
		}
	}
	return false
}

// ReadFile reads name from the highest-priority layer that contains it.
func (l *LayeredFS) ReadFile(name string) ([]byte, string, error) {
	if !l.disableOSFS {
		osPath := l.resolveOSPath(name)
		if fi, err := os.Stat(osPath); err == nil && !fi.IsDir() {
			data, err := os.ReadFile(osPath)
			if err == nil {
				return data, name, nil
			}
		}
	}
	vpath := normalizeVFSPath(name)
	if vpath != "." && vpath != "" && fs.ValidPath(vpath) {
		for _, layer := range l.layers {
			if fi, err := fs.Stat(layer, vpath); err == nil && !fi.IsDir() {
				data, err := fs.ReadFile(layer, vpath)
				if err == nil {
					return data, name, nil
				}
			}
		}
	}
	return nil, "", fmt.Errorf("cannot open %s: No such file or directory", name)
}

// LoadChunkUncached reads filename fresh from LayeredFS and compiles it without
// consulting or updating CodeCache.
func (l *LayeredFS) LoadChunkUncached(filename string) (*moelua.Chunk, error) {
	data, resolvedName, err := l.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return compileChunkBytes("@"+resolvedName, data)
}

// SearchPath implements Lua 5.5 package.searchpath(name, path [, sep [, rep]])
// across OS disk and embedded VFS layers so loader.lua and coroutine-mode
// skynet.require resolve embedded and disk modules uniformly.
func (l *LayeredFS) SearchPath(name, pathTemplate, sep, rep string) (string, string) {
	if sep != "" {
		name = strings.ReplaceAll(name, sep, rep)
	}
	var tried strings.Builder
	for _, tmpl := range strings.Split(pathTemplate, ";") {
		if tmpl == "" {
			continue
		}
		candidate := strings.ReplaceAll(tmpl, "?", name)
		if l.Exists(candidate) {
			return candidate, ""
		}
		tried.WriteString("\n\tno file '")
		tried.WriteString(candidate)
		tried.WriteString("'")
	}
	return "", tried.String()
}

// CodeCacheMode controls Skynet-compatible bytecode sharing across actors.
type CodeCacheMode int

const (
	CodeCacheON CodeCacheMode = iota
	CodeCacheEXIST
	CodeCacheOFF
)

func (m CodeCacheMode) String() string {
	switch m {
	case CodeCacheON:
		return "ON"
	case CodeCacheEXIST:
		return "EXIST"
	case CodeCacheOFF:
		return "OFF"
	default:
		return "ON"
	}
}

type searchPathKey struct {
	name string
	tmpl string
	sep  string
	rep  string
}

type searchPathResult struct {
	filename string
	errMsg   string
}

// CodeCache compiles and caches immutable *moelua.Chunk bytecode prototypes
// shared across all actors in a Node. Debug info is NEVER stripped so
// debug.getupvalue(f, 1) == "_ENV" holds for skynet.service, snax.hotfix,
// and sharetable.update.
type CodeCache struct {
	vfs *LayeredFS

	mu       sync.RWMutex
	mode     CodeCacheMode
	chunks   map[string]*moelua.Chunk
	paths    map[searchPathKey]searchPathResult
	notFound map[string]struct{}
}

func NewCodeCache(vfs *LayeredFS) *CodeCache {
	return &CodeCache{
		vfs:      vfs,
		mode:     CodeCacheON,
		chunks:   make(map[string]*moelua.Chunk),
		paths:    make(map[searchPathKey]searchPathResult),
		notFound: make(map[string]struct{}),
	}
}

func (c *CodeCache) Mode(newMode string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if newMode == "" {
		return c.mode.String(), nil
	}
	switch strings.ToUpper(newMode) {
	case "ON":
		c.mode = CodeCacheON
	case "EXIST":
		c.mode = CodeCacheEXIST
	case "OFF":
		c.mode = CodeCacheOFF
	default:
		return "", fmt.Errorf("invalid codecache mode %q (expected ON, OFF, or EXIST)", newMode)
	}
	return c.mode.String(), nil
}

func (c *CodeCache) Clear() {
	c.mu.Lock()
	c.chunks = make(map[string]*moelua.Chunk)
	c.paths = make(map[searchPathKey]searchPathResult)
	c.notFound = make(map[string]struct{})
	c.mu.Unlock()
}

// SearchPath resolves Lua module paths via LayeredFS, caching lookups whenever
// CodeCache is not OFF so spawning thousands of actors avoids redundant os.Stat syscalls.
func (c *CodeCache) SearchPath(name, pathTemplate, sep, rep string) (string, string) {
	key := searchPathKey{name: name, tmpl: pathTemplate, sep: sep, rep: rep}
	c.mu.RLock()
	mode := c.mode
	if mode != CodeCacheOFF {
		if res, ok := c.paths[key]; ok {
			c.mu.RUnlock()
			return res.filename, res.errMsg
		}
	}
	c.mu.RUnlock()

	filename, errMsg := c.vfs.SearchPath(name, pathTemplate, sep, rep)
	if mode == CodeCacheON {
		c.mu.Lock()
		if c.mode == CodeCacheON {
			c.paths[key] = searchPathResult{filename: filename, errMsg: errMsg}
		}
		c.mu.Unlock()
	}
	return filename, errMsg
}

// LoadChunk returns a compiled immutable *moelua.Chunk for filename, respecting
// the current CodeCache mode and LayeredFS precedence.
func (c *CodeCache) LoadChunk(filename string) (*moelua.Chunk, error) {
	key := normalizeVFSPath(filename)
	if filepath.IsAbs(filename) {
		key = filename
	}

	c.mu.RLock()
	mode := c.mode
	var cached *moelua.Chunk
	var miss bool
	if mode != CodeCacheOFF {
		cached = c.chunks[key]
		if cached == nil && mode == CodeCacheON {
			_, miss = c.notFound[key]
		}
	}
	c.mu.RUnlock()

	if cached != nil {
		return cached, nil
	}
	if miss {
		return nil, fmt.Errorf("cannot open %s: No such file or directory", filename)
	}

	data, resolvedName, err := c.vfs.ReadFile(filename)
	if err != nil {
		if mode == CodeCacheON {
			c.mu.Lock()
			if c.mode == CodeCacheON {
				c.notFound[key] = struct{}{}
			}
			c.mu.Unlock()
		}
		return nil, err
	}
	ch, err := compileChunkBytes("@"+resolvedName, data)
	if err != nil {
		return nil, err
	}

	if mode == CodeCacheON {
		c.mu.Lock()
		if c.mode == CodeCacheON {
			if existing := c.chunks[key]; existing != nil {
				ch = existing
			} else {
				c.chunks[key] = ch
			}
			delete(c.notFound, key)
		}
		c.mu.Unlock()
	}
	return ch, nil
}

// stripBOMAndShebangBytes strips an optional UTF-8 BOM and leading '#' shebang
// line while preserving line numbers (matching Lua 5.5 luaL_loadfilex).
func stripBOMAndShebangBytes(data []byte) []byte {
	if bytes.HasPrefix(data, []byte{0xEF, 0xBB, 0xBF}) {
		data = data[3:]
	}
	if len(data) > 0 && data[0] == '#' {
		if idx := bytes.IndexByte(data, '\n'); idx >= 0 {
			if idx+1 < len(data) && data[idx+1] == 0x1b {
				return data[idx+1:]
			}
			return data[idx:]
		}
		return nil
	}
	return data
}

// compileChunkBytes handles UTF-8 BOM, leading shebang '#', and binary Lua chunks
// just like Lua 5.5 luaL_loadfilex, preserving full debug metadata.
func compileChunkBytes(chunkName string, data []byte) (*moelua.Chunk, error) {
	data = stripBOMAndShebangBytes(data)
	// Binary Lua 5.5 chunk.
	if len(data) >= 4 && data[0] == 0x1b && data[1] == 'L' && data[2] == 'u' && data[3] == 'a' {
		return moelua.LoadChunk(data)
	}
	ch, err := moelua.Compile(chunkName, string(data))
	if err != nil {
		return nil, err
	}
	if ch == nil {
		return nil, errors.New("empty chunk")
	}
	return ch, nil
}
