package runtime

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strings"

	"github.com/yuchanns/moelua"
)

// LuaProfile configures a reusable Lua actor VM factory backed by its own
// compacted *moelua.Template, optional isolated VFS/CodeCache, library subset,
// preload modules, capability scope, hard memory cap, and lifecycle hooks.
//
// The zero value matches SpaceNet's built-in "snlua" profile: moelua.LibAll,
// the Node's LayeredFS/CodeCache and Config.Preloads, full spacenet.core /
// spacenet.codecache / spacenet.profile modules, no forced memory cap, and the
// standard loader.lua bootstrapper.
type LuaProfile struct {
	// Libs selects which Lua 5.5.1 standard libraries to open.
	// When HasLibs is false and Libs == 0, defaults to moelua.LibAll.
	// Set HasLibs = true to explicitly allow moelua.LibNone.
	Libs    moelua.LibMask
	HasLibs bool

	// Preloads registers Go module loaders into package.preload for actors using
	// this profile. Unless IsolatedPreloads is true, Node-level Config.Preloads
	// are inherited and entries here override matching names (a nil entry removes
	// an inherited Node preload).
	Preloads         map[string]moelua.GoFunc
	IsolatedPreloads bool

	// FS, DisableOSFS, and IsolatedFS configure VFS and CodeCache resolution.
	//
	// When IsolatedFS is true, the profile uses an independent LayeredFS mounted
	// exclusively over FS (OS filesystem access is always disabled, and neither
	// Node FS nor coreassets are visible) with an independent CodeCache. Callers
	// needing a jailed directory should pass os.DirFS(dir) inside FS.
	//
	// When IsolatedFS is false and both len(FS) == 0 and !DisableOSFS, the profile
	// shares the Node's LayeredFS and CodeCache directly. Otherwise FS is layered
	// ahead of the Node's VFS layers with an independent CodeCache.
	FS          []fs.FS
	DisableOSFS bool
	IsolatedFS  bool

	// Built-in SpaceNet module capability switches (false = enabled, matching
	// default snlua). Setting DisableCore omits "spacenet.core" from the VM
	// entirely so untrusted Lua code has no raw send/redirect/command primitives
	// without requiring hot-path runtime send filters.
	DisableCore      bool
	DisableCodeCache bool
	DisableProfile   bool

	// CoreCommands optionally restricts which spacenet.core command verbs (e.g.
	// "TIMEOUT", "QUERY", "EXIT", "STAT") actors in this profile may invoke when
	// Core is enabled. Nil allows all verbs (default); a non-nil slice (including
	// empty) acts as an exact uppercase allowlist and disallowed verbs raise a
	// Lua error without invoking Node.Command.
	CoreCommands []string

	// DisableRedirect omits "spacenet.core.redirect" (source-address spoofing)
	// when Core is enabled.
	DisableRedirect bool

	// MemoryLimit enforces a hard per-VM memory cap in bytes immediately after
	// cloning from the Template (before InitState and before any Lua script runs).
	// When > 0, Lua scripts calling skynet.memlimit may only tighten this cap,
	// never raise or remove it. Must be >= 0.
	MemoryLimit int64

	// JIT overrides Node Config.JIT when non-nil.
	JIT *bool

	// Loader overrides the default "lualoader" script path (default: Node env
	// "lualoader"). Ignored when LoaderFunc is set.
	Loader string

	// SetupTemplate runs once while building the shared compacted *moelua.Template
	// (after standard libraries, preloads, and VFS loaders are installed).
	// Consistent with moelua.NewTemplate, SetupTemplate must not leave open OS
	// files, unreleased Pin handles, or active debug hooks.
	SetupTemplate func(s *moelua.State) error

	// InitState runs per-actor right after cloning from the Template and applying
	// MemoryLimit / context / interrupt flag, and before the initial launch message.
	InitState func(actor *Actor, s *moelua.State) error

	// LoaderFunc, when non-nil, replaces loader.lua on the actor's initial launch
	// message (running on a worker goroutine). Returning a non-nil error logs the
	// failure, notifies .launcher, and kills the actor.
	LoaderFunc func(actor *Actor, s *moelua.State, param string) error

	// DispatchFunc, when non-nil, handles all post-launch mailbox messages on the
	// actor's worker goroutine instead of invoking a callback registered via
	// spacenet.core.callback. This lets capability-restricted (DisableCore) Lua
	// actors process SpaceNet messages via Go-managed dispatch while keeping raw
	// actor primitives invisible to untrusted Lua code.
	DispatchFunc func(actor *Actor, s *moelua.State, msg *Message) error

	// CloseFunc, when non-nil, runs during actor teardown right before s.Close()
	// (after killCtx is cleared on s so cleanup and finalizers can run). It is
	// only invoked if actor initialization completed without error.
	CloseFunc func(actor *Actor, s *moelua.State)
}

// Clone returns a shallow defensive copy of p with cloned slices, maps, and pointers.
func (p LuaProfile) Clone() LuaProfile {
	out := p
	if p.Preloads != nil {
		out.Preloads = maps.Clone(p.Preloads)
	}
	if p.FS != nil {
		out.FS = slices.Clone(p.FS)
	}
	if p.CoreCommands != nil {
		out.CoreCommands = slices.Clone(p.CoreCommands)
	}
	if p.JIT != nil {
		v := *p.JIT
		out.JIT = &v
	}
	return out
}

// LuaTemplate is a compiled, immutable Lua VM factory bound to a Node.
// Multiple actors and concurrent goroutines may share a single LuaTemplate to
// spawn isolated Lua VMs via MoeLua's zero-relocation template cloning.
type LuaTemplate struct {
	node         *Node
	tmpl         *moelua.Template
	vfs          *LayeredFS
	cache        *CodeCache
	libs         moelua.LibMask
	disableCore  bool
	disableCC    bool
	disableProf  bool
	disableRedir bool
	allowedCmds  map[string]struct{} // nil means unrestricted
	memLimit     int64
	jitOverride  *bool
	loaderPath   string
	initState    func(*Actor, *moelua.State) error
	loaderFunc   func(*Actor, *moelua.State, string) error
	dispatchFn   func(*Actor, *moelua.State, *Message) error
	closeFn      func(*Actor, *moelua.State)
}

// NewLuaTemplate compiles and compacts a reusable *LuaTemplate for profile on n.
// Safe to call concurrently on a running Node.
func (n *Node) NewLuaTemplate(profile LuaProfile) (*LuaTemplate, error) {
	if n == nil {
		return nil, errors.New("spacenet: nil node")
	}
	if profile.MemoryLimit < 0 {
		return nil, fmt.Errorf("spacenet: LuaProfile MemoryLimit must be >= 0 (got %d)", profile.MemoryLimit)
	}

	libs := moelua.LibAll
	if profile.HasLibs || profile.Libs != 0 {
		libs = profile.Libs
	}

	// Resolve VFS and CodeCache according to isolation settings.
	var vfs *LayeredFS
	var cache *CodeCache
	switch {
	case profile.IsolatedFS:
		// IsolatedFS always disables OS filesystem access (fail-closed).
		vfs = NewLayeredFS("", true, slices.Clone(profile.FS)...)
		cache = NewCodeCache(vfs)
	case len(profile.FS) > 0 || profile.DisableOSFS:
		layers := make([]fs.FS, 0, len(profile.FS)+len(n.vfs.layers))
		layers = append(layers, profile.FS...)
		layers = append(layers, n.vfs.layers...)
		disableOS := n.vfs.disableOSFS || profile.DisableOSFS
		vfs = NewLayeredFS(n.vfs.baseDir, disableOS, layers...)
		cache = NewCodeCache(vfs)
	default:
		vfs = n.vfs
		cache = n.cache
	}

	var allowedCmds map[string]struct{}
	if profile.CoreCommands != nil {
		allowedCmds = make(map[string]struct{}, len(profile.CoreCommands))
		for _, c := range profile.CoreCommands {
			verb := strings.ToUpper(strings.TrimSpace(c))
			if verb != "" {
				allowedCmds[verb] = struct{}{}
			}
		}
	}

	var jitOverride *bool
	if profile.JIT != nil {
		v := *profile.JIT
		jitOverride = &v
	}

	lt := &LuaTemplate{
		node:         n,
		vfs:          vfs,
		cache:        cache,
		libs:         libs,
		disableCore:  profile.DisableCore,
		disableCC:    profile.DisableCodeCache,
		disableProf:  profile.DisableProfile,
		disableRedir: profile.DisableRedirect,
		allowedCmds:  allowedCmds,
		memLimit:     profile.MemoryLimit,
		jitOverride:  jitOverride,
		loaderPath:   profile.Loader,
		initState:    profile.InitState,
		loaderFunc:   profile.LoaderFunc,
		dispatchFn:   profile.DispatchFunc,
		closeFn:      profile.CloseFunc,
	}

	// Assemble effective preload map deterministically.
	preloads := make(map[string]moelua.GoFunc)
	if !profile.IsolatedPreloads {
		for name, loader := range n.preloads {
			if name != "" && loader != nil {
				preloads[name] = loader
			}
		}
	}
	for name, loader := range profile.Preloads {
		if name == "" {
			continue
		}
		if loader == nil {
			delete(preloads, name)
		} else {
			preloads[name] = loader
		}
	}

	opts := []moelua.Option{
		moelua.WithNoEnv(),
		moelua.WithLibs(libs),
	}
	if !lt.disableCore {
		opts = append(opts, moelua.WithPreload("spacenet.core", lt.preloadCore))
	}
	if !lt.disableCC {
		opts = append(opts, moelua.WithPreload("spacenet.codecache", lt.preloadCodeCache))
	}
	if !lt.disableProf {
		opts = append(opts, moelua.WithPreload("spacenet.profile", n.preloadProfile))
	}
	for _, name := range slices.Sorted(maps.Keys(preloads)) {
		opts = append(opts, moelua.WithPreload(name, preloads[name]))
	}

	tmpl, err := moelua.NewTemplate(func(s *moelua.State) error {
		lt.installVFSLoaders(s, false)
		if profile.SetupTemplate != nil {
			return profile.SetupTemplate(s)
		}
		return nil
	}, opts...)
	if err != nil {
		return nil, fmt.Errorf("spacenet: build lua template: %w", err)
	}
	lt.tmpl = tmpl
	return lt, nil
}

// DefaultLuaTemplate returns the Node's built-in "snlua" *LuaTemplate.
func (n *Node) DefaultLuaTemplate() *LuaTemplate {
	if n == nil {
		return nil
	}
	return n.defaultLuaTmpl
}

// NewService constructs a fresh Lua ServiceHandler backed by lt.
// Its method value lt.NewService directly satisfies ServiceFactory.
func (lt *LuaTemplate) NewService() ServiceHandler {
	return &snluaService{
		tmpl:      lt,
		memReport: memWarningReportStart,
	}
}

// VFS returns the LayeredFS bound to this template.
func (lt *LuaTemplate) VFS() *LayeredFS { return lt.vfs }

// CodeCache returns the CodeCache bound to this template.
func (lt *LuaTemplate) CodeCache() *CodeCache { return lt.cache }

// InstallVFSLoaders installs this template's LayeredFS/CodeCache loaders onto s,
// respecting the template's LibMask (loadfile/dofile only with LibBaseFile;
// package.searchpath only with LibPackage; searcher added cleanly under LibPackageSafe).
func (lt *LuaTemplate) InstallVFSLoaders(s *moelua.State) {
	lt.installVFSLoaders(s, false)
}

// InstallFreshVFSLoaders installs uncached LayeredFS loaders onto s.
func (lt *LuaTemplate) InstallFreshVFSLoaders(s *moelua.State) {
	lt.installVFSLoaders(s, true)
}

func (lt *LuaTemplate) commandAllowed(cmd string) bool {
	if lt.allowedCmds == nil {
		return true
	}
	_, ok := lt.allowedCmds[cmd]
	return ok
}
