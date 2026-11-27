package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	stdruntime "runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuchanns/moelua"
	coreassets "github.com/yuchanns/spacenet/assets/core"
)

// Default worker scheduling weights mirroring skynet-src/skynet_start.c.
var workerWeights = []int{
	-1, -1, -1, -1, 0, 0, 0, 0,
	1, 1, 1, 1, 1, 1, 1, 1,
	2, 2, 2, 2, 2, 2, 2, 2,
	3, 3, 3, 3, 3, 3, 3, 3,
}

// Config configures a spacenet Node.
type Config struct {
	Threads         int
	MonitorInterval time.Duration
	Harbor          uint8
	JIT             bool
	BaseDir         string
	DisableOSFS     bool
	LogOutput       io.Writer
	LoggerFile      string
	Bootstrap       string
	Env             map[string]string
	FS              []fs.FS
	Contribs        []Contrib
}

// Actor represents an isolated Skynet service context scheduled on the Node's worker pool.
type Actor struct {
	node    *Node
	handle  uint32
	handler ServiceHandler
	queue   *Mailbox
	borrow  DispatchBorrow

	ref          atomic.Int32
	reserved     bool
	initDone     atomic.Bool
	retired      atomic.Bool
	endless      atomic.Bool
	profile      atomic.Bool
	sessionSeq   atomic.Int32
	messageCount atomic.Uint64
	cpuCostNS    atomic.Int64
	cpuStartNS   atomic.Int64

	logMu   sync.Mutex
	logfile *os.File

	killCtx    context.Context
	killCancel context.CancelFunc
	sig0Flag   atomic.Bool
}

func (a *Actor) Handle() uint32       { return a.handle }
func (a *Actor) Node() *Node          { return a.node }
func (a *Actor) Borrow() *DispatchBorrow { return &a.borrow }

func (a *Actor) grab() {
	a.ref.Add(1)
}

// Release decrements the actor's reference count and tears it down when reaching 0.
func (a *Actor) Release() {
	if a.ref.Add(-1) == 0 {
		a.destroy()
	}
}

func (a *Actor) destroy() {
	a.killCancel()
	a.logMu.Lock()
	if a.logfile != nil {
		_ = a.logfile.Close()
		a.logfile = nil
	}
	a.logMu.Unlock()

	if a.handler != nil {
		a.handler.Close(a)
	}
	a.queue.MarkRelease(a.node.globalQ)
	if !a.reserved {
		a.node.decActiveActors()
	}
}

func (a *Actor) NewSession() int32 {
	for {
		cur := a.sessionSeq.Load()
		next := cur + 1
		if next <= 0 {
			next = 1
		}
		if a.sessionSeq.CompareAndSwap(cur, next) {
			return next
		}
	}
}

func (a *Actor) writeServiceLog(msg *Message) {
	a.logMu.Lock()
	f := a.logfile
	if f != nil {
		_, _ = fmt.Fprintf(f, "%08x %d %d %d\n", msg.Source, msg.Type, msg.Session, len(msg.Data))
		if len(msg.Data) > 0 {
			_, _ = f.Write(msg.Data)
			_, _ = f.Write([]byte{'\n'})
		}
	}
	a.logMu.Unlock()
}

type workerMonitor struct {
	version      atomic.Int64
	checkVersion int64
	source       atomic.Uint32
	dest         atomic.Uint32
}

func (m *workerMonitor) trigger(source, dest uint32) {
	m.source.Store(source)
	m.dest.Store(dest)
	m.version.Add(1)
}

// Node is an isolated, self-contained spacenet runtime instance. Multiple Nodes
// may run concurrently inside the same Go process with zero shared global state.
type Node struct {
	cfg Config

	handles *HandleRegistry
	globalQ *GlobalQueue
	timer   *TimerManager
	msgs    *MsgRegistry
	objs    *ObjRegistry
	vfs     *LayeredFS
	cache   *CodeCache
	tmpl    *moelua.Template

	envMu sync.RWMutex
	env   map[string]string

	svcMu    sync.RWMutex
	services map[string]ServiceFactory

	extMu sync.RWMutex
	ext   map[string]any

	contribClosers []io.Closer

	logOutput   io.Writer
	loggerActor *Actor
	harborMu    sync.RWMutex
	localHarbor *localHarborService
	monitorExit atomic.Uint32
	profileDef  bool

	activeActors atomic.Int64
	zeroOnce     sync.Once
	zeroCh       chan struct{}

	monitors   []*workerMonitor
	stopMonCh  chan struct{}
	monDoneCh  chan struct{}
	workerWG   sync.WaitGroup
	closeOnce  sync.Once
	closed     atomic.Bool
}

// NewNode constructs and starts a spacenet Node (logger, timer, worker pool,
// monitor, and opt-in plugins) without launching bootstrap yet. Call Start or
// Launch to start services.
func NewNode(cfg Config) (*Node, error) {
	if cfg.Threads <= 0 {
		cfg.Threads = stdruntime.GOMAXPROCS(0)
		if cfg.Threads < 4 {
			cfg.Threads = 4
		}
	}
	if cfg.MonitorInterval <= 0 {
		cfg.MonitorInterval = 5 * time.Second
	}
	if cfg.LogOutput == nil {
		cfg.LogOutput = os.Stdout
	}
	if cfg.Bootstrap == "" {
		cfg.Bootstrap = "snlua bootstrap"
	}

	orderedContribs, err := resolveContribs(cfg.Contribs)
	if err != nil {
		return nil, err
	}

	// Build LayeredFS: user FS > contrib FS > core embedded FS.
	layers := make([]fs.FS, 0, len(cfg.FS)+len(orderedContribs)+1)
	layers = append(layers, cfg.FS...)
	for _, c := range orderedContribs {
		if cfs := c.FS(); cfs != nil {
			layers = append(layers, cfs)
		}
	}
	layers = append(layers, coreassets.FS())

	vfs := NewLayeredFS(cfg.BaseDir, cfg.DisableOSFS, layers...)
	cache := NewCodeCache(vfs)

	envMap := map[string]string{
		"thread":     strconv.Itoa(cfg.Threads),
		"harbor":     strconv.Itoa(int(cfg.Harbor)),
		"lualoader":  "./lualib/loader.lua",
		"luaservice": "./service/?.lua;./test/?.lua;./examples/?.lua",
		"lua_path":   "./lualib/?.lua;./lualib/?/init.lua",
		"lua_cpath":  "./luaclib/?.so",
		"snax":       "./examples/?.lua;./test/?.lua",
		"bootstrap":  cfg.Bootstrap,
	}
	for k, v := range cfg.Env {
		envMap[k] = v
	}
	profileDef := true
	if v, ok := envMap["profile"]; ok && (v == "false" || v == "0") {
		profileDef = false
	}

	n := &Node{
		cfg:        cfg,
		handles:    NewHandleRegistry(cfg.Harbor),
		globalQ:    NewGlobalQueue(),
		msgs:       NewMsgRegistry(),
		objs:       NewObjRegistry(),
		vfs:        vfs,
		cache:      cache,
		env:        envMap,
		services:   make(map[string]ServiceFactory),
		ext:        make(map[string]any),
		logOutput:  cfg.LogOutput,
		profileDef: profileDef,
		zeroCh:     make(chan struct{}),
		stopMonCh:  make(chan struct{}),
		monDoneCh:  make(chan struct{}),
	}

	n.services["logger"] = newLoggerService
	n.services["snlua"] = newSnluaService
	n.services["harbor"] = newLocalHarborService

	// Collect contrib services and preloads.
	preloads := make(map[string]moelua.GoFunc)
	for _, c := range orderedContribs {
		for name, factory := range c.Services() {
			n.services[name] = factory
		}
		for name, loader := range c.Preloads() {
			preloads[name] = loader
		}
	}

	// Build shared compacted MoeLua Template ONCE for fast actor cloning.
	tmpl, err := n.buildLuaTemplate(preloads)
	if err != nil {
		return nil, fmt.Errorf("spacenet: build lua template: %w", err)
	}
	n.tmpl = tmpl

	// Initialize TimerManager.
	n.timer = NewTimerManager(func(handle uint32, session int32) {
		n.pushMessage(handle, Message{
			Source:  0,
			Session: session,
			Type:    PTypeResponse,
		})
	})

	// Launch reserved built-in logger service first so all subsequent logs work.
	loggerHandle, err := n.launchInternal("logger", cfg.LoggerFile, true)
	if err != nil {
		n.timer.Close()
		return nil, fmt.Errorf("spacenet: launch logger: %w", err)
	}
	n.handles.NameHandle(loggerHandle, "logger")
	n.loggerActor = n.handles.Grab(loggerHandle)

	// Initialize opt-in contribs in dependency order.
	for _, c := range orderedContribs {
		closer, err := c.InitNode(n)
		if err != nil {
			n.Close()
			return nil, fmt.Errorf("spacenet: init contrib %q: %w", c.Name(), err)
		}
		if closer != nil {
			n.contribClosers = append(n.contribClosers, closer)
		}
	}

	// Start bounded worker pool and monitor goroutine.
	n.monitors = make([]*workerMonitor, cfg.Threads)
	for i := 0; i < cfg.Threads; i++ {
		mon := &workerMonitor{}
		n.monitors[i] = mon
		w := 0
		if i < len(workerWeights) {
			w = workerWeights[i]
		}
		n.workerWG.Add(1)
		go n.workerLoop(mon, w)
	}
	go n.monitorLoop()

	return n, nil
}

func resolveContribs(contribs []Contrib) ([]Contrib, error) {
	byName := make(map[string]Contrib)
	var collect func(list []Contrib)
	collect = func(list []Contrib) {
		for _, c := range list {
			if c == nil {
				continue
			}
			name := c.Name()
			if _, exists := byName[name]; exists {
				continue
			}
			byName[name] = c
			collect(c.Dependencies())
		}
	}
	collect(contribs)

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
	var sorted []Contrib
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

func (n *Node) Handles() *HandleRegistry { return n.handles }
func (n *Node) Timer() *TimerManager     { return n.timer }
func (n *Node) Msgs() *MsgRegistry       { return n.msgs }
func (n *Node) Objs() *ObjRegistry       { return n.objs }
func (n *Node) VFS() *LayeredFS          { return n.vfs }
func (n *Node) CodeCache() *CodeCache    { return n.cache }
func (n *Node) LogOutput() io.Writer     { return n.logOutput }
func (n *Node) ActiveActors() int64      { return n.activeActors.Load() }

// Ext retrieves per-Node plugin state by key.
func (n *Node) Ext(key string) any {
	n.extMu.RLock()
	v := n.ext[key]
	n.extMu.RUnlock()
	return v
}

// SetExt stores per-Node plugin state by key.
func (n *Node) SetExt(key string, val any) {
	n.extMu.Lock()
	n.ext[key] = val
	n.extMu.Unlock()
}

func (n *Node) GetEnv(key string) (string, bool) {
	n.envMu.RLock()
	v, ok := n.env[key]
	n.envMu.RUnlock()
	return v, ok
}

func (n *Node) SetEnv(key, val string) {
	n.envMu.Lock()
	n.env[key] = val
	n.envMu.Unlock()
}

func (n *Node) decActiveActors() {
	if n.activeActors.Add(-1) == 0 {
		n.zeroOnce.Do(func() {
			close(n.zeroCh)
		})
	}
}

// Start launches the configured bootstrap service (default "snlua bootstrap").
func (n *Node) Start() error {
	boot, _ := n.GetEnv("bootstrap")
	if boot == "" {
		boot = "snlua bootstrap"
	}
	fields := strings.Fields(boot)
	if len(fields) == 0 {
		return errors.New("spacenet: empty bootstrap command")
	}
	svc := fields[0]
	param := strings.TrimSpace(strings.TrimPrefix(boot, svc))
	h, err := n.Launch(svc, param)
	if err != nil {
		return err
	}
	if h == 0 {
		return fmt.Errorf("spacenet: failed to launch bootstrap %q", boot)
	}
	return nil
}

// Wait blocks until all non-reserved actors exit (or ctx is canceled).
func (n *Node) Wait(ctx context.Context) error {
	select {
	case <-n.zeroCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Abort retires all non-reserved actors, signaling the node to shut down.
func (n *Node) Abort() {
	actors := n.handles.RetireAll(false)
	for _, a := range actors {
		a.retired.Store(true)
		a.killCancel()
		a.Release()
	}
	if n.activeActors.Load() == 0 {
		n.zeroOnce.Do(func() {
			close(n.zeroCh)
		})
	}
}

// Close shuts down the Node, terminating all actors, plugins, workers, monitor, and timer.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.closed.Store(true)

		// Stop monitor loop first.
		close(n.stopMonCh)
		<-n.monDoneCh

		// Stop timer manager before tearing down workers.
		n.timer.Close()

		// Retire all non-reserved actors and cancel their contexts.
		n.Abort()

		// Close contribs in reverse dependency order so sockets/IO unblock.
		for i := len(n.contribClosers) - 1; i >= 0; i-- {
			_ = n.contribClosers[i].Close()
		}

		// Stop worker pool (rejects further pushes and wakes idle workers).
		n.globalQ.Close()
		n.workerWG.Wait()

		// Retire, drain, and release the reserved logger actor now that all workers stopped.
		if n.loggerActor != nil {
			if retired := n.handles.Retire(n.loggerActor.handle); retired != nil {
				retired.retired.Store(true)
				retired.Release()
			}
			for {
				msg, ok := n.loggerActor.queue.Pop()
				if !ok {
					break
				}
				_ = n.loggerActor.handler.Dispatch(n.loggerActor, &msg)
			}
			n.loggerActor.Release()
			n.loggerActor = nil
		}
	})
	return nil
}

// Launch creates and initializes a service by registered module name (e.g. "snlua").
func (n *Node) Launch(serviceName, param string) (uint32, error) {
	return n.launchInternal(serviceName, param, false)
}

func (n *Node) launchInternal(serviceName, param string, reserved bool) (uint32, error) {
	if n.closed.Load() {
		return 0, errors.New("spacenet: node is closed")
	}
	n.svcMu.RLock()
	factory, ok := n.services[serviceName]
	n.svcMu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("spacenet: unknown service %q", serviceName)
	}

	handler := factory()
	if handler == nil {
		return 0, fmt.Errorf("spacenet: service %q factory returned nil", serviceName)
	}

	killCtx, killCancel := context.WithCancel(context.Background())
	actor := &Actor{
		node:       n,
		handler:    handler,
		reserved:   reserved,
		killCtx:    killCtx,
		killCancel: killCancel,
	}
	actor.ref.Store(2) // 1 for HandleRegistry + 1 for this launch call
	actor.profile.Store(n.profileDef)

	handle := n.handles.Register(actor)
	actor.queue = NewMailbox(handle)
	if !reserved {
		n.activeActors.Add(1)
	}

	if err := handler.Init(actor, param); err != nil {
		n.Error(handle, "error: launch %s FAILED: %v", serviceName, err)
		actor.Release()
		if retired := n.handles.Retire(handle); retired != nil {
			retired.retired.Store(true)
			retired.Release()
		}
		actor.queue.DrainIfReleased(n.globalQ, func(m Message) {
			n.dropMessage(handle, m)
		})
		return 0, err
	}

	actor.initDone.Store(true)
	n.globalQ.Push(actor.queue)
	if !reserved {
		n.Error(handle, "LAUNCH %s %s", serviceName, param)
	}
	actor.Release()
	return handle, nil
}

// Kill retires an actor by handle and notifies monitor_exit if configured.
func (n *Node) Kill(caller *Actor, target uint32) {
	if target == 0 && caller != nil {
		target = caller.handle
		n.Error(caller.handle, "KILL self")
	} else if caller != nil {
		n.Error(caller.handle, "KILL :%0x", target)
	}
	if mon := n.monitorExit.Load(); mon != 0 {
		_ = n.Send(caller, target, mon, PTypeClient, 0, nil)
	}
	if retired := n.handles.Retire(target); retired != nil {
		retired.retired.Store(true)
		retired.Release()
	}
}

// Error sends a PTYPE_TEXT log message to the built-in logger service.
func (n *Node) Error(source uint32, format string, args ...any) {
	text := format
	if len(args) > 0 {
		text = fmt.Sprintf(format, args...)
	}
	logger := n.handles.FindName("logger")
	if logger == 0 {
		if n.logOutput != nil {
			_, _ = fmt.Fprintf(n.logOutput, "[:%08x] %s\n", source, text)
		}
		return
	}
	n.pushMessage(logger, Message{
		Source: source,
		Type:   PTypeText,
		Data:   []byte(text),
	})
}

func (n *Node) pushMessage(dest uint32, msg Message) bool {
	actor := n.handles.Grab(dest)
	if actor == nil {
		return false
	}
	actor.queue.Push(msg, n.globalQ)
	actor.Release()
	return true
}

// PushTypedMessage delivers a Message with a Go Payload (e.g. SocketEvent) directly
// to dest's mailbox. Returns false if dest does not exist.
func (n *Node) PushTypedMessage(dest uint32, msg Message) bool {
	return n.pushMessage(dest, msg)
}

func (n *Node) dropMessage(handle uint32, msg Message) {
	if msg.OwnedToken != 0 {
		n.msgs.Free(msg.OwnedToken)
	}
	if msg.Source != 0 && msg.Session != 0 {
		n.pushMessage(msg.Source, Message{
			Source:  handle,
			Session: msg.Session,
			Type:    PTypeError,
		})
	}
}

// Send delivers a message to destination handle, mirroring skynet_send.
// Returns session (>= 0), -1 on invalid destination, or -2 on oversized payload.
func (n *Node) Send(ctx *Actor, source, dest uint32, msgType int, session int32, data []byte) int32 {
	if len(data) > MaxMessageSize {
		src := source
		if src == 0 && ctx != nil {
			src = ctx.handle
		}
		n.Error(src, "error: The message to %x is too large", dest)
		return -2
	}
	allocSession := (msgType & PTypeTagAllocSession) != 0
	needCopy := (msgType & PTypeTagDontCopy) == 0
	ptype := uint8(msgType & PTypeMask)

	if allocSession && ctx != nil {
		session = ctx.NewSession()
	}
	if source == 0 && ctx != nil {
		source = ctx.handle
	}
	if dest == 0 {
		if len(data) > 0 {
			n.Error(source, "error: Destination address can't be 0")
			return -1
		}
		return session
	}
	var payload []byte
	if len(data) > 0 {
		if needCopy {
			payload = make([]byte, len(data))
			copy(payload, data)
		} else {
			payload = data
		}
	}
	if !n.pushMessage(dest, Message{
		Source:  source,
		Session: session,
		Type:    ptype,
		Data:    payload,
	}) {
		return -1
	}
	return session
}

func (n *Node) setHarbor(h *localHarborService) {
	n.harborMu.Lock()
	n.localHarbor = h
	n.harborMu.Unlock()
}

func (n *Node) clearHarbor(h *localHarborService) {
	n.harborMu.Lock()
	if n.localHarbor == h {
		n.localHarbor = nil
	}
	n.harborMu.Unlock()
}

// SendName resolves addr (":hex", ".localname", or standalone global name) and sends a message.
func (n *Node) SendName(ctx *Actor, source uint32, addr string, msgType int, session int32, data []byte) int32 {
	if len(addr) == 0 {
		return -1
	}
	if addr[0] == ':' || addr[0] == '.' {
		dest := n.handles.ParseHandle(addr)
		if dest == 0 {
			return -1
		}
		return n.Send(ctx, source, dest, msgType, session, data)
	}

	// Global name (standalone local harbor routing).
	if len(data) > MaxMessageSize {
		src := source
		if src == 0 && ctx != nil {
			src = ctx.handle
		}
		n.Error(src, "error: The message to %s is too large", addr)
		return -2
	}
	allocSession := (msgType & PTypeTagAllocSession) != 0
	needCopy := (msgType & PTypeTagDontCopy) == 0
	ptype := uint8(msgType & PTypeMask)
	if allocSession && ctx != nil {
		session = ctx.NewSession()
	}
	if source == 0 && ctx != nil {
		source = ctx.handle
	}
	var payload []byte
	if len(data) > 0 {
		if needCopy {
			payload = make([]byte, len(data))
			copy(payload, data)
		} else {
			payload = data
		}
	}
	n.harborMu.RLock()
	h := n.localHarbor
	n.harborMu.RUnlock()
	if h == nil {
		return -1
	}
	h.routeGlobal(n, addr, Message{
		Source:  source,
		Session: session,
		Type:    ptype,
		Data:    payload,
	})
	return session
}

// Command executes one of Skynet's 16 built-in skynet_command verbs.
func (n *Node) Command(actor *Actor, cmd, param string) (string, bool) {
	switch cmd {
	case "TIMEOUT":
		ti, err := strconv.Atoi(strings.TrimSpace(param))
		if err != nil {
			return "", false
		}
		session := actor.NewSession()
		n.timer.Timeout(actor.handle, session, ti)
		return strconv.Itoa(int(session)), true

	case "REG":
		if param == "" {
			return fmt.Sprintf(":%x", actor.handle), true
		}
		if strings.HasPrefix(param, ".") {
			if name := n.handles.NameHandle(actor.handle, param[1:]); name != "" {
				return name, true
			}
			return "", false
		}
		n.Error(actor.handle, "error: Can't register global name %s in Go", param)
		return "", false

	case "QUERY":
		if strings.HasPrefix(param, ".") {
			if h := n.handles.FindName(param[1:]); h != 0 {
				return fmt.Sprintf(":%x", h), true
			}
		}
		return "", false

	case "NAME":
		fields := strings.Fields(param)
		if len(fields) < 2 || !strings.HasPrefix(fields[1], ":") {
			return "", false
		}
		h := n.handles.ParseHandle(fields[1])
		if h == 0 {
			return "", false
		}
		if strings.HasPrefix(fields[0], ".") {
			if name := n.handles.NameHandle(h, fields[0][1:]); name != "" {
				return name, true
			}
		} else {
			n.Error(actor.handle, "error: Can't set global name %s in Go", fields[0])
		}
		return "", false

	case "EXIT":
		n.Kill(actor, 0)
		return "", false

	case "KILL":
		h := n.handles.ParseHandle(strings.TrimSpace(param))
		if h != 0 {
			n.Kill(actor, h)
		}
		return "", false

	case "LAUNCH":
		trimmed := strings.TrimSpace(param)
		if trimmed == "" {
			return "", false
		}
		var svc, args string
		if idx := strings.IndexAny(trimmed, " \t\r\n"); idx >= 0 {
			svc = trimmed[:idx]
			args = strings.TrimLeft(trimmed[idx+1:], " \t")
			if nl := strings.IndexAny(args, "\r\n"); nl >= 0 {
				args = args[:nl]
			}
		} else {
			svc = trimmed
		}
		h, err := n.Launch(svc, args)
		if err != nil || h == 0 {
			return "", false
		}
		return FormatHandle(h), true

	case "GETENV":
		v, ok := n.GetEnv(param)
		return v, ok

	case "SETENV":
		idx := strings.IndexByte(param, ' ')
		if idx <= 0 {
			return "", false
		}
		n.SetEnv(param[:idx], param[idx+1:])
		return "", false

	case "STARTTIME":
		return strconv.FormatUint(n.timer.StartTime(), 10), true

	case "ABORT":
		n.Abort()
		return "", false

	case "MONITOR":
		if param == "" {
			if mon := n.monitorExit.Load(); mon != 0 {
				return fmt.Sprintf(":%x", mon), true
			}
			return "", false
		}
		h := n.handles.ParseHandle(strings.TrimSpace(param))
		n.monitorExit.Store(h)
		return "", false

	case "STAT":
		switch param {
		case "mqlen":
			return strconv.Itoa(actor.queue.Len()), true
		case "endless":
			if actor.endless.Swap(false) {
				return "1", true
			}
			return "0", true
		case "cpu":
			return fmt.Sprintf("%f", float64(actor.cpuCostNS.Load())/1e9), true
		case "time":
			if actor.profile.Load() {
				start := actor.cpuStartNS.Load()
				if start > 0 {
					return fmt.Sprintf("%f", float64(time.Now().UnixNano()-start)/1e9), true
				}
			}
			return "0", true
		case "message":
			return strconv.FormatUint(actor.messageCount.Load(), 10), true
		default:
			return "", true
		}

	case "LOGON":
		h := n.handles.ParseHandle(strings.TrimSpace(param))
		if h == 0 {
			return "", false
		}
		target := n.handles.Grab(h)
		if target == nil {
			return "", false
		}
		defer target.Release()
		logpath, ok := n.GetEnv("logpath")
		if !ok || logpath == "" {
			return "", false
		}
		_ = os.MkdirAll(logpath, 0o755)
		fname := filepath.Join(logpath, fmt.Sprintf("%08x.log", h))
		target.logMu.Lock()
		if target.logfile == nil {
			if f, err := os.OpenFile(fname, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
				target.logfile = f
			}
		}
		target.logMu.Unlock()
		return "", false

	case "LOGOFF":
		h := n.handles.ParseHandle(strings.TrimSpace(param))
		if h == 0 {
			return "", false
		}
		target := n.handles.Grab(h)
		if target == nil {
			return "", false
		}
		defer target.Release()
		target.logMu.Lock()
		if target.logfile != nil {
			_ = target.logfile.Close()
			target.logfile = nil
		}
		target.logMu.Unlock()
		return "", false

	case "SIGNAL":
		fields := strings.Fields(param)
		if len(fields) == 0 {
			return "", false
		}
		h := n.handles.ParseHandle(fields[0])
		if h == 0 {
			return "", false
		}
		target := n.handles.Grab(h)
		if target == nil {
			return "", false
		}
		defer target.Release()
		sig := 0
		if len(fields) >= 2 {
			if v, err := strconv.ParseInt(fields[1], 0, 32); err == nil {
				sig = int(v)
			}
		}
		target.handler.Signal(target, sig)
		return "", false
	}
	return "", false
}

func (n *Node) workerLoop(mon *workerMonitor, weight int) {
	defer n.workerWG.Done()
	var q *Mailbox
	for {
		if q == nil {
			q = n.globalQ.PopWait()
			if q == nil {
				return
			}
		}
		q = n.dispatchQueue(mon, q, weight)
	}
}

func (n *Node) dispatchQueue(mon *workerMonitor, q *Mailbox, weight int) *Mailbox {
	handle := q.Handle()
	actor := n.handles.Grab(handle)
	if actor == nil {
		requeue := n.globalQ
		if n.closed.Load() {
			requeue = nil
		}
		q.DrainIfReleased(requeue, func(m Message) {
			n.dropMessage(handle, m)
		})
		return n.globalQ.Pop()
	}

	batch := 1
	for i := 0; i < batch; i++ {
		msg, ok := q.Pop()
		if !ok {
			actor.Release()
			return n.globalQ.Pop()
		}
		if i == 0 && weight >= 0 {
			rem := q.Len()
			batch = rem >> uint(weight)
			if batch < 1 {
				batch = 1
			}
		}
		if ov := q.ConsumeOverload(); ov > 0 {
			n.Error(handle, "error: May overload, message queue length = %d", ov)
		}

		mon.trigger(msg.Source, handle)

		actor.writeServiceLog(&msg)
		actor.messageCount.Add(1)
		if actor.profile.Load() {
			start := time.Now().UnixNano()
			actor.cpuStartNS.Store(start)
			_ = actor.handler.Dispatch(actor, &msg)
			cost := time.Now().UnixNano() - start
			actor.cpuCostNS.Add(cost)
			actor.cpuStartNS.Store(0)
		} else {
			_ = actor.handler.Dispatch(actor, &msg)
		}

		mon.trigger(0, 0)
	}

	nq := n.globalQ.Pop()
	if nq != nil {
		n.globalQ.Push(q)
		q = nq
	}
	actor.Release()
	return q
}

func (n *Node) monitorLoop() {
	defer close(n.monDoneCh)
	ticker := time.NewTicker(n.cfg.MonitorInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopMonCh:
			return
		case <-ticker.C:
			for _, mon := range n.monitors {
				v := mon.version.Load()
				if v == mon.checkVersion {
					if dest := mon.dest.Load(); dest != 0 {
						if actor := n.handles.Grab(dest); actor != nil {
							actor.endless.Store(true)
							actor.Release()
						}
						n.Error(0, "error: A message from [ :%08x ] to [ :%08x ] maybe in an endless loop (version = %d)", mon.source.Load(), dest, v)
					}
				} else {
					mon.checkVersion = v
				}
			}
		}
	}
}

// CallLua sends a PTYPE_LUA call from Go code to a Lua service (identified by
// handle or ".name") and waits for its response or ctx cancellation.
func (n *Node) CallLua(ctx context.Context, target string, args ...any) ([]any, error) {
	dest := n.handles.ParseHandle(target)
	if dest == 0 {
		return nil, fmt.Errorf("spacenet: unknown service address %q", target)
	}
	payload, err := PackGoValues(args...)
	if err != nil {
		return nil, err
	}

	type callResult struct {
		vals []any
		err  error
	}
	resCh := make(chan callResult, 1)

	caller := &ephemeralCaller{
		onMsg: func(m *Message) {
			if m.Type == PTypeError {
				resCh <- callResult{err: errors.New("spacenet: remote service returned PTYPE_ERROR")}
				return
			}
			vals, uerr := UnpackGoValues(m.Data)
			resCh <- callResult{vals: vals, err: uerr}
		},
	}

	killCtx, killCancel := context.WithCancel(context.Background())
	actor := &Actor{
		node:       n,
		handler:    caller,
		reserved:   true,
		killCtx:    killCtx,
		killCancel: killCancel,
	}
	actor.ref.Store(2)
	handle := n.handles.Register(actor)
	actor.queue = NewMailbox(handle)
	actor.initDone.Store(true)
	n.globalQ.Push(actor.queue)
	defer func() {
		if retired := n.handles.Retire(handle); retired != nil {
			retired.retired.Store(true)
			retired.Release()
		}
		actor.Release()
	}()

	sess := n.Send(actor, handle, dest, PTypeLua|PTypeTagAllocSession, 0, payload)
	if sess < 0 {
		return nil, fmt.Errorf("spacenet: failed to send to %q (code %d)", target, sess)
	}

	select {
	case r := <-resCh:
		return r.vals, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SendLua sends a one-way PTYPE_LUA message from Go code to a Lua service.
func (n *Node) SendLua(target string, args ...any) error {
	dest := n.handles.ParseHandle(target)
	if dest == 0 {
		return fmt.Errorf("spacenet: unknown service address %q", target)
	}
	payload, err := PackGoValues(args...)
	if err != nil {
		return err
	}
	if s := n.Send(nil, 0, dest, PTypeLua, 0, payload); s < 0 {
		return fmt.Errorf("spacenet: failed to send to %q (code %d)", target, s)
	}
	return nil
}

type ephemeralCaller struct {
	once  sync.Once
	onMsg func(*Message)
}

func (e *ephemeralCaller) Init(_ *Actor, _ string) error { return nil }
func (e *ephemeralCaller) Dispatch(_ *Actor, msg *Message) error {
	e.once.Do(func() {
		if e.onMsg != nil {
			e.onMsg(msg)
		}
	})
	return nil
}
func (e *ephemeralCaller) Signal(_ *Actor, _ int) {}
func (e *ephemeralCaller) Close(_ *Actor)         {}
