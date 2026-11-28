// Package debug provides Skynet's debug_console, skynet.memory, skynet.debugchannel,
// and remote debugging/hot-injection Lua libraries and services.
package debug

import (
	"context"
	"errors"
	"io"
	"math"
	stdruntime "runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/contrib/http"
	"github.com/yuchanns/spacenet/runtime"
	"github.com/yuchanns/spacenet/runtime/contrib"
)

const (
	metaDebugChannel  = "debugchannel"
	regDebugHookTable = "_spacenet_debugchannel_hooks"
	extKeyDebugState  = "spacenet.debug.state"
)

type debugNodeState struct {
	profActive atomic.Bool
}

// New returns the debug contrib, declaring a dependency on contrib/http.
func New() *contrib.Contrib {
	return contrib.New("debug",
		contrib.WithDeps(http.New()),
		contrib.WithInit(initDebugNode),
		contrib.WithFS(subFS),
		contrib.WithPreloads(map[string]moelua.GoFunc{
			"spacenet.memory":       preloadMemory,
			"spacenet.debugchannel": preloadDebugChannel,
		}),
	)
}

func initDebugNode(node *runtime.Node) (io.Closer, error) {
	node.SetExt(extKeyDebugState, &debugNodeState{})
	return nil, nil
}

func nodeDebugState(node *runtime.Node) *debugNodeState {
	if st, ok := node.Ext(extKeyDebugState).(*debugNodeState); ok && st != nil {
		return st
	}
	return &debugNodeState{}
}

func preloadMemory(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, err
	}
	st := nodeDebugState(actor.Node())

	tbl := s.NewTable(0, 10)
	_ = tbl.SetString("total", s.NewFunc("memory.total", func(_ *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		var ms stdruntime.MemStats
		stdruntime.ReadMemStats(&ms)
		return []moelua.Value{moelua.Int(int64(ms.Alloc))}, nil
	}))
	_ = tbl.SetString("block", s.NewFunc("memory.block", func(_ *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		var ms stdruntime.MemStats
		stdruntime.ReadMemStats(&ms)
		return []moelua.Value{moelua.Int(int64(ms.Mallocs - ms.Frees))}, nil
	}))
	_ = tbl.SetString("current", s.NewFunc("memory.current", func(ls *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		return []moelua.Value{moelua.Int(ls.MemoryUsage())}, nil
	}))
	_ = tbl.SetString("jestat", s.NewFunc("memory.jestat", func(ls *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		var ms stdruntime.MemStats
		stdruntime.ReadMemStats(&ms)
		out := ls.NewTable(0, 5)
		_ = out.SetString("stats.allocated", moelua.Int(int64(ms.Alloc)))
		_ = out.SetString("stats.resident", moelua.Int(int64(ms.Sys)))
		_ = out.SetString("stats.retained", moelua.Int(int64(ms.HeapIdle)))
		_ = out.SetString("stats.mapped", moelua.Int(int64(ms.HeapSys)))
		_ = out.SetString("stats.active", moelua.Int(int64(ms.HeapInuse)))
		return []moelua.Value{out.Value()}, nil
	}))
	_ = tbl.SetString("info", s.NewFunc("memory.info", func(ls *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		node := actor.Node()
		if a, err := runtime.ActorFromState(ls); err == nil {
			a.SetMemoryUsage(ls.MemoryUsage())
			node = a.Node()
		}
		snapshot := node.MemoryInfo()
		out := ls.NewTable(0, len(snapshot))
		for handle, bytes := range snapshot {
			_ = out.SetInt(int64(handle), moelua.Int(bytes))
		}
		return []moelua.Value{out.Value()}, nil
	}))
	_ = tbl.SetString("mallctl", s.NewFunc("memory.mallctl", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 || !args[0].IsString() {
			return nil, errors.New("bad argument #1 to 'mallctl' (string expected)")
		}
		return []moelua.Value{moelua.Int(0)}, nil
	}))
	_ = tbl.SetString("dumpinfo", s.NewFunc("memory.dumpinfo", func(_ *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		return nil, nil
	}))
	_ = tbl.SetString("dump", s.NewFunc("memory.dump", func(_ *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		return nil, nil
	}))
	_ = tbl.SetString("dumpheap", s.NewFunc("memory.dumpheap", func(_ *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		return nil, nil
	}))
	_ = tbl.SetString("profactive", s.NewFunc("memory.profactive", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) > 0 {
			st.profActive.Store(!args[0].IsFalsy())
		}
		return []moelua.Value{moelua.Bool(st.profActive.Load())}, nil
	}))
	return []moelua.Value{tbl.Value()}, nil
}

type debugChan struct {
	mu     sync.Mutex
	ref    int
	tok    uint64
	node   *runtime.Node
	q      []string
	notify chan struct{}
	closed chan struct{}
}

type debugChanBox struct {
	ch *debugChan
}

func newDebugChan(node *runtime.Node) *debugChan {
	c := &debugChan{
		ref:    1,
		node:   node,
		notify: make(chan struct{}, 1),
		closed: make(chan struct{}),
	}
	c.mu.Lock()
	c.tok = node.Objs().Put(runtime.KindDebugChan, c)
	c.mu.Unlock()
	return c
}

func (c *debugChan) pop() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.q) == 0 {
		return "", false
	}
	msg := c.q[0]
	c.q[0] = ""
	c.q = c.q[1:]
	if len(c.q) == 0 {
		c.q = nil
	}
	return msg, true
}

func (c *debugChan) write(msg string) {
	c.mu.Lock()
	if c.ref <= 0 {
		c.mu.Unlock()
		return
	}
	c.q = append(c.q, msg)
	c.mu.Unlock()
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *debugChan) read(ctx context.Context, timeout float64) (string, bool) {
	if msg, ok := c.pop(); ok {
		return msg, true
	}
	if timeout <= 0 || math.IsNaN(timeout) {
		return "", false
	}

	// Skynet lua-debugchannel.c scales timeout by 100ms (usleep(timeout * 100000)).
	const maxTicks = float64(math.MaxInt64 / int64(100*time.Millisecond))
	var dur time.Duration
	if math.IsInf(timeout, 1) || timeout >= maxTicks {
		dur = time.Duration(math.MaxInt64)
	} else {
		dur = time.Duration(timeout * float64(100*time.Millisecond))
		if dur <= 0 {
			return "", false
		}
	}

	timer := time.NewTimer(dur)
	defer timer.Stop()

	for {
		select {
		case <-c.notify:
			if msg, ok := c.pop(); ok {
				return msg, true
			}
		case <-c.closed:
			return "", false
		case <-ctx.Done():
			return "", false
		case <-timer.C:
			return c.pop()
		}
	}
}

func (c *debugChan) release() {
	c.mu.Lock()
	if c.ref <= 0 {
		c.mu.Unlock()
		return
	}
	c.ref--
	dead := c.ref == 0
	tok := c.tok
	if dead {
		c.q = nil
		close(c.closed)
	}
	c.mu.Unlock()
	if dead && tok != 0 {
		c.node.Objs().Delete(runtime.KindDebugChan, tok)
	}
}

func newDebugChanBox(s *moelua.State, ch *debugChan) (moelua.Value, error) {
	mt, created := s.NewMetatable(metaDebugChannel)
	if created {
		idx := s.NewTable(0, 2)
		_ = idx.SetString("read", s.NewFunc("debugchannel.read", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
			if len(args) < 1 {
				return nil, errors.New("expected debugchannel")
			}
			v, ok := st.CheckUserdata(args[0], metaDebugChannel)
			if !ok {
				return nil, errors.New("expected debugchannel")
			}
			box := v.(*debugChanBox)
			if box.ch == nil {
				return nil, nil
			}
			timeout := 0.0
			if len(args) >= 2 && !args[1].IsNil() {
				if args[1].IsInt() {
					timeout = float64(args[1].Int())
				} else if args[1].IsFloat() {
					timeout = args[1].Float()
				} else {
					return nil, errors.New("bad argument #2 to 'read' (number expected)")
				}
			}
			ctx := context.Background()
			if a, err := runtime.ActorFromState(st); err == nil && a.Context() != nil {
				ctx = a.Context()
			}
			msg, ok := box.ch.read(ctx, timeout)
			if !ok {
				return nil, nil
			}
			return []moelua.Value{st.String(msg)}, nil
		}))
		_ = idx.SetString("write", s.NewFunc("debugchannel.write", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
			if len(args) < 1 {
				return nil, errors.New("expected debugchannel")
			}
			v, ok := st.CheckUserdata(args[0], metaDebugChannel)
			if !ok {
				return nil, errors.New("expected debugchannel")
			}
			box := v.(*debugChanBox)
			if len(args) < 2 || !args[1].IsString() {
				return nil, errors.New("bad argument #2 to 'write' (string expected)")
			}
			if box.ch == nil {
				return nil, nil
			}
			str, _ := st.ToString(args[1])
			box.ch.write(str)
			return nil, nil
		}))
		_ = mt.SetString("__index", idx.Value())
		_ = mt.SetString("__gc", s.NewFunc("debugchannel.__gc", lDebugRelease))
	}
	return s.NewUserdataWithMeta(&debugChanBox{ch: ch}, mt)
}

func lDebugRelease(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) == 0 {
		return nil, nil
	}
	if v, ok := s.CheckUserdata(args[0], metaDebugChannel); ok {
		box := v.(*debugChanBox)
		if box.ch != nil {
			box.ch.release()
			box.ch = nil
		}
	}
	return nil, nil
}

func preloadDebugChannel(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, err
	}
	node := actor.Node()

	tbl := s.NewTable(0, 4)
	_ = tbl.SetString("create", s.NewFunc("debugchannel.create", func(st *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		ch := newDebugChan(node)
		ud, err := newDebugChanBox(st, ch)
		if err != nil {
			ch.release()
			return nil, err
		}
		return []moelua.Value{ud, runtime.LightUDToken(ch.tok)}, nil
	}))
	_ = tbl.SetString("connect", s.NewFunc("debugchannel.connect", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("Invalid channel pointer")
		}
		tok, ok := runtime.ExtractToken(args[0])
		if !ok || tok == 0 {
			return nil, errors.New("Invalid channel pointer")
		}
		v, ok := node.Objs().Get(runtime.KindDebugChan, tok)
		if !ok {
			return nil, errors.New("new channel failed")
		}
		ch := v.(*debugChan)
		ch.mu.Lock()
		if ch.ref != 1 {
			ch.mu.Unlock()
			return nil, errors.New("new channel failed")
		}
		ch.ref++
		ch.mu.Unlock()
		ud, err := newDebugChanBox(st, ch)
		if err != nil {
			ch.release()
			return nil, err
		}
		return []moelua.Value{ud}, nil
	}))
	_ = tbl.SetString("release", s.NewFunc("debugchannel.release", lDebugRelease))
	_ = tbl.SetString("sethook", s.NewFunc("debugchannel.sethook", lDebugSetHook))
	return []moelua.Value{tbl.Value()}, nil
}

func getDebugHookTable(s *moelua.State) *moelua.Table {
	reg := s.Registry()
	if reg == nil {
		return nil
	}
	if v := reg.GetString(regDebugHookTable); v.IsTable() {
		return s.TableOf(v)
	}
	t := s.NewTable(0, 4)
	mt := s.NewTable(0, 1)
	_ = mt.SetString("__mode", s.String("k"))
	_ = t.SetMetatable(mt)
	_ = reg.SetString(regDebugHookTable, t.Value())
	return t
}

func currentCoroutine(s *moelua.State) (moelua.Value, error) {
	coro := s.TableOf(s.GetGlobal("coroutine"))
	if coro == nil {
		return moelua.Nil(), errors.New("coroutine library not loaded")
	}
	running := coro.GetString("running")
	if running.IsNil() {
		return moelua.Nil(), errors.New("coroutine.running not available")
	}
	res, err := s.Call(running)
	if err != nil {
		return moelua.Nil(), err
	}
	if len(res) == 0 {
		return moelua.Nil(), errors.New("coroutine.running returned no value")
	}
	return res[0], nil
}

// lDebugSetHook mirrors lualib-src/lua-debugchannel.c:db_sethook / hookf:
// registers a per-thread Lua hook callback in a weak-key registry table and
// invokes coroutine.yield() whenever the callback returns a truthy value.
// Because the native hook wrapper does not push a Lua call frame, debug.getinfo(2)
// inside the callback observes the debugged Lua frame directly. Note that
// moelua v0.1.0 increments nny inside fireHook, so yielding from a debug hook
// currently surfaces an explicit C-call boundary error rather than silently
// skipping breakpoints.
func lDebugSetHook(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	dbg := st.TableOf(st.GetGlobal("debug"))
	if dbg == nil {
		return nil, errors.New("debug library not loaded")
	}
	sh := dbg.GetString("sethook")
	if sh.IsNil() {
		return nil, errors.New("debug.sethook not available")
	}

	var targetCo moelua.Value
	argIdx := 0
	if len(args) > 0 && args[0].IsThread() {
		targetCo = args[0]
		argIdx = 1
	} else {
		var err error
		targetCo, err = currentCoroutine(st)
		if err != nil {
			return nil, err
		}
	}
	coPin := st.Pin(targetCo)
	defer coPin.Release()

	hookTbl := getDebugHookTable(st)
	if len(args) <= argIdx || args[argIdx].IsNil() {
		if hookTbl != nil {
			_ = hookTbl.Set(targetCo, moelua.Nil())
		}
		_, err := st.Call(sh, targetCo)
		return nil, err
	}

	userFn := args[argIdx]
	if !userFn.IsFunction() {
		return nil, errors.New("bad argument to 'sethook' (function expected)")
	}
	if len(args) <= argIdx+1 || !args[argIdx+1].IsString() {
		return nil, errors.New("bad argument to 'sethook' (mask string expected)")
	}
	mask := args[argIdx+1]
	var count moelua.Value
	hasCount := len(args) > argIdx+2 && !args[argIdx+2].IsNil()
	if hasCount {
		count = args[argIdx+2]
	}

	if hookTbl != nil {
		_ = hookTbl.Set(targetCo, userFn)
	}

	wrapper := st.NewFunc("debugchannel.hookf", func(hst *moelua.State, hargs []moelua.Value) ([]moelua.Value, error) {
		co, err := currentCoroutine(hst)
		if err != nil {
			return nil, err
		}
		ht := getDebugHookTable(hst)
		if ht == nil {
			return nil, nil
		}
		fn := ht.Get(co)
		if fn.IsNil() {
			return nil, nil
		}
		res, err := hst.Call(fn, hargs...)
		if err != nil {
			return nil, err
		}
		if len(res) > 0 && !res[0].IsFalsy() {
			coro := hst.TableOf(hst.GetGlobal("coroutine"))
			if coro != nil {
				if yieldFn := coro.GetString("yield"); !yieldFn.IsNil() {
					return hst.Call(yieldFn)
				}
			}
			return nil, errors.New("debugchannel.sethook: attempt to yield from debug hook")
		}
		return nil, nil
	})
	wrapPin := st.Pin(wrapper)
	defer wrapPin.Release()

	if hasCount {
		_, err := st.Call(sh, targetCo, wrapper, mask, count)
		return nil, err
	}
	_, err := st.Call(sh, targetCo, wrapper, mask)
	return nil, err
}
