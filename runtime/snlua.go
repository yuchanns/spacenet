package runtime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/moelua/vm"
)

const (
	memWarningReportStart = 32 * 1024 * 1024 // 32 MiB, matching service_snlua.c
	ansiRed               = "\x1B[31m"
	ansiNormal            = "\x1B[0m"
)

// ActorFromState retrieves the *Actor bound to a MoeLua State via LUA_REGISTRYINDEX["skynet_context"].
func ActorFromState(s *moelua.State) (*Actor, error) {
	reg := s.Registry()
	if reg == nil {
		return nil, errors.New("Init skynet context first")
	}
	v := reg.GetString("skynet_context")
	if !v.IsUserdata() {
		return nil, errors.New("Init skynet context first")
	}
	raw := s.Raw()
	if raw == nil {
		return nil, errors.New("Init skynet context first")
	}
	a, ok := raw.UserdataData(raw.AsUserdata(v)).(*Actor)
	if !ok || a == nil {
		return nil, errors.New("Init skynet context first")
	}
	return a, nil
}

// snluaService implements Skynet's "snlua" service backed by a per-actor MoeLua VM
// cloned from Node's shared compacted *moelua.Template.
type snluaService struct {
	actor *Actor
	state *moelua.State

	launched bool
	cbFn     *moelua.Handle
	tbFn     *moelua.Handle
	forward  bool

	memReport int64

	// profile stores per-coroutine timing tables on this actor's Lua State.
	profStart  *moelua.Handle
	profTotal  *moelua.Handle
	profActive int
}

func newSnluaService() ServiceHandler {
	return &snluaService{
		memReport: memWarningReportStart,
	}
}

// buildLuaTemplate builds the single immutable MoeLua Template shared by all
// snlua actors in this Node.
func (n *Node) buildLuaTemplate(pluginPreloads map[string]moelua.GoFunc) (*moelua.Template, error) {
	opts := []moelua.Option{
		moelua.WithNoEnv(),
		moelua.WithLibs(moelua.LibAll),
		moelua.WithPreload("skynet.core", n.preloadSkynetCore),
		moelua.WithPreload("skynet.codecache", n.preloadCodeCache),
		moelua.WithPreload("skynet.profile", n.preloadProfile),
	}
	for name, loader := range pluginPreloads {
		opts = append(opts, moelua.WithPreload(name, loader))
	}

	return moelua.NewTemplate(func(s *moelua.State) error {
		// Override loadfile, dofile, package.searchpath, and package.searchers[2]
		// so loader.lua and coroutine-mode skynet.require seamlessly read from
		// LayeredFS (OS disk > user FS > plugin FS > core embedded FS) + CodeCache.
		s.RegisterFunc("loadfile", n.luaLoadFile)
		s.RegisterFunc("dofile", n.luaDoFile)

		pkg := s.TableOf(s.GetGlobal("package"))
		if pkg != nil {
			_ = pkg.SetString("searchpath", s.NewFunc("package.searchpath", n.luaSearchPath))
			searchers := s.TableOf(pkg.GetString("searchers"))
			if searchers != nil {
				// Replace Lua file searcher (index 2) with LayeredFS + CodeCache searcher.
				_ = searchers.SetInt(2, s.NewFunc("spacenet.lua_searcher", n.luaSearcher))
			}
		}
		return nil
	}, opts...)
}

func (n *Node) luaSearchPath(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("bad argument to 'searchpath' (string expected)")
	}
	name, ok := s.ToString(args[0])
	if !ok {
		return nil, errors.New("bad argument #1 to 'searchpath' (string expected)")
	}
	pathStr, ok := s.ToString(args[1])
	if !ok {
		return nil, errors.New("bad argument #2 to 'searchpath' (string expected)")
	}
	sep := "."
	if len(args) >= 3 && !args[2].IsNil() {
		sep, ok = s.ToString(args[2])
		if !ok {
			return nil, errors.New("bad argument #3 to 'searchpath' (string expected)")
		}
	}
	rep := "/"
	if len(args) >= 4 && !args[3].IsNil() {
		rep, ok = s.ToString(args[3])
		if !ok {
			return nil, errors.New("bad argument #4 to 'searchpath' (string expected)")
		}
	}
	found, errMsg := n.cache.SearchPath(name, pathStr, sep, rep)
	if found == "" {
		return []moelua.Value{moelua.Nil(), s.String(errMsg)}, nil
	}
	return []moelua.Value{s.String(found)}, nil
}

func (n *Node) luaLoadFile(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) == 0 || args[0].IsNil() {
		return []moelua.Value{moelua.Nil(), s.String("loadfile without filename is not supported")}, nil
	}
	filename, ok := s.ToString(args[0])
	if !ok {
		return nil, errors.New("bad argument #1 to 'loadfile' (string expected)")
	}
	mode := "bt"
	if len(args) >= 2 && !args[1].IsNil() {
		mode, ok = s.ToString(args[1])
		if !ok {
			return nil, errors.New("bad argument #2 to 'loadfile' (string expected)")
		}
	}
	hasEnv := len(args) >= 3
	var envVal moelua.Value
	if hasEnv {
		envVal = args[2]
	}

	var ch *moelua.Chunk
	var err error
	if mode == "bt" {
		ch, err = n.cache.LoadChunk(filename)
	} else {
		data, resolved, rerr := n.vfs.ReadFile(filename)
		if rerr != nil {
			err = rerr
		} else {
			isBin := len(data) >= 4 && data[0] == 0x1b && data[1] == 'L' && data[2] == 'u' && data[3] == 'a'
			if isBin && !strings.ContainsRune(mode, 'b') {
				err = fmt.Errorf("attempt to load a binary chunk (mode is '%s')", mode)
			} else if !isBin && !strings.ContainsRune(mode, 't') {
				err = fmt.Errorf("attempt to load a text chunk (mode is '%s')", mode)
			} else {
				ch, err = compileChunkBytes("@"+resolved, data)
			}
		}
	}
	if err != nil {
		return []moelua.Value{moelua.Nil(), s.String(err.Error())}, nil
	}
	fn, err := s.Load(ch)
	if err != nil {
		return []moelua.Value{moelua.Nil(), s.String(err.Error())}, nil
	}
	if hasEnv {
		// Pin fn across s.Call(debug.setupvalue, ...) so post-call GC steps
		// cannot sweep fn or its ArenaProto before luaLoadFile returns it.
		raw := s.Raw()
		fnRef := raw.Ref(fn)
		defer raw.Unref(fnRef)
		dbg := s.TableOf(s.GetGlobal("debug"))
		if dbg != nil {
			if setup := dbg.GetString("setupvalue"); !setup.IsNil() {
				_, _ = s.Call(setup, fn, moelua.Int(1), envVal)
			}
		}
	}
	return []moelua.Value{fn}, nil
}

func (n *Node) luaDoFile(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	res, err := n.luaLoadFile(s, args)
	if err != nil {
		return nil, err
	}
	if len(res) == 0 || res[0].IsNil() {
		msg := "dofile failed"
		if len(res) > 1 {
			if str, ok := s.ToString(res[1]); ok {
				msg = str
			}
		}
		return nil, errors.New(msg)
	}
	return s.Call(res[0])
}

func (n *Node) luaSearcher(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("bad argument #1 to searcher (string expected)")
	}
	modName, ok := s.ToString(args[0])
	if !ok {
		return nil, errors.New("bad argument #1 to searcher (string expected)")
	}
	pkg := s.TableOf(s.GetGlobal("package"))
	if pkg == nil {
		return []moelua.Value{s.String("\n\tno 'package' table")}, nil
	}
	pathStr, ok := s.ToString(pkg.GetString("path"))
	if !ok {
		return []moelua.Value{s.String("\n\t'package.path' must be a string")}, nil
	}
	filename, errMsg := n.cache.SearchPath(modName, pathStr, ".", "/")
	if filename == "" {
		return []moelua.Value{s.String(errMsg)}, nil
	}
	ch, err := n.cache.LoadChunk(filename)
	if err != nil {
		return nil, fmt.Errorf("error loading module '%s' from file '%s':\n\t%v", modName, filename, err)
	}
	fn, err := s.Load(ch)
	if err != nil {
		return nil, fmt.Errorf("error loading module '%s' from file '%s':\n\t%v", modName, filename, err)
	}
	return []moelua.Value{fn, s.String(filename)}, nil
}

func (l *snluaService) Init(actor *Actor, param string) error {
	l.actor = actor
	n := actor.node

	opts := []moelua.Option{
		moelua.WithNoEnv(),
		moelua.WithContext(actor.killCtx),
		moelua.WithErrorOutput(&actorWarnWriter{actor: actor}),
	}
	if n.cfg.JIT {
		opts = append(opts, moelua.WithJIT())
	}
	s := n.tmpl.NewState(opts...)
	l.state = s

	raw := s.Raw()
	raw.SetWarnings(true)
	raw.SetInterruptFlag(&actor.sig0Flag)
	raw.SetGCGenerational()

	// Bind *Actor and *snluaService in registry (never overwrite raw.Host!).
	reg := s.Registry()
	_ = reg.SetString("skynet_context", s.NewUserdata(actor))
	_ = reg.SetString("spacenet_snlua", s.NewUserdata(l))

	// Pin traceback helper once.
	tbFn := s.NewFunc("skynet.traceback", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		msg := "(no error message)"
		if len(args) > 0 && !args[0].IsNil() {
			if str, ok := st.ToString(args[0]); ok {
				msg = str
			} else if str, err := st.Raw().ToStringValue(args[0]); err == nil {
				msg = str
			}
		}
		return []moelua.Value{st.String(st.Traceback(nil, msg, 1))}, nil
	})
	l.tbFn = s.Pin(tbFn)

	// Open skynet.profile and replace coroutine.resume / coroutine.wrap (matching service_snlua.c:448-458).
	profMod, err := s.RequireLib("skynet.profile", n.preloadProfile, false)
	if err != nil {
		s.Close()
		return err
	}
	profTbl := s.TableOf(profMod)
	coroTbl := s.TableOf(s.GetGlobal("coroutine"))
	if profTbl != nil && coroTbl != nil {
		_ = coroTbl.SetString("resume", profTbl.GetString("resume"))
		_ = coroTbl.SetString("wrap", profTbl.GetString("wrap"))
	}

	// Register skynet.codecache in package.loaded.
	if _, err := s.RequireLib("skynet.codecache", n.preloadCodeCache, false); err != nil {
		s.Close()
		return err
	}

	// Queue the initial launch message to self (must be the first message, matching snlua_init).
	actor.queue.Push(Message{
		Source:  0,
		Session: 0,
		Type:    PTypeText,
		Data:    []byte(param),
	}, nil)
	return nil
}

func (l *snluaService) launch(args string) {
	s := l.state
	n := l.actor.node

	optEnv := func(key, def string) string {
		if v, ok := n.GetEnv(key); ok {
			return v
		}
		return def
	}

	s.SetGlobal("LUA_PATH", s.String(optEnv("lua_path", "./lualib/?.lua;./lualib/?/init.lua")))
	s.SetGlobal("LUA_CPATH", s.String(optEnv("lua_cpath", "./luaclib/?.so")))
	s.SetGlobal("LUA_SERVICE", s.String(optEnv("luaservice", "./service/?.lua")))
	if preload, ok := n.GetEnv("preload"); ok && preload != "" {
		s.SetGlobal("LUA_PRELOAD", s.String(preload))
	} else {
		s.SetGlobal("LUA_PRELOAD", moelua.Nil())
	}

	loaderPath := optEnv("lualoader", "./lualib/loader.lua")
	ch, err := n.cache.LoadChunk(loaderPath)
	if err != nil {
		n.Error(l.actor.handle, "Can't load %s : %v", loaderPath, err)
		l.reportLauncherError()
		n.Kill(l.actor, 0)
		return
	}
	fn, err := s.Load(ch)
	if err != nil {
		n.Error(l.actor.handle, "Can't load %s : %v", loaderPath, err)
		l.reportLauncherError()
		n.Kill(l.actor, 0)
		return
	}

	_, errVal, ok, uerr := s.Raw().CallProtected(fn, l.tbFn.Value(), s.String(args))
	if uerr != nil || !ok {
		errMsg := formatProtectedError(s, errVal, uerr)
		n.Error(l.actor.handle, "lua loader error : %s", errMsg)
		l.reportLauncherError()
		n.Kill(l.actor, 0)
		return
	}

	// Honor skynet.memlimit(bytes) set during top-level script evaluation (service_snlua.c:499-505).
	reg := s.Registry()
	if reg != nil {
		if mv := reg.GetString("memlimit"); mv.IsNumber() {
			limit := int64(0)
			if mv.IsInt() {
				limit = mv.Int()
			} else {
				limit = int64(mv.Float())
			}
			if limit > 0 {
				s.SetMemoryLimit(limit)
				n.Error(l.actor.handle, "Set memory limit to %.2f M", float64(limit)/(1024*1024))
			}
			_ = reg.SetString("memlimit", moelua.Nil())
		}
	}
	l.checkMemWarning()
}

func (l *snluaService) reportLauncherError() {
	l.actor.node.SendName(l.actor, 0, ".launcher", PTypeText, 0, []byte("ERROR"))
}

func (l *snluaService) Dispatch(actor *Actor, msg *Message) error {
	if !l.launched {
		l.launched = true
		l.launch(string(msg.Data))
		return nil
	}
	if l.cbFn == nil || l.state == nil {
		if msg.OwnedToken != 0 {
			actor.node.msgs.Free(msg.OwnedToken)
		}
		return nil
	}

	var tok uint64
	sz := len(msg.Data)
	usedBorrow := false

	if msg.OwnedToken != 0 {
		tok = msg.OwnedToken
		if !l.forward {
			defer actor.node.msgs.Free(tok)
		}
	} else if len(msg.Data) > 0 || msg.Payload != nil {
		if l.forward {
			if msg.Payload != nil {
				tok = actor.node.objs.Put(KindSocketEvt, msg.Payload)
			} else {
				tok = actor.node.msgs.Alloc(msg.Data)
			}
		} else {
			tok = actor.borrow.Bind(actor.handle, msg.Data, msg.Payload)
			usedBorrow = true
			defer actor.borrow.Clear()
		}
	}

	var msgVal moelua.Value
	if tok != 0 {
		msgVal = LightUDToken(tok)
	} else {
		msgVal = LightUDToken(0)
	}

	_, errVal, ok, uerr := l.state.Raw().CallProtected(
		l.cbFn.Value(),
		l.tbFn.Value(),
		moelua.Int(int64(msg.Type)),
		msgVal,
		moelua.Int(int64(sz)),
		moelua.Int(int64(msg.Session)),
		moelua.Int(int64(msg.Source)),
	)
	_ = usedBorrow

	if uerr != nil || !ok {
		if !errors.Is(uerr, context.Canceled) {
			errMsg := formatProtectedError(l.state, errVal, uerr)
			actor.node.Error(actor.handle, "lua call [%x to :%x : %d msgsz = %d] error : %s%s%s",
				msg.Source, actor.handle, msg.Session, sz, ansiRed, errMsg, ansiNormal)
		}
	}
	l.checkMemWarning()
	return nil
}

func (l *snluaService) checkMemWarning() {
	if l.state == nil {
		return
	}
	used := l.state.MemoryUsage()
	if used > l.memReport {
		for l.memReport < used {
			l.memReport *= 2
		}
		l.actor.node.Error(l.actor.handle, "Memory warning %.2f M", float64(used)/(1024*1024))
	}
}

func (l *snluaService) Signal(actor *Actor, sig int) {
	actor.node.Error(actor.handle, "recv a signal %d", sig)
	switch sig {
	case 0:
		actor.sig0Flag.Store(true)
	case 1:
		if l.state != nil {
			used := l.state.MemoryUsage()
			actor.node.Error(actor.handle, "Current Memory %.3fK", float64(used)/1024.0)
		}
	}
}

func (l *snluaService) Close(_ *Actor) {
	if l.cbFn != nil {
		l.cbFn.Release()
		l.cbFn = nil
	}
	if l.tbFn != nil {
		l.tbFn.Release()
		l.tbFn = nil
	}
	if l.profStart != nil {
		l.profStart.Release()
		l.profStart = nil
	}
	if l.profTotal != nil {
		l.profTotal.Release()
		l.profTotal = nil
	}
	if l.state != nil {
		// Clear the actor's canceled killCtx before Close so Lua/userdata __gc
		// finalizers (e.g. datasheet/sharedata/stm cleanup) run during teardown.
		l.state.SetContext(nil)
		l.state.Close()
		l.state = nil
	}
}

func formatProtectedError(s *moelua.State, errVal moelua.Value, uerr error) string {
	if uerr != nil {
		return uerr.Error()
	}
	if str, ok := s.ToString(errVal); ok {
		return str
	}
	return s.FormatValue(errVal)
}

func snluaFromState(s *moelua.State) (*snluaService, error) {
	reg := s.Registry()
	if reg == nil {
		return nil, errors.New("Init skynet context first")
	}
	v := reg.GetString("spacenet_snlua")
	if !v.IsUserdata() {
		return nil, errors.New("Init skynet context first")
	}
	raw := s.Raw()
	l, ok := raw.UserdataData(raw.AsUserdata(v)).(*snluaService)
	if !ok || l == nil {
		return nil, errors.New("Init skynet context first")
	}
	return l, nil
}

// ---------- skynet.codecache ----------

func (n *Node) preloadCodeCache(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	tbl := s.NewTable(0, 4)
	_ = tbl.SetString("clear", s.NewFunc("codecache.clear", func(_ *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		n.cache.Clear()
		return nil, nil
	}))
	_ = tbl.SetString("mode", s.NewFunc("codecache.mode", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		var mode string
		if len(args) > 0 && !args[0].IsNil() {
			var ok bool
			mode, ok = st.ToString(args[0])
			if !ok {
				return nil, errors.New("bad argument #1 to 'mode' (string expected)")
			}
		}
		res, err := n.cache.Mode(mode)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{st.String(res)}, nil
	}))
	_ = tbl.SetString("loadfile", s.NewFunc("codecache.loadfile", n.luaLoadFile))
	return []moelua.Value{tbl.Value()}, nil
}

// ---------- skynet.profile ----------

func (n *Node) preloadProfile(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	l, err := snluaFromState(s)
	if err != nil {
		return nil, err
	}

	makeWeakTable := func() *moelua.Table {
		t := s.NewTable(0, 8)
		mt := s.NewTable(0, 1)
		_ = mt.SetString("__mode", s.String("kv"))
		_ = t.SetMetatable(mt)
		return t
	}
	startTbl := makeWeakTable()
	totalTbl := makeWeakTable()
	l.profStart = s.Pin(startTbl.Value())
	l.profTotal = s.Pin(totalTbl.Value())

	coroTbl := s.TableOf(s.GetGlobal("coroutine"))
	origResume := coroTbl.GetString("resume")
	origCreate := coroTbl.GetString("create")
	origRunning := coroTbl.GetString("running")
	origClose := coroTbl.GetString("close")
	origStatus := coroTbl.GetString("status")

	profNow := func() float64 {
		return float64(n.timer.HPC()) / 1e9
	}

	getThreadArg := func(st *moelua.State, args []moelua.Value) (moelua.Value, error) {
		if len(args) > 0 && !args[0].IsNil() {
			if !args[0].IsThread() {
				return moelua.Nil(), fmt.Errorf("bad argument #1 (thread expected, got %s)", args[0].TypeName())
			}
			return args[0], nil
		}
		res, err := st.Call(origRunning)
		if err != nil || len(res) == 0 {
			return moelua.Nil(), err
		}
		return res[0], nil
	}

	recordProfilePause := func(raw *vm.State, coVal moelua.Value) {
		stTbl := raw.AsTable(l.profStart.Value())
		if sv := stTbl.RawGet(raw, coVal); sv.IsNumber() {
			diff := profNow() - sv.Float()
			if diff < 0 {
				diff = 0
			}
			totTbl := raw.AsTable(l.profTotal.Value())
			prev := 0.0
			if tv := totTbl.RawGet(raw, coVal); tv.IsNumber() {
				prev = tv.Float()
			}
			_ = totTbl.RawSet(raw, coVal, moelua.Float(prev+diff))
		}
	}

	timingResume := func(st *moelua.State, coVal moelua.Value, callArgs []moelua.Value) ([]moelua.Value, error) {
		raw := st.Raw()
		if l.profActive == 0 {
			resumeArgs := make([]moelua.Value, 1+len(callArgs))
			resumeArgs[0] = coVal
			copy(resumeArgs[1:], callArgs)
			res, err := raw.Call(origResume, resumeArgs...)
			if l.profActive > 0 {
				recordProfilePause(raw, coVal)
			}
			return res, err
		}
		stTbl := raw.AsTable(l.profStart.Value())
		if !stTbl.RawGet(raw, coVal).IsNil() {
			_ = stTbl.RawSet(raw, coVal, moelua.Float(profNow()))
		}

		resumeArgs := make([]moelua.Value, 1+len(callArgs))
		resumeArgs[0] = coVal
		copy(resumeArgs[1:], callArgs)
		res, err := raw.Call(origResume, resumeArgs...)

		if l.profActive > 0 {
			recordProfilePause(raw, coVal)
		}
		return res, err
	}

	lstart := func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		coVal, err := getThreadArg(st, args)
		if err != nil {
			return nil, err
		}
		raw := st.Raw()
		stTbl := raw.AsTable(l.profStart.Value())
		totTbl := raw.AsTable(l.profTotal.Value())
		if !stTbl.RawGet(raw, coVal).IsNil() {
			return nil, fmt.Errorf("Thread %s start profile more than once", st.FormatValue(coVal))
		}
		_ = totTbl.RawSet(raw, coVal, moelua.Float(0))
		_ = stTbl.RawSet(raw, coVal, moelua.Float(profNow()))
		l.profActive++
		return nil, nil
	}

	lstop := func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		coVal, err := getThreadArg(st, args)
		if err != nil {
			return nil, err
		}
		raw := st.Raw()
		stTbl := raw.AsTable(l.profStart.Value())
		totTbl := raw.AsTable(l.profTotal.Value())
		sv := stTbl.RawGet(raw, coVal)
		if sv.IsNil() {
			return nil, errors.New("Call profile.start() before profile.stop()")
		}
		diff := profNow() - sv.Float()
		if diff < 0 {
			diff = 0
		}
		total := 0.0
		if tv := totTbl.RawGet(raw, coVal); tv.IsNumber() {
			total = tv.Float()
		}
		_ = stTbl.RawSet(raw, coVal, moelua.Nil())
		_ = totTbl.RawSet(raw, coVal, moelua.Nil())
		if l.profActive > 0 {
			l.profActive--
		}
		return []moelua.Value{moelua.Float(total + diff)}, nil
	}

	lresume := func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) == 0 || !args[0].IsThread() {
			return nil, errors.New("bad argument #1 to 'resume' (thread expected)")
		}
		if l.profActive == 0 {
			raw := st.Raw()
			res, err := raw.Call(origResume, args...)
			if l.profActive > 0 {
				recordProfilePause(raw, args[0])
			}
			return res, err
		}
		return timingResume(st, args[0], args[1:])
	}

	lwrap := func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) == 0 || !args[0].IsFunction() {
			return nil, errors.New("bad argument #1 to 'wrap' (function expected)")
		}
		coRes, err := st.Call(origCreate, args[0])
		if err != nil || len(coRes) == 0 {
			return nil, err
		}
		coVal := coRes[0]
		coHandle := st.Pin(coVal)
		wrapper := st.NewFunc("coroutine.wrap", func(wst *moelua.State, wargs []moelua.Value) ([]moelua.Value, error) {
			res, err := timingResume(wst, coHandle.Value(), wargs)
			if err != nil {
				return nil, err
			}
			if len(res) == 0 {
				return nil, nil
			}
			if res[0].IsFalsy() {
				errObj := moelua.Nil()
				if len(res) > 1 {
					errObj = res[1]
				}
				raw := wst.Raw()
				errRef := raw.Ref(errObj)
				defer raw.Unref(errRef)
				// Close errored coroutine and propagate error (matching luaB_auxwrap).
				if stRes, serr := wst.Call(origStatus, coHandle.Value()); serr == nil && len(stRes) > 0 {
					if ss, ok := wst.ToString(stRes[0]); ok && ss == "dead" {
						_, _ = wst.Call(origClose, coHandle.Value())
					}
				}
				if str, ok := wst.ToString(errObj); ok {
					return nil, errors.New(str)
				}
				return nil, &vm.LuaError{Val: errObj}
			}
			return res[1:], nil
		})
		return []moelua.Value{wrapper}, nil
	}

	mod := s.NewTable(0, 4)
	_ = mod.SetString("start", s.NewFunc("profile.start", lstart))
	_ = mod.SetString("stop", s.NewFunc("profile.stop", lstop))
	_ = mod.SetString("resume", s.NewFunc("profile.resume", lresume))
	_ = mod.SetString("wrap", s.NewFunc("profile.wrap", lwrap))
	return []moelua.Value{mod.Value()}, nil
}

// ---------- skynet.core ----------

func (n *Node) preloadSkynetCore(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	actor, err := ActorFromState(s)
	if err != nil {
		return nil, err
	}
	l, err := snluaFromState(s)
	if err != nil {
		return nil, err
	}
	tbl := s.NewTable(0, 18)
	setFn := func(name string, fn moelua.GoFunc) {
		_ = tbl.SetString(name, s.NewFunc("skynet.core."+name, fn))
	}
	setFn("send", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return n.sendLuaMessage(st, actor, args, 0, 1)
	})
	setFn("genid", func(_ *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		return []moelua.Value{moelua.Int(int64(actor.NewSession()))}, nil
	})
	setFn("redirect", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 2 {
			return nil, errors.New("redirect requires source address")
		}
		src, ok := toIntArg(args[1])
		if !ok {
			return nil, errors.New("bad argument #2 to 'redirect' (number expected)")
		}
		return n.sendLuaMessage(st, actor, args, uint32(src), 2)
	})
	setFn("command", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return n.coreCommand(st, actor, args)
	})
	setFn("intcommand", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return n.coreIntCommand(st, actor, args)
	})
	setFn("addresscommand", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return n.coreAddressCommand(st, actor, args)
	})
	setFn("error", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return n.coreError(st, actor, args)
	})
	setFn("harbor", n.coreHarbor)
	setFn("callback", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return n.coreCallback(st, l, args)
	})
	setFn("trace", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return n.coreTrace(st, actor, args)
	})
	setFn("tostring", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return n.coreToString(st, actor, args)
	})
	setFn("pack", n.corePack)
	setFn("unpack", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return n.coreUnpack(st, actor, args)
	})
	setFn("unpackn", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return n.coreUnpackN(st, actor, args)
	})
	setFn("packstring", n.corePackString)
	setFn("trash", n.coreTrash)
	setFn("now", n.coreNow)
	setFn("hpc", n.coreHPC)
	return []moelua.Value{tbl.Value()}, nil
}

func (n *Node) coreCallback(s *moelua.State, l *snluaService, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) == 0 || !args[0].IsFunction() {
		return nil, errors.New("bad argument #1 to 'callback' (function expected)")
	}
	forward := len(args) >= 2 && !args[1].IsFalsy()
	if l.cbFn != nil {
		l.cbFn.Release()
	}
	l.cbFn = s.Pin(args[0])
	l.forward = forward
	return nil, nil
}

func (n *Node) coreCommand(s *moelua.State, actor *Actor, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("bad argument #1 to 'command' (string expected)")
	}
	cmd, ok := s.ToString(args[0])
	if !ok {
		return nil, errors.New("bad argument #1 to 'command' (string expected)")
	}
	var param string
	if len(args) >= 2 && !args[1].IsNil() {
		param, ok = s.ToString(args[1])
		if !ok {
			return nil, errors.New("bad argument #2 to 'command' (string expected)")
		}
	}
	res, has := n.Command(actor, cmd, param)
	if !has {
		return nil, nil
	}
	return []moelua.Value{s.String(res)}, nil
}

func (n *Node) coreAddressCommand(s *moelua.State, actor *Actor, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("bad argument #1 to 'addresscommand' (string expected)")
	}
	cmd, ok := s.ToString(args[0])
	if !ok {
		return nil, errors.New("bad argument #1 to 'addresscommand' (string expected)")
	}
	var param string
	if len(args) >= 2 && !args[1].IsNil() {
		param, ok = s.ToString(args[1])
		if !ok {
			return nil, errors.New("bad argument #2 to 'addresscommand' (string expected)")
		}
	}
	res, has := n.Command(actor, cmd, param)
	if !has || !strings.HasPrefix(res, ":") {
		return nil, nil
	}
	v, err := strconv.ParseUint(res[1:], 16, 32)
	if err != nil {
		return nil, nil
	}
	return []moelua.Value{moelua.Int(int64(v))}, nil
}

func (n *Node) coreIntCommand(s *moelua.State, actor *Actor, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("bad argument #1 to 'intcommand' (string expected)")
	}
	cmd, ok := s.ToString(args[0])
	if !ok {
		return nil, errors.New("bad argument #1 to 'intcommand' (string expected)")
	}
	var param string
	if len(args) >= 2 && !args[1].IsNil() {
		if args[1].IsNumber() {
			var iv int64
			if args[1].IsInt() {
				iv = args[1].Int()
			} else {
				iv = int64(args[1].Float())
			}
			param = strconv.FormatInt(int64(int32(iv)), 10)
		} else {
			param, ok = s.ToString(args[1])
			if !ok {
				return nil, errors.New("bad argument #2 to 'intcommand' (string or number expected)")
			}
		}
	}
	res, has := n.Command(actor, cmd, param)
	if !has {
		return nil, nil
	}
	if iv, err := strconv.ParseInt(res, 0, 64); err == nil {
		return []moelua.Value{moelua.Int(iv)}, nil
	}
	if fv, err := strconv.ParseFloat(res, 64); err == nil {
		return []moelua.Value{moelua.Float(fv)}, nil
	}
	return nil, fmt.Errorf("Invalid result %s", res)
}

func toIntArg(v moelua.Value) (int64, bool) {
	if v.IsInt() {
		return v.Int(), true
	}
	if v.IsFloat() {
		f := v.Float()
		if !math.IsNaN(f) && !math.IsInf(f, 0) && f == math.Trunc(f) {
			return int64(f), true
		}
	}
	return 0, false
}

func (n *Node) sendLuaMessage(s *moelua.State, actor *Actor, args []moelua.Value, source uint32, idxType int) ([]moelua.Value, error) {
	if len(args) < idxType+3 {
		return nil, errors.New("too few arguments to send")
	}

	var dest uint32
	var destStr string
	if args[0].IsNumber() {
		iv, _ := toIntArg(args[0])
		dest = uint32(iv)
		if dest == 0 {
			return nil, errors.New("Invalid service address 0")
		}
	} else {
		var ok bool
		destStr, ok = s.ToString(args[0])
		if !ok {
			return nil, fmt.Errorf("dest address type (%s) must be a string or number.", args[0].TypeName())
		}
	}

	typeInt, ok := toIntArg(args[idxType])
	if !ok {
		return nil, errors.New("invalid message type")
	}
	msgType := int(typeInt)
	var session int32
	if args[idxType+1].IsNil() {
		msgType |= PTypeTagAllocSession
	} else {
		sv, ok := toIntArg(args[idxType+1])
		if !ok {
			return nil, errors.New("invalid session")
		}
		session = int32(sv)
	}

	msgArg := args[idxType+2]
	var data []byte
	switch {
	case msgArg.IsString():
		str, _ := s.ToString(msgArg)
		if len(str) > 0 {
			data = []byte(str)
		}
	case msgArg.IsLightUD():
		if len(args) <= idxType+3 {
			return nil, errors.New("lightuserdata send requires size argument")
		}
		szVal, ok := toIntArg(args[idxType+3])
		if !ok || szVal < 0 {
			return nil, errors.New("invalid message size")
		}
		tok, _ := ExtractToken(msgArg)
		msgType |= PTypeTagDontCopy
		if tok != 0 {
			kind, _, _ := SplitToken(tok)
			switch kind {
			case KindMsgBuf:
				buf, ok := n.msgs.Take(tok)
				if ok {
					if int(szVal) <= len(buf) {
						data = buf[:szVal]
					} else {
						data = buf
					}
				}
			case KindDispatchSlot:
				if borrowed, _, ok := actor.borrow.Read(tok); ok {
					cp := make([]byte, min(int(szVal), len(borrowed)))
					copy(cp, borrowed)
					data = cp
				}
			}
		}
	default:
		return nil, fmt.Errorf("invalid param %s", msgArg.TypeName())
	}

	var ret int32
	if destStr != "" {
		ret = n.SendName(actor, source, destStr, msgType, session, data)
	} else {
		ret = n.Send(actor, source, dest, msgType, session, data)
	}
	if ret < 0 {
		if ret == -2 {
			return []moelua.Value{moelua.Bool(false)}, nil
		}
		return nil, nil
	}
	return []moelua.Value{moelua.Int(int64(ret))}, nil
}

func (n *Node) coreError(s *moelua.State, actor *Actor, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) == 0 {
		n.Error(actor.handle, "nil")
		return nil, nil
	}
	var sb strings.Builder
	for i, a := range args {
		if i > 0 {
			sb.WriteByte(' ')
		}
		str, err := s.Raw().ToStringValue(a)
		if err != nil {
			sb.WriteString(s.FormatValue(a))
		} else {
			sb.WriteString(str)
		}
	}
	n.Error(actor.handle, "%s", sb.String())
	return nil, nil
}

func (n *Node) coreHarbor(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("bad argument #1 to 'harbor' (number expected)")
	}
	h, ok := toIntArg(args[0])
	if !ok {
		return nil, errors.New("bad argument #1 to 'harbor' (number expected)")
	}
	hb, remote := n.handles.IsRemote(uint32(h))
	return []moelua.Value{moelua.Int(int64(hb)), moelua.Bool(remote)}, nil
}

func (n *Node) resolveMsgSlice(s *moelua.State, actor *Actor, v moelua.Value, sz int) ([]byte, error) {
	if v.IsString() {
		str, _ := s.ToString(v)
		return []byte(str), nil
	}
	if v.IsLightUD() {
		tok, _ := ExtractToken(v)
		if tok == 0 {
			return nil, nil
		}
		kind, _, _ := SplitToken(tok)
		switch kind {
		case KindDispatchSlot:
			if actor != nil {
				if data, _, ok := actor.borrow.Read(tok); ok {
					if sz >= 0 && sz <= len(data) {
						return data[:sz], nil
					}
					return data, nil
				}
			}
		case KindMsgBuf:
			if data, ok := n.msgs.Borrow(tok); ok {
				if sz >= 0 && sz <= len(data) {
					return data[:sz], nil
				}
				return data, nil
			}
		}
		return nil, fmt.Errorf("invalid or freed message buffer token 0x%x", tok)
	}
	return nil, fmt.Errorf("expected string or lightuserdata, got %s", v.TypeName())
}

func (n *Node) coreToString(s *moelua.State, actor *Actor, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) == 0 || args[0].IsNil() {
		return nil, nil
	}
	if len(args) < 2 {
		return nil, errors.New("bad argument #2 to 'tostring' (number expected)")
	}
	sz, ok := toIntArg(args[1])
	if !ok || sz < 0 {
		return nil, errors.New("bad argument #2 to 'tostring' (non-negative integer expected)")
	}
	data, err := n.resolveMsgSlice(s, actor, args[0], int(sz))
	if err != nil {
		return nil, err
	}
	if int(sz) <= len(data) {
		data = data[:sz]
	}
	strVal, err := s.TryString(string(data))
	if err != nil {
		return nil, err
	}
	return []moelua.Value{strVal}, nil
}

func (n *Node) corePack(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	buf, err := PackLuaValues(s, args)
	if err != nil {
		return nil, err
	}
	tok := n.msgs.Alloc(buf)
	return []moelua.Value{LightUDToken(tok), moelua.Int(int64(len(buf)))}, nil
}

func (n *Node) corePackString(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	buf, err := PackLuaValues(s, args)
	if err != nil {
		return nil, err
	}
	strVal, err := s.TryString(string(buf))
	if err != nil {
		return nil, err
	}
	return []moelua.Value{strVal}, nil
}

func (n *Node) coreUnpack(s *moelua.State, actor *Actor, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) == 0 || args[0].IsNil() {
		return nil, nil
	}
	sz := -1
	if args[0].IsLightUD() {
		if len(args) < 2 {
			return nil, errors.New("bad argument #2 to 'unpack' (number expected)")
		}
		v, ok := toIntArg(args[1])
		if !ok || v < 0 {
			return nil, errors.New("bad argument #2 to 'unpack' (non-negative integer expected)")
		}
		sz = int(v)
	}
	data, err := n.resolveMsgSlice(s, actor, args[0], sz)
	if err != nil {
		return nil, err
	}
	return UnpackLuaValues(s, data)
}

func (n *Node) coreUnpackN(s *moelua.State, actor *Actor, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) == 0 || args[0].IsNil() {
		return nil, errors.New("unpackn requires buffer argument")
	}
	var data []byte
	var offIdx int
	if args[0].IsString() {
		str, _ := s.ToString(args[0])
		data = []byte(str)
		offIdx = 1
	} else if args[0].IsLightUD() {
		if len(args) < 4 {
			return nil, errors.New("unpackn(msg, sz, offset, limit) requires 4 arguments")
		}
		sz, ok := toIntArg(args[1])
		if !ok || sz < 0 {
			return nil, errors.New("invalid size for unpackn")
		}
		var err error
		data, err = n.resolveMsgSlice(s, actor, args[0], int(sz))
		if err != nil {
			return nil, err
		}
		offIdx = 2
	} else {
		return nil, fmt.Errorf("bad argument #1 to 'unpackn' (string or lightuserdata expected, got %s)", args[0].TypeName())
	}
	if len(args) <= offIdx+1 {
		return nil, errors.New("unpackn requires offset and count")
	}
	offset, ok := toIntArg(args[offIdx])
	if !ok {
		return nil, errors.New("invalid offset for unpackn")
	}
	limit, ok := toIntArg(args[offIdx+1])
	if !ok {
		return nil, errors.New("invalid limit for unpackn")
	}
	nextOff, vals, err := UnpackLuaValuesN(s, data, int(offset), int(limit))
	if err != nil {
		return nil, err
	}
	out := make([]moelua.Value, 1+len(vals))
	out[0] = moelua.Int(int64(nextOff))
	copy(out[1:], vals)
	return out, nil
}

func (n *Node) coreTrash(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) == 0 {
		return nil, errors.New("skynet.trash invalid param no value")
	}
	switch {
	case args[0].IsString():
		return nil, nil
	case args[0].IsLightUD():
		if len(args) < 2 {
			return nil, errors.New("bad argument #2 to 'trash' (number expected)")
		}
		if _, ok := toIntArg(args[1]); !ok {
			return nil, errors.New("bad argument #2 to 'trash' (number expected)")
		}
		tok, _ := ExtractToken(args[0])
		if tok != 0 {
			n.msgs.Free(tok)
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("skynet.trash invalid param %s", args[0].TypeName())
	}
}

func (n *Node) coreNow(_ *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	return []moelua.Value{moelua.Int(int64(n.timer.Now()))}, nil
}

func (n *Node) coreHPC(_ *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	return []moelua.Value{moelua.Int(n.timer.HPC())}, nil
}

func (n *Node) coreTrace(s *moelua.State, actor *Actor, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("trace requires tag and userstring")
	}
	tag, ok := s.ToString(args[0])
	if !ok {
		return nil, errors.New("bad argument #1 to 'trace' (string expected)")
	}
	user, ok := s.ToString(args[1])
	if !ok {
		return nil, errors.New("bad argument #2 to 'trace' (string expected)")
	}
	hpc := n.timer.HPC()
	if len(args) >= 3 && !args[2].IsNil() {
		var th *moelua.Thread
		level := 1
		if args[2].IsThread() {
			th = s.ThreadOf(args[2])
			if len(args) >= 4 && !args[3].IsNil() {
				if lv, ok := toIntArg(args[3]); ok {
					level = int(lv)
				}
			}
		} else if lv, ok := toIntArg(args[2]); ok {
			level = int(lv)
		}
		tb := s.Traceback(th, "", level)
		var locs []string
		for _, line := range strings.Split(tb, "\n") {
			line = strings.TrimSpace(line)
			if line == "" || line == "stack traceback:" || strings.HasPrefix(line, "[C]:") {
				continue
			}
			// Extract file:line prefix.
			parts := strings.SplitN(line, ":", 3)
			if len(parts) >= 2 {
				if _, err := strconv.Atoi(parts[1]); err == nil {
					locs = append(locs, parts[0]+":"+parts[1])
					if len(locs) == 3 {
						break
					}
				}
			}
		}
		if len(locs) > 0 {
			n.Error(actor.handle, "<TRACE %s> %d %s : %s", tag, hpc, user, strings.Join(locs, " "))
			return nil, nil
		}
	}
	n.Error(actor.handle, "<TRACE %s> %d %s", tag, hpc, user)
	return nil, nil
}

// actorWarnWriter translates MoeLua's warn() / emitWarning() writes into
// Skynet's exact skynet_error warning format (matching service_snlua.c:415-437).
type actorWarnWriter struct {
	mu    sync.Mutex
	actor *Actor
	parts []string
}

func (w *actorWarnWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	s := string(p)
	// Fast path: single-call emitWarning("Lua warning: ...\n") from __gc errors.
	if len(w.parts) == 0 && strings.HasPrefix(s, "Lua warning: ") && strings.HasSuffix(s, "\n") {
		msg := strings.TrimSuffix(strings.TrimPrefix(s, "Lua warning: "), "\n")
		w.actor.node.Error(w.actor.handle, "Lua warning: %s", msg)
		return len(p), nil
	}
	if s == "Lua warning: " {
		w.parts = w.parts[:0]
		return len(p), nil
	}
	if s == "\n" {
		switch len(w.parts) {
		case 0:
			w.actor.node.Error(w.actor.handle, "Lua warning: ")
		case 1:
			w.actor.node.Error(w.actor.handle, "Lua warning: %s", w.parts[0])
		default:
			for i, part := range w.parts {
				switch i {
				case 0:
					w.actor.node.Error(w.actor.handle, "Lua warning< %s", part)
				case len(w.parts) - 1:
					w.actor.node.Error(w.actor.handle, "Lua warning> %s", part)
				default:
					w.actor.node.Error(w.actor.handle, "Lua warning+ %s", part)
				}
			}
		}
		w.parts = w.parts[:0]
		return len(p), nil
	}
	w.parts = append(w.parts, s)
	return len(p), nil
}

// unused import guard for time
var _ = time.Nanosecond
