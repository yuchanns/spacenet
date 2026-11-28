package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/fstest"
	"time"

	"github.com/yuchanns/moelua"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitLogContains(t *testing.T, buf *lockedBuffer, substr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), substr) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for log %q\n--- logs ---\n%s", substr, buf.String())
}

func TestLuaProfileSandboxIsolationAndGoDispatch(t *testing.T) {
	nodeFS := fstest.MapFS{
		"secret_host.lua": &fstest.MapFile{Data: []byte(`return "HOST_SECRET"`)},
	}
	guestFS := fstest.MapFS{
		"helper.lua": &fstest.MapFile{Data: []byte(`
local M = {}
function M.double(n) return n * 2 end
return M
`)},
		"index.lua": &fstest.MapFile{Data: []byte(`
local helper = require "helper"
local cap = require "guest.cap"

local M = {}

function M.inspect()
	local core_ok = pcall(require, "spacenet.core")
	local cc_ok = pcall(require, "spacenet.codecache")
	local prof_ok = pcall(require, "spacenet.profile")
	local host_preload_ok = pcall(require, "host.secret")
	local host_fs_ok = pcall(require, "secret_host")
	return {
		io = tostring(io),
		os = tostring(os),
		debug = tostring(debug),
		loadfile = tostring(loadfile),
		dofile = tostring(dofile),
		loadlib = tostring(package and package.loadlib),
		searchpath = tostring(package and package.searchpath),
		core_ok = core_ok,
		cc_ok = cc_ok,
		prof_ok = prof_ok,
		host_preload_ok = host_preload_ok,
		host_fs_ok = host_fs_ok,
		cap_tag = cap.tag(),
		doubled = helper.double(21),
	}
end

function M.echo(x)
	return "echo:" .. tostring(x)
end

return M
`)},
	}

	n, err := NewNode(Config{
		Threads:     2,
		LogOutput:   io.Discard,
		DisableOSFS: true,
		FS:          []fs.FS{nodeFS},
		Preloads: map[string]moelua.GoFunc{
			"host.secret": func(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
				return []moelua.Value{s.String("TOP_SECRET")}, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	var closeCalled atomic.Bool
	var modHandles sync.Map // handle -> *moelua.Handle

	var guestTmpl *LuaTemplate
	guestTmpl, err = n.NewLuaTemplate(LuaProfile{
		Libs:             moelua.LibSafe | moelua.LibPackageSafe,
		IsolatedFS:       true,
		FS:               []fs.FS{guestFS},
		IsolatedPreloads: true,
		Preloads: map[string]moelua.GoFunc{
			"guest.cap": func(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
				tbl := s.NewTable(0, 1)
				_ = tbl.SetString("tag", s.NewFunc("cap.tag", func(st *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
					return []moelua.Value{st.String("sandbox-ok")}, nil
				}))
				return []moelua.Value{tbl.Value()}, nil
			},
		},
		DisableCore:      true,
		DisableCodeCache: true,
		DisableProfile:   true,
		MemoryLimit:      8 << 20,
		LoaderFunc: func(actor *Actor, s *moelua.State, param string) error {
			ch, err := guestTmpl.CodeCache().LoadChunk("index.lua")
			if err != nil {
				return err
			}
			fn, err := s.Load(ch)
			if err != nil {
				return err
			}
			res, err := s.Call(fn, s.String(param))
			if err != nil {
				return err
			}
			if len(res) == 0 || !res[0].IsTable() {
				return errors.New("index.lua must return module table")
			}
			modHandles.Store(actor.Handle(), s.Pin(res[0]))
			return nil
		},
		DispatchFunc: func(actor *Actor, s *moelua.State, msg *Message) error {
			if msg.Type != PTypeLua {
				return nil
			}
			vals, err := UnpackLuaValues(s, msg.Data)
			if err != nil {
				return err
			}
			if len(vals) == 0 {
				return errors.New("missing method name")
			}
			method, ok := s.ToString(vals[0])
			if !ok {
				return errors.New("method name must be string")
			}
			rawH, _ := modHandles.Load(actor.Handle())
			modTbl := s.TableOf(rawH.(*moelua.Handle).Value())
			fn := modTbl.GetString(method)
			if !fn.IsFunction() {
				if msg.Session != 0 {
					actor.Node().Send(actor, actor.Handle(), msg.Source, PTypeError, msg.Session, nil)
				}
				return fmt.Errorf("unknown method %q", method)
			}
			out, err := s.Call(fn, vals[1:]...)
			if err != nil {
				if msg.Session != 0 {
					actor.Node().Send(actor, actor.Handle(), msg.Source, PTypeError, msg.Session, nil)
				}
				return err
			}
			if msg.Session != 0 {
				packed, perr := PackLuaValues(s, out)
				if perr != nil {
					actor.Node().Send(actor, actor.Handle(), msg.Source, PTypeError, msg.Session, nil)
					return perr
				}
				actor.Node().Send(actor, actor.Handle(), msg.Source, PTypeResponse, msg.Session, packed)
			}
			return nil
		},
		CloseFunc: func(actor *Actor, _ *moelua.State) {
			if rawH, ok := modHandles.LoadAndDelete(actor.Handle()); ok {
				rawH.(*moelua.Handle).Release()
			}
			closeCalled.Store(true)
		},
	})
	if err != nil {
		t.Fatalf("NewLuaTemplate: %v", err)
	}

	// Launch dynamically without registering a global service name.
	h, err := n.LaunchWith("guest-plugin", guestTmpl.NewService(), "init-arg")
	if err != nil {
		t.Fatalf("LaunchWith: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := n.CallLua(ctx, FormatHandle(h), "inspect")
	if err != nil {
		t.Fatalf("CallLua inspect: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("expected 1 return value, got %#v", res)
	}
	info, ok := res[0].(map[any]any)
	if !ok {
		t.Fatalf("expected table result, got %T", res[0])
	}

	for _, nilKey := range []string{"io", "os", "debug", "loadfile", "dofile", "loadlib", "searchpath"} {
		if info[nilKey] != "nil" {
			t.Errorf("expected %s to be nil in sandbox, got %v", nilKey, info[nilKey])
		}
	}
	for _, falseKey := range []string{"core_ok", "cc_ok", "prof_ok", "host_preload_ok", "host_fs_ok"} {
		if info[falseKey] != false {
			t.Errorf("expected %s to be false in sandbox, got %v", falseKey, info[falseKey])
		}
	}
	if info["cap_tag"] != "sandbox-ok" {
		t.Errorf("cap_tag = %v, want sandbox-ok", info["cap_tag"])
	}
	if info["doubled"] != int64(42) {
		t.Errorf("doubled = %v, want 42", info["doubled"])
	}

	echoRes, err := n.CallLua(ctx, FormatHandle(h), "echo", "hello")
	if err != nil {
		t.Fatalf("CallLua echo: %v", err)
	}
	if len(echoRes) != 1 || echoRes[0] != "echo:hello" {
		t.Fatalf("unexpected echo response: %#v", echoRes)
	}

	n.Kill(nil, h)
	deadline := time.Now().Add(2 * time.Second)
	for !closeCalled.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !closeCalled.Load() {
		t.Fatal("expected CloseFunc to run on actor teardown")
	}
}

func TestLuaProfileCoreCommandsAndDisableRedirect(t *testing.T) {
	svcFS := fstest.MapFS{
		"service/restricted.lua": &fstest.MapFile{Data: []byte(`
local spacenet = require "spacenet"
local c = require "spacenet.core"

spacenet.start(function()
	spacenet.dispatch("lua", function(session, source, cmd)
		if cmd == "check" then
			local launch_ok, launch_err = pcall(c.command, "LAUNCH", "snlua restricted")
			local kill_ok, kill_err = pcall(c.command, "KILL", ":00000001")
			local setenv_ok, setenv_err = pcall(c.command, "SETENV", "evil 1")
			local abort_ok, abort_err = pcall(c.command, "ABORT")
			local reg_ok, reg_err = pcall(c.command, "REG", ".hijack")
			local stat = c.command("STAT", "mqlen")
			spacenet.sleep(1) -- exercises TIMEOUT
			spacenet.retpack({
				redirect_nil = (c.redirect == nil),
				launch_ok = launch_ok,
				launch_err = tostring(launch_err),
				kill_ok = kill_ok,
				kill_err = tostring(kill_err),
				setenv_ok = setenv_ok,
				setenv_err = tostring(setenv_err),
				abort_ok = abort_ok,
				abort_err = tostring(abort_err),
				reg_ok = reg_ok,
				reg_err = tostring(reg_err),
				stat_ok = (stat ~= nil),
			})
		end
	end)
	spacenet.error("RESTRICTED_READY")
end)
`)},
	}

	logs := &lockedBuffer{}
	n, err := NewNode(Config{
		Threads:     2,
		LogOutput:   logs,
		DisableOSFS: true,
		FS:          []fs.FS{svcFS},
		LuaProfiles: map[string]LuaProfile{
			"restricted": {
				CoreCommands:    []string{"TIMEOUT", "QUERY", "EXIT", "STAT", "GETENV"},
				DisableRedirect: true,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	h, err := n.Launch("restricted", "restricted")
	if err != nil {
		t.Fatalf("Launch restricted: %v", err)
	}
	waitLogContains(t, logs, "RESTRICTED_READY", 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := n.CallLua(ctx, FormatHandle(h), "check")
	if err != nil {
		t.Fatalf("CallLua check: %v\n--- logs ---\n%s", err, logs.String())
	}
	info := res[0].(map[any]any)
	if info["redirect_nil"] != true {
		t.Errorf("expected spacenet.core.redirect to be nil, got %v", info["redirect_nil"])
	}
	if info["stat_ok"] != true {
		t.Errorf("expected allowed STAT command to succeed")
	}
	for _, tc := range []struct {
		okKey  string
		errKey string
	}{
		{"launch_ok", "launch_err"},
		{"kill_ok", "kill_err"},
		{"setenv_ok", "setenv_err"},
		{"abort_ok", "abort_err"},
		{"reg_ok", "reg_err"},
	} {
		if info[tc.okKey] != false {
			t.Errorf("expected %s=false, got %v", tc.okKey, info[tc.okKey])
		}
		if !strings.Contains(fmt.Sprint(info[tc.errKey]), "not permitted") {
			t.Errorf("expected %s to contain 'not permitted', got %v", tc.errKey, info[tc.errKey])
		}
	}
	if _, exists := n.GetEnv("evil"); exists {
		t.Fatal("disallowed SETENV mutated node env")
	}
}

func TestLuaProfileMemoryLimitCannotBeRaisedByLua(t *testing.T) {
	svcFS := fstest.MapFS{
		"service/memcap.lua": &fstest.MapFile{Data: []byte(`
local spacenet = require "spacenet"
-- Attempt to raise memory limit to 100 MiB above host's 2 MiB cap.
spacenet.memlimit(100 * 1024 * 1024)

spacenet.start(function()
	spacenet.dispatch("lua", function()
		local ok, err = pcall(function()
			local t = {}
			for i = 1, 20000 do
				t[i] = string.rep("a", 1024)
			end
		end)
		spacenet.retpack(ok, tostring(err))
	end)
	spacenet.error("MEMCAP_READY")
end)
`)},
	}

	logs := &lockedBuffer{}
	n, err := NewNode(Config{
		Threads:     2,
		LogOutput:   logs,
		DisableOSFS: true,
		FS:          []fs.FS{svcFS},
		LuaProfiles: map[string]LuaProfile{
			"capped": {
				MemoryLimit: 2 * 1024 * 1024,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	h, err := n.Launch("capped", "memcap")
	if err != nil {
		t.Fatalf("Launch capped: %v", err)
	}
	waitLogContains(t, logs, "MEMCAP_READY", 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := n.CallLua(ctx, FormatHandle(h), "alloc")
	if err != nil {
		t.Fatalf("CallLua alloc: %v\n--- logs ---\n%s", err, logs.String())
	}
	if len(res) < 2 || res[0] != false {
		t.Fatalf("expected OOM failure under host MemoryLimit, got %#v", res)
	}
	if !strings.Contains(fmt.Sprint(res[1]), "not enough memory") {
		t.Fatalf("expected 'not enough memory', got %v", res[1])
	}
}

func TestLuaProfileLoadfileWithEnvWithoutDebugLib(t *testing.T) {
	profFS := fstest.MapFS{
		"mod.lua": &fstest.MapFile{Data: []byte(`return greeting .. "!"`)},
	}

	n, err := NewNode(Config{
		Threads:     2,
		LogOutput:   io.Discard,
		DisableOSFS: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	var got string
	done := make(chan struct{})
	lt, err := n.NewLuaTemplate(LuaProfile{
		Libs:             moelua.LibSafe | moelua.LibBaseFile | moelua.LibPackageSafe,
		IsolatedFS:       true,
		FS:               []fs.FS{profFS},
		DisableCore:      true,
		DisableCodeCache: true,
		DisableProfile:   true,
		LoaderFunc: func(actor *Actor, s *moelua.State, _ string) error {
			res, err := s.DoString(`
				assert(debug == nil)
				local fn = assert(loadfile("mod.lua", "t", { greeting = "hi" }))
				return fn()
			`)
			if err != nil {
				return err
			}
			got, _ = s.ToString(res[0])
			close(done)
			actor.Node().Kill(actor, 0)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := n.LaunchWith("no-debug-env", lt.NewService(), ""); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for loadfile with env")
	}
	if got != "hi!" {
		t.Fatalf("got %q, want %q", got, "hi!")
	}
}

func TestLuaProfileIsolatedCodeCacheDoesNotClearNodeCache(t *testing.T) {
	nodeFS := fstest.MapFS{
		"node_mod.lua": &fstest.MapFile{Data: []byte(`return 1`)},
	}
	isoFS := fstest.MapFS{
		"iso_mod.lua": &fstest.MapFile{Data: []byte(`return 2`)},
	}

	n, err := NewNode(Config{
		Threads:     2,
		LogOutput:   io.Discard,
		DisableOSFS: true,
		FS:          []fs.FS{nodeFS},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	if _, err := n.CodeCache().LoadChunk("node_mod.lua"); err != nil {
		t.Fatal(err)
	}
	n.CodeCache().mu.RLock()
	beforeCount := len(n.CodeCache().chunks)
	n.CodeCache().mu.RUnlock()
	if beforeCount == 0 {
		t.Fatal("expected node CodeCache to hold node_mod.lua")
	}

	done := make(chan error, 1)
	lt, err := n.NewLuaTemplate(LuaProfile{
		Libs:       moelua.LibSafe | moelua.LibPackageSafe,
		IsolatedFS: true,
		FS:         []fs.FS{isoFS},
		DisableCore: true,
		DisableProfile: true,
		LoaderFunc: func(actor *Actor, s *moelua.State, _ string) error {
			_, err := s.DoString(`
				local cc = require "spacenet.codecache"
				assert(cc.loadfile == nil, "codecache.loadfile must be absent without LibBaseFile")
				assert(require("iso_mod") == 2)
				cc.clear()
			`)
			done <- err
			actor.Node().Kill(actor, 0)
			return err
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.LaunchWith("iso-cc", lt.NewService(), ""); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for isolated codecache test")
	}

	n.CodeCache().mu.RLock()
	afterCount := len(n.CodeCache().chunks)
	n.CodeCache().mu.RUnlock()
	if afterCount != beforeCount {
		t.Fatalf("isolated codecache.clear() mutated Node CodeCache: before=%d after=%d", beforeCount, afterCount)
	}
}

func TestLuaProfileInitStateFailureSkipsCloseFuncAndValidation(t *testing.T) {
	n, err := NewNode(Config{
		Threads:     2,
		LogOutput:   io.Discard,
		DisableOSFS: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := n.NewLuaTemplate(LuaProfile{MemoryLimit: -1}); err == nil {
		t.Fatal("expected error for negative MemoryLimit")
	}
	if err := n.RegisterService("", func() ServiceHandler { return &dummyService{} }); err == nil {
		t.Fatal("expected error for empty service name")
	}
	if err := n.RegisterService("x", nil); err == nil {
		t.Fatal("expected error for nil factory")
	}
	if _, err := n.LaunchWith("x", nil, ""); err == nil {
		t.Fatal("expected error for nil handler")
	}

	var closeCalled atomic.Bool
	wantErr := errors.New("init boom")
	lt, err := n.NewLuaTemplate(LuaProfile{
		DisableCore:      true,
		DisableCodeCache: true,
		DisableProfile:   true,
		InitState: func(_ *Actor, _ *moelua.State) error {
			return wantErr
		},
		CloseFunc: func(_ *Actor, _ *moelua.State) {
			closeCalled.Store(true)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.LaunchWith("fail-init", lt.NewService(), ""); !errors.Is(err, wantErr) {
		t.Fatalf("LaunchWith err = %v, want %v", err, wantErr)
	}
	if closeCalled.Load() {
		t.Fatal("CloseFunc must not run when InitState fails")
	}

	// Verify RegisterService / UnregisterService lifecycle and concurrent template cloning.
	var launched atomic.Int32
	goodTmpl, err := n.NewLuaTemplate(LuaProfile{
		Libs:             moelua.LibSafe | moelua.LibPackageSafe,
		IsolatedFS:       true,
		DisableCore:      true,
		DisableCodeCache: true,
		DisableProfile:   true,
		LoaderFunc: func(actor *Actor, s *moelua.State, param string) error {
			res, err := s.DoString(`return 1 + 1`)
			if err != nil {
				return err
			}
			if res[0].Int() != 2 {
				return errors.New("math failed")
			}
			launched.Add(1)
			actor.Node().Kill(actor, 0)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.RegisterService("dyn_worker", goodTmpl.NewService); err != nil {
		t.Fatal(err)
	}

	const workers = 16
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			if _, err := n.Launch("dyn_worker", "ok"); err != nil {
				t.Errorf("concurrent Launch failed: %v", err)
			}
		})
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.Wait(ctx); err != nil {
		t.Fatalf("Wait after concurrent launches: %v", err)
	}
	if got := launched.Load(); got != workers {
		t.Fatalf("launched = %d, want %d", got, workers)
	}

	n.UnregisterService("dyn_worker")
	if _, err := n.Launch("dyn_worker", ""); err == nil {
		t.Fatal("expected Launch after UnregisterService to fail")
	}

	_ = n.Close()
	if err := n.RegisterService("after_close", goodTmpl.NewService); err == nil {
		t.Fatal("expected RegisterService on closed node to fail")
	}
}

type mutableMapFS struct {
	mu    sync.RWMutex
	files map[string][]byte
}

func (m *mutableMapFS) Open(name string) (fs.File, error) {
	m.mu.RLock()
	data, ok := m.files[name]
	m.mu.RUnlock()
	if !ok {
		return nil, fs.ErrNotExist
	}
	sub := fstest.MapFS{name: &fstest.MapFile{Data: append([]byte(nil), data...)}}
	return sub.Open(name)
}

func (m *mutableMapFS) Set(name string, data []byte) {
	m.mu.Lock()
	m.files[name] = append([]byte(nil), data...)
	m.mu.Unlock()
}

func TestLuaProfileInitStateEarlyMessageAndFreshReloadAndCrossNodeRejection(t *testing.T) {
	// 1. Early message sent during InitState must arrive AFTER the launch message.
	t.Run("InitStateEarlyMessageOrdering", func(t *testing.T) {
		n, err := NewNode(Config{Threads: 2, LogOutput: io.Discard, DisableOSFS: true})
		if err != nil {
			t.Fatal(err)
		}
		defer n.Close()

		var gotLaunch string
		var gotEarly string
		done := make(chan struct{})
		lt, err := n.NewLuaTemplate(LuaProfile{
			Libs:             moelua.LibSafe | moelua.LibPackageSafe,
			IsolatedFS:       true,
			DisableCore:      true,
			DisableCodeCache: true,
			DisableProfile:   true,
			InitState: func(actor *Actor, _ *moelua.State) error {
				// Send an early message directly to self before Init finishes.
				if ret := actor.Node().Send(nil, 0, actor.Handle(), PTypeText, 0, []byte("early-msg")); ret < 0 {
					return fmt.Errorf("early send failed: %d", ret)
				}
				return nil
			},
			LoaderFunc: func(_ *Actor, _ *moelua.State, param string) error {
				gotLaunch = param
				return nil
			},
			DispatchFunc: func(actor *Actor, _ *moelua.State, msg *Message) error {
				gotEarly = string(msg.Data)
				close(done)
				actor.Node().Kill(actor, 0)
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := n.LaunchWith("early-order", lt.NewService(), "launch-arg"); err != nil {
			t.Fatal(err)
		}
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for early message dispatch")
		}
		if gotLaunch != "launch-arg" {
			t.Fatalf("LoaderFunc param = %q, want %q", gotLaunch, "launch-arg")
		}
		if gotEarly != "early-msg" {
			t.Fatalf("DispatchFunc early msg = %q, want %q", gotEarly, "early-msg")
		}
	})

	// 2. InstallFreshVFSLoaders on a LibPackageSafe profile replaces the cached searcher in place.
	t.Run("LibPackageSafeFreshVFSReload", func(t *testing.T) {
		mfs := &mutableMapFS{files: map[string][]byte{
			"dyn.lua": []byte(`return "v1"`),
		}}
		n, err := NewNode(Config{Threads: 2, LogOutput: io.Discard, DisableOSFS: true})
		if err != nil {
			t.Fatal(err)
		}
		defer n.Close()

		var v1, v2 string
		done := make(chan error, 1)
		var lt *LuaTemplate
		lt, err = n.NewLuaTemplate(LuaProfile{
			Libs:             moelua.LibSafe | moelua.LibPackageSafe,
			IsolatedFS:       true,
			FS:               []fs.FS{mfs},
			DisableCore:      true,
			DisableCodeCache: true,
			DisableProfile:   true,
			LoaderFunc: func(actor *Actor, s *moelua.State, _ string) error {
				r1, err := s.DoString(`return require("dyn")`)
				if err != nil {
					done <- err
					return err
				}
				v1, _ = s.ToString(r1[0])

				// Mutate backing FS, clear package.loaded["dyn"], and switch to fresh VFS loaders.
				mfs.Set("dyn.lua", []byte(`return "v2"`))
				lt.InstallFreshVFSLoaders(s)
				r2, err := s.DoString(`
					package.loaded["dyn"] = nil
					return require("dyn")
				`)
				if err != nil {
					done <- err
					return err
				}
				v2, _ = s.ToString(r2[0])
				done <- nil
				actor.Node().Kill(actor, 0)
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := n.LaunchWith("fresh-reload", lt.NewService(), ""); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for fresh VFS reload")
		}
		if v1 != "v1" || v2 != "v2" {
			t.Fatalf("got v1=%q v2=%q, want v1=v1 v2=v2", v1, v2)
		}
	})

	// 3. Reject LuaTemplate created on Node A when launched on Node B.
	t.Run("RejectCrossNodeLuaTemplate", func(t *testing.T) {
		nodeA, err := NewNode(Config{Threads: 2, LogOutput: io.Discard, DisableOSFS: true})
		if err != nil {
			t.Fatal(err)
		}
		defer nodeA.Close()
		nodeB, err := NewNode(Config{Threads: 2, LogOutput: io.Discard, DisableOSFS: true})
		if err != nil {
			t.Fatal(err)
		}
		defer nodeB.Close()

		tmplA, err := nodeA.NewLuaTemplate(LuaProfile{
			Libs:             moelua.LibSafe | moelua.LibPackageSafe,
			IsolatedFS:       true,
			DisableCore:      true,
			DisableCodeCache: true,
			DisableProfile:   true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := nodeB.LaunchWith("cross-node", tmplA.NewService(), ""); err == nil || !strings.Contains(err.Error(), "different Node") {
			t.Fatalf("expected cross-node rejection error, got %v", err)
		}
		if err := nodeB.RegisterService("cross", tmplA.NewService); err != nil {
			t.Fatal(err)
		}
		if _, err := nodeB.Launch("cross", ""); err == nil || !strings.Contains(err.Error(), "different Node") {
			t.Fatalf("expected cross-node Launch error, got %v", err)
		}
	})
}
