package spacenet_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/yuchanns/spacenet"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitForLog(t *testing.T, buf *syncBuffer, substr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), substr) {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for log %q\n--- captured logs ---\n%s", substr, buf.String())
}

func newCoreTestNode(t *testing.T, startSvc string, extra ...spacenet.Option) (*spacenet.Node, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	opts := []spacenet.Option{
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(logs),
		spacenet.WithStart(startSvc),
		spacenet.WithFS(os.DirFS("third_party/skynet")),
		spacenet.WithEnv("luaservice", "./service/?.lua;./test/?.lua;./examples/?.lua"),
		spacenet.WithEnv("snax", "./test/?.lua;./examples/?.lua"),
	}
	opts = append(opts, extra...)
	node, err := spacenet.New(opts...)
	if err != nil {
		t.Fatalf("spacenet.New: %v", err)
	}
	t.Cleanup(func() {
		_ = node.Close()
	})
	return node, logs
}

func TestCoreBootstrapAndGoLuaRPC(t *testing.T) {
	customFS := fstest.MapFS{
		"service/go_rpc_main.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
require "skynet.manager"

skynet.start(function()
	skynet.dispatch("lua", function(session, source, cmd, a, b)
		if cmd == "add" then
			skynet.retpack(a + b, "ok")
		elseif cmd == "ping" then
			skynet.retpack("pong:" .. tostring(a))
		end
	end)
	skynet.register(".calc")
	skynet.error("GO_RPC_READY")
end)
`)},
	}

	node, logs := newCoreTestNode(t, "go_rpc_main", spacenet.WithFS(customFS))
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, logs, "GO_RPC_READY", 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	res, err := node.Call(ctx, ".calc", "add", 19, 23)
	if err != nil {
		t.Fatalf("Call add: %v", err)
	}
	if len(res) != 2 || res[0].(int64) != 42 || res[1].(string) != "ok" {
		t.Fatalf("unexpected add response: %#v", res)
	}

	res2, err := node.Call(ctx, ".calc", "ping", "spacenet")
	if err != nil {
		t.Fatalf("Call ping: %v", err)
	}
	if len(res2) != 1 || res2[0].(string) != "pong:spacenet" {
		t.Fatalf("unexpected ping response: %#v", res2)
	}
}

func TestUpstreamCoreSkynetTests(t *testing.T) {
	cases := []struct {
		name      string
		start     string
		expectLog string
		timeout   time.Duration
		extra     []spacenet.Option
	}{
		{
			name:      "testunpackn",
			start:     "testunpackn",
			expectLog: "skynet.unpackn tests passed",
			timeout:   5 * time.Second,
		},
		{
			name:      "testwarn",
			start:     "testwarn",
			expectLog: "error inside __gc",
			timeout:   5 * time.Second,
		},
		{
			name:      "testmemlimit",
			start:     "testmemlimit",
			expectLog: "Set memory limit to 1.00 M",
			timeout:   5 * time.Second,
		},
		{
			name:      "testselect",
			start:     "testselect",
			expectLog: "ping in CALL 20",
			timeout:   8 * time.Second,
		},
		{
			name:      "testtimeout",
			start:     "testtimeout",
			expectLog: "4 ",
			timeout:   8 * time.Second,
		},
		{
			name:      "testterm",
			start:     "testterm",
			expectLog: "unblock the unexisted service call",
			timeout:   5 * time.Second,
		},
		{
			name:      "testtobeclosed",
			start:     "testtobeclosed",
			expectLog: "closemeta:skynet.exit",
			timeout:   5 * time.Second,
		},
		{
			name:      "testping_snax_hotfix",
			start:     "testping",
			expectLog: "KILL self",
			timeout:   8 * time.Second,
		},
		{
			name:      "testqueue_snax",
			start:     "testqueue",
			expectLog: "KILL self",
			timeout:   8 * time.Second,
		},
		{
			name:      "testdeadcall",
			start:     "testdeadcall",
			expectLog: "KILL self",
			timeout:   5 * time.Second,
		},
		{
			name:      "testcoroutine_profile",
			start:     "testcoroutine",
			expectLog: "KILL self",
			timeout:   10 * time.Second,
		},
		{
			name:      "testdatacenter",
			start:     "testdatacenter",
			expectLog: "LAUNCH snlua testdatacenter",
			timeout:   5 * time.Second,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			node, logs := newCoreTestNode(t, tc.start, tc.extra...)
			if err := node.Start(); err != nil {
				t.Fatal(err)
			}
			waitForLog(t, logs, tc.expectLog, tc.timeout)

			out := logs.String()
			if tc.name == "testwarn" {
				if !strings.Contains(out, "Lua warning: test warning") {
					t.Fatalf("missing single-part warning:\n%s", out)
				}
				if !strings.Contains(out, "Lua warning< multi-part ") || !strings.Contains(out, "Lua warning+ test ") || !strings.Contains(out, "Lua warning> warning") {
					t.Fatalf("missing multi-part warning:\n%s", out)
				}
				if strings.Contains(out, "INVISIBLE warning") {
					t.Fatalf("warn(@off) failed to suppress warning:\n%s", out)
				}
			}
			if tc.name == "testmemlimit" {
				waitForLog(t, logs, "not enough memory", 5*time.Second)
			}
			if tc.name == "testtobeclosed" {
				for _, want := range []string{"closemeta:stack_raise_error", "closemeta:dispatch_error", "closemeta:session_id_coroutine_wait", "closemeta:session_id_coroutine_call"} {
					if !strings.Contains(out, want) {
						t.Fatalf("missing %s in testtobeclosed output:\n%s", want, out)
					}
				}
			}
		})
	}
}

func TestEndlessLoopDetectionAndSignal0(t *testing.T) {
	customFS := fstest.MapFS{
		"service/loop_driver.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
require "skynet.manager"

local mode = ...
if mode == "worker" then
	skynet.start(function()
		skynet.dispatch("lua", function()
			local ok, err = pcall(function()
				local i = 0
				while true do
					i = i + 1
				end
			end)
			skynet.error("LOOP_INTERRUPTED", ok, tostring(err))
			skynet.retpack(ok, tostring(err))
		end)
	end)
else
	skynet.start(function()
		local w = skynet.newservice("loop_driver", "worker")
		skynet.fork(function()
			skynet.call(w, "lua")
			skynet.error("DRIVER_DONE")
			skynet.exit()
		end)
		skynet.sleep(30)
		skynet.send(".launcher", "lua", "LIST")
		-- Send signal 0 to interrupt endless loop
		local c = require "skynet.core"
		c.command("SIGNAL", skynet.address(w) .. " 0")
	end)
end
`)},
	}

	node, logs := newCoreTestNode(t, "loop_driver",
		spacenet.WithFS(customFS),
		spacenet.WithMonitorInterval(100*time.Millisecond),
	)
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, logs, "maybe in an endless loop", 5*time.Second)
	waitForLog(t, logs, "LOOP_INTERRUPTED false", 5*time.Second)
	waitForLog(t, logs, "DRIVER_DONE", 5*time.Second)
}

func TestCodeCacheClearAndMode(t *testing.T) {
	customFS := fstest.MapFS{
		"service/cache_test.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local cc = require "skynet.codecache"

skynet.start(function()
	assert(cc.mode() == "ON")
	assert(cc.mode("EXIST") == "EXIST")
	assert(cc.mode("OFF") == "OFF")
	assert(cc.mode("ON") == "ON")
	cc.clear()
	skynet.error("CODECACHE_OK")
	skynet.exit()
end)
`)},
	}
	node, logs := newCoreTestNode(t, "cache_test", spacenet.WithFS(customFS))
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, logs, "CODECACHE_OK", 5*time.Second)
}

func TestLoadFileWithEnvAndMetaPairsUnderGCPressure(t *testing.T) {
	// Generate a multi-function Lua chunk large enough to accumulate significant
	// GC debt during s.Load(ch), then loadfile(path, "t", env)() repeatedly in a
	// loop so post-call GC steps fire inside debug.setupvalue.
	var chunk strings.Builder
	chunk.WriteString("local M = {}\n")
	for i := 0; i < 32; i++ {
		chunk.WriteString("local function f")
		chunk.WriteString(strings.Repeat("x", 8))
		chunk.WriteString(string(rune('a'+i%26)) + string(rune('a'+i/26)))
		chunk.WriteString("(n) return n + 1 end\n")
	}
	chunk.WriteString("function M.run(x) local function inner(y) return y * 2 end return inner(x) + tag end\nreturn M\n")

	customFS := fstest.MapFS{
		"lualib/gc_chunk.lua": &fstest.MapFile{Data: []byte(chunk.String())},
		"service/gc_pin_test.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"

skynet.start(function()
	for i = 1, 40 do
		local env = setmetatable({ tag = i }, { __index = _G })
		local m = assert(loadfile("lualib/gc_chunk.lua", "t", env))()
		assert(m.run(10) == 20 + i)
	end

	-- Exercise custom __pairs iterator closure + fresh key/value under GC pressure.
	local proxy = setmetatable({}, {
		__pairs = function()
			local n = 0
			return function(_, prev)
				n = n + 1
				if n > 200 then return nil end
				return "k_" .. n, { v = string.rep("z", 128) }
			end, {}, nil
		end,
	})
	local msg, sz = skynet.pack(proxy)
	local unpacked = skynet.unpack(msg, sz)
	assert(unpacked["k_1"].v == string.rep("z", 128))
	assert(unpacked["k_200"].v == string.rep("z", 128))

	skynet.error("GC_PIN_OK")
	skynet.exit()
end)
`)},
	}

	node, logs := newCoreTestNode(t, "gc_pin_test", spacenet.WithFS(customFS))
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, logs, "GC_PIN_OK", 8*time.Second)
}
