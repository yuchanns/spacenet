package debug_test

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/debug"
	"github.com/yuchanns/spacenet/runtime"
)

type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitLog(t *testing.T, buf *syncBuf, substr string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), substr) {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %q\nlogs:\n%s", substr, buf.String())
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

func TestDebugConsoleMemoryAndChannel(t *testing.T) {
	port := freeTCPPort(t)

	customFS := fstest.MapFS{
		"service/dbg_worker.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local mem = require "skynet.memory"
local dc = require "skynet.debugchannel"

skynet.start(function()
	skynet.dispatch("lua", function(_, _, cmd, arg)
		if cmd == "prof" then
			skynet.retpack(mem.profactive())
		elseif cmd == "read_wait" then
			local r = dc.connect(arg)
			-- Block with a long timeout (5s); writer wakes us immediately.
			local msg = r:read(50)
			dc.release(r)
			skynet.retpack(msg)
		end
	end)
end)
`)},
		"service/dbg_main.lua": &fstest.MapFile{Data: []byte(fmt.Sprintf(`
local skynet = require "skynet"
local mem = require "skynet.memory"
local dc = require "skynet.debugchannel"

skynet.start(function()
	assert(mem.total() > 0)
	assert(mem.current() > 0)
	local js = mem.jestat()
	assert(js["stats.allocated"] > 0)
	assert(mem.mallctl("epoch") == 0)
	assert(not pcall(mem.mallctl, 123))

	-- profactive state is shared per-Node across actors.
	local worker = skynet.newservice("dbg_worker")
	assert(mem.profactive() == false)
	assert(mem.profactive(true) == true)
	assert(skynet.call(worker, "lua", "prof") == true)
	assert(mem.profactive(false) == false)

	-- Basic channel write/read, single-consumer connect limit, and idempotent release.
	local w, ptr = dc.create()
	local r = dc.connect(ptr)
	assert(not pcall(dc.connect, ptr), "second connect should fail while connected")
	w:write("hello-dbg")
	assert(r:read() == "hello-dbg")
	assert(r:read() == nil)
	assert(not pcall(w.write, w, 123))
	assert(not pcall(r.read, r, "bad"))
	dc.release(r)
	dc.release(r)
	dc.release(w)
	dc.release(w)

	-- Blocking read wakes immediately when writer sends a message.
	local w2, ptr2 = dc.create()
	local t0 = skynet.hpc()
	skynet.fork(function()
		skynet.sleep(2)
		w2:write("wake-now")
	end)
	local got = skynet.call(worker, "lua", "read_wait", ptr2)
	local elapsed_ms = (skynet.hpc() - t0) / 1e6
	assert(got == "wake-now", "unexpected wake msg: " .. tostring(got))
	assert(elapsed_ms < 2000, "blocking read did not wake early: " .. tostring(elapsed_ms) .. "ms")
	dc.release(w2)

	skynet.newservice("debug_console", "127.0.0.1", %d)
	skynet.error("DEBUG_READY")
end)
`, port))},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(debug.New()),
		spacenet.WithFS(customFS),
		spacenet.WithStart("dbg_main"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "DEBUG_READY")
	if strings.Contains(logs.String(), "error :") {
		t.Fatalf("debug setup errored:\n%s", logs.String())
	}

	if live := node.Runtime().Objs().Count(runtime.KindDebugChan); live != 0 {
		t.Fatalf("expected 0 leaked debug channels after release, got %d", live)
	}

	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))

	br := bufio.NewReader(conn)
	welcome, err := br.ReadString('\n')
	if err != nil || !strings.Contains(welcome, "Welcome to skynet console") {
		t.Fatalf("unexpected welcome: %q err=%v", welcome, err)
	}
	_, _ = conn.Write([]byte("list\n"))
	var out strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("read list failed: %v (got %q)", err, out.String())
		}
		out.WriteString(line)
		if strings.Contains(line, "<CMD OK>") {
			break
		}
	}
	if !strings.Contains(out.String(), "debug_console") {
		t.Fatalf("list output missing debug_console:\n%s", out.String())
	}
}

func TestDebugChannelReadCancelsOnNodeClose(t *testing.T) {
	customFS := fstest.MapFS{
		"service/dbg_block.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local dc = require "skynet.debugchannel"

skynet.start(function()
	local w, ptr = dc.create()
	local r = dc.connect(ptr)
	skynet.error("BLOCK_READ_START")
	-- 600s timeout must unblock immediately when Node.Close() cancels the actor.
	r:read(6000)
	dc.release(r)
	dc.release(w)
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(debug.New()),
		spacenet.WithFS(customFS),
		spacenet.WithStart("dbg_block"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "BLOCK_READ_START")
	time.Sleep(50 * time.Millisecond)

	closed := make(chan struct{})
	go func() {
		_ = node.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatalf("node.Close() blocked on debugchannel.read\nlogs:\n%s", logs.String())
	}
	if live := node.Runtime().Objs().Count(runtime.KindDebugChan); live != 0 {
		t.Fatalf("expected 0 leaked debug channels after Close, got %d", live)
	}
}
