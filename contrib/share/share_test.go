package share_test

import (
	"bytes"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/share"
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
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), substr) {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %q\nlogs:\n%s", substr, buf.String())
}

func TestShareTableDatasheetShareDataSTMMulticast(t *testing.T) {
	t.Run("sharetable_and_update", func(t *testing.T) {
		customFS := fstest.MapFS{
			"service/st_test.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local sharetable = require "skynet.sharetable"

skynet.start(function()
	sharetable.loadstring("cfg", "return { ver = 1, items = { a = 10, b = 20 }, fn = function(x) return x * 2 end }")
	local t = sharetable.query("cfg")
	assert(t.ver == 1 and t.items.a == 10 and t.fn(21) == 42)

	sharetable.loadtable("tbl", { hello = "spacenet", list = { 1, 2, 3 } })
	local t2 = sharetable.query("tbl")
	assert(t2.hello == "spacenet" and t2.list[3] == 3)

	-- Live hot-update via sharetable.update
	sharetable.loadstring("cfg", "return { ver = 2, items = { a = 99 } }")
	sharetable.update("cfg")
	assert(t.ver == 2 and t.items.a == 99 and t.items.b == nil, "update failed: ver=" .. tostring(t.ver))

	skynet.error("SHARETABLE_OK")
	skynet.exit()
end)
`)},
		}
		logs := &syncBuf{}
		node, err := spacenet.New(
			spacenet.WithThreads(4),
			spacenet.WithLogOutput(logs),
			spacenet.WithContrib(share.New()),
			spacenet.WithFS(customFS),
			spacenet.WithStart("st_test"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()
		_ = node.Start()
		waitLog(t, logs, "SHARETABLE_OK")
	})

	t.Run("upstream_testdatasheet", func(t *testing.T) {
		logs := &syncBuf{}
		node, err := spacenet.New(
			spacenet.WithThreads(4),
			spacenet.WithLogOutput(logs),
			spacenet.WithContrib(share.New()),
			spacenet.WithFS(os.DirFS("../../third_party/skynet")),
			spacenet.WithStart("testdatasheet"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()
		_ = node.Start()
		waitLog(t, logs, "KILL self")
		if strings.Contains(logs.String(), "error :") {
			t.Fatalf("testdatasheet errored:\n%s", logs.String())
		}
	})

	t.Run("upstream_teststm", func(t *testing.T) {
		logs := &syncBuf{}
		node, err := spacenet.New(
			spacenet.WithThreads(4),
			spacenet.WithLogOutput(logs),
			spacenet.WithContrib(share.New()),
			spacenet.WithFS(os.DirFS("../../third_party/skynet")),
			spacenet.WithStart("teststm"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()
		_ = node.Start()
		// Both master (:00000008) and slave (:00000009) complete and call skynet.exit().
		waitLog(t, logs, "sleep and read")
		waitLog(t, logs, "[:00000008] KILL self")
		waitLog(t, logs, "[:00000009] KILL self")
		if strings.Contains(logs.String(), "error :") {
			t.Fatalf("teststm errored:\n%s", logs.String())
		}
	})

	t.Run("upstream_testmulticast_and_verified_delivery", func(t *testing.T) {
		customFS := fstest.MapFS{
			"service/mc_verify.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local mc = require "skynet.multicast"
local mode, parent = ...

if mode == "sub" then
	parent = tonumber(parent)
	skynet.start(function()
		skynet.dispatch("lua", function(_, _, cmd, channel)
			assert(cmd == "init")
			local c = mc.new {
				channel = channel,
				dispatch = function(_, _, msg, num)
					skynet.send(parent, "lua", "got", msg, num)
				end
			}
			c:subscribe()
			skynet.ret(skynet.pack())
		end)
	end)
else
	skynet.start(function()
		local got = 0
		skynet.dispatch("lua", function(_, _, cmd, msg, num)
			if cmd == "got" then
				assert(msg == "Hello Multicast" and num == 42)
				got = got + 1
				if got == 10 then
					skynet.error("MULTICAST_ALL_10_OK")
				end
			end
		end)
		local ch = mc.new()
		for i = 1, 10 do
			local sub = skynet.newservice("mc_verify", "sub", skynet.self())
			skynet.call(sub, "lua", "init", ch.channel)
		end
		ch:publish("Hello Multicast", 42)
	end)
end
`)},
		}
		logs := &syncBuf{}
		node, err := spacenet.New(
			spacenet.WithThreads(4),
			spacenet.WithLogOutput(logs),
			spacenet.WithContrib(share.New()),
			spacenet.WithFS(customFS),
			spacenet.WithStart("mc_verify"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()
		_ = node.Start()
		waitLog(t, logs, "MULTICAST_ALL_10_OK")
		if strings.Contains(logs.String(), "error :") {
			t.Fatalf("multicast errored:\n%s", logs.String())
		}
	})

	t.Run("sharedata_roundtrip", func(t *testing.T) {
		customFS := fstest.MapFS{
			"service/sd_test.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local sharedata = require "skynet.sharedata"

skynet.start(function()
	sharedata.new("foobar", { a = 1, b = { "x", "y" }, c = 3.5 })
	local obj = sharedata.query("foobar")
	assert(obj.a == 1 and obj.b[2] == "y" and obj.c == 3.5)
	sharedata.update("foobar", { a = 42, b = { "z" } })
	skynet.sleep(20)
	assert(obj.a == 42 and obj.b[1] == "z", "sharedata update failed: a=" .. tostring(obj.a))
	skynet.error("SHAREDATA_OK")
	skynet.exit()
end)
`)},
		}
		logs := &syncBuf{}
		node, err := spacenet.New(
			spacenet.WithThreads(4),
			spacenet.WithLogOutput(logs),
			spacenet.WithContrib(share.New()),
			spacenet.WithFS(customFS),
			spacenet.WithStart("sd_test"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer node.Close()
		_ = node.Start()
		waitLog(t, logs, "SHAREDATA_OK")
	})
}
