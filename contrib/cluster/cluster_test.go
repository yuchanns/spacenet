package cluster_test

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/cluster"
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

// TestTwoNodeClusterRPCAndLargeMultipart spawns two independent spacenet Nodes
// in the same Go process and verifies Skynet's cluster RPC, push, proxy, error
// propagation, and >64 KiB multipart framing end-to-end.
func TestTwoNodeClusterRPCAndLargeMultipart(t *testing.T) {
	port := freeTCPPort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	serverFS := fstest.MapFS{
		"service/node_a.lua": &fstest.MapFile{Data: []byte(fmt.Sprintf(`
local skynet = require "skynet"
local cluster = require "skynet.cluster"

skynet.start(function()
	local pushed = nil
	skynet.dispatch("lua", function(session, source, cmd, arg)
		if cmd == "echo" then
			skynet.retpack("A:" .. arg)
		elseif cmd == "push_msg" then
			pushed = arg
		elseif cmd == "get_pushed" then
			skynet.retpack(pushed)
		elseif cmd == "fail" then
			error("boom-from-node-a")
		end
	end)
	cluster.reload {
		db = %q,
	}
	cluster.register("echo_svc", skynet.self())
	cluster.open("db")
	skynet.error("NODE_A_READY")
end)
`, addr))},
	}

	clientFS := fstest.MapFS{
		"service/node_b.lua": &fstest.MapFile{Data: []byte(fmt.Sprintf(`
local skynet = require "skynet"
local cluster = require "skynet.cluster"

skynet.start(function()
	cluster.reload {
		db = %q,
	}
	-- Small RPC
	local r1 = cluster.call("db", "@echo_svc", "echo", "hello-cluster")
	assert(r1 == "A:hello-cluster", "unexpected r1: " .. tostring(r1))

	-- Large > 64 KiB multipart RPC (both request and response split into chunks)
	local big = string.rep("Z", 70000)
	local r2 = cluster.call("db", "@echo_svc", "echo", big)
	assert(#r2 == 2 + #big and r2:sub(1, 2) == "A:", "multipart mismatch: len=" .. tostring(#r2))

	-- One-way push (small and multipart)
	cluster.send("db", "@echo_svc", "push_msg", "async-ok")
	local r3 = cluster.call("db", "@echo_svc", "get_pushed")
	assert(r3 == "async-ok", "unexpected push: " .. tostring(r3))

	-- Multipart push sends padding chunks via low-priority socket.lwrite, so a subsequent
	-- high-priority cluster.call can overtake it; poll briefly until the push lands.
	local bigPush = string.rep("P", 70000)
	cluster.send("db", "@echo_svc", "push_msg", bigPush)
	local r3b
	for _ = 1, 50 do
		r3b = cluster.call("db", "@echo_svc", "get_pushed")
		if r3b == bigPush then
			break
		end
		skynet.sleep(2)
	end
	assert(r3b == bigPush, "multipart push mismatch: len=" .. tostring(#(r3b or "")))

	-- Cluster proxy
	local proxy = cluster.proxy("db", "@echo_svc")
	local r4 = skynet.call(proxy, "lua", "echo", "via-proxy")
	assert(r4 == "A:via-proxy", "unexpected proxy: " .. tostring(r4))

	-- Remote error propagation
	local okErr = pcall(cluster.call, "db", "@echo_svc", "fail")
	assert(not okErr, "expected remote error to fail cluster.call")

	skynet.error("CLUSTER_E2E_OK")
	skynet.exit()
end)
`, addr))},
	}

	logsA := &syncBuf{}
	nodeA, err := spacenet.New(
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(logsA),
		spacenet.WithContrib(cluster.New()),
		spacenet.WithFS(serverFS),
		spacenet.WithStart("node_a"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nodeA.Close()
	if err := nodeA.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logsA, "NODE_A_READY")

	logsB := &syncBuf{}
	nodeB, err := spacenet.New(
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(logsB),
		spacenet.WithContrib(cluster.New()),
		spacenet.WithFS(clientFS),
		spacenet.WithStart("node_b"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nodeB.Close()
	if err := nodeB.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logsB, "CLUSTER_E2E_OK")
}

func TestClusterCoreFramingAndBounds(t *testing.T) {
	customFS := fstest.MapFS{
		"service/core_bounds.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local core = require "skynet.cluster.core"

skynet.start(function()
	assert(core.isname("@svc") == true)
	assert(core.isname("svc") == nil)
	assert(core.isname(123) == nil)
	local nn = core.nodename()
	assert(type(nn) == "string" and #nn > 0)

	-- Trace packet round-trip
	local tracePkt = core.packtrace("trace-id-42")
	local tag = core.unpackrequest(tracePkt:sub(3))
	assert(tag == "trace-id-42", "trace mismatch: " .. tostring(tag))

	-- Numeric address single-packet round-trip
	local msg, sz = skynet.pack("ping", 123)
	local req, nextSess = core.packrequest(0x102, 7, msg, sz)
	assert(nextSess == 8)
	local addr, sess, umsg, usz = core.unpackrequest(req:sub(3))
	assert(addr == 0x102 and sess == 7 and usz == sz)
	local a, b = skynet.unpack(umsg, usz)
	assert(a == "ping" and b == 123)
	-- Consume umsg via append + concat
	local acc = {}
	core.append(acc, nil, usz)
	core.append(acc, umsg, usz)
	local cmsg, csz = core.concat(acc)
	assert(csz == usz)
	local ca, cb = skynet.unpack(cmsg, csz)
	assert(ca == "ping" and cb == 123)
	-- Free cmsg via packrequest
	local _ = core.packrequest("@dst", 1, cmsg, csz)

	-- Forged/negative sizes in concat must safely return nil without allocating or panicking
	assert(core.concat({ -1 }) == nil)
	assert(core.concat({ 0x7fffffff, "abc" }) == nil)
	assert(core.concat({ 2, "abcd" }) == nil)

	-- Invalid session / negative size in packrequest must error and free buffer
	local m1, s1 = skynet.pack("x")
	assert(not pcall(core.packrequest, "@dst", 0, m1, s1))
	local m2, _ = skynet.pack("y")
	assert(not pcall(core.packrequest, "@dst", 1, m2, -5))

	-- Error response round-trip
	local errResp = core.packresponse(9, false, "err-detail")
	local rsess, rok, rmsg = core.unpackresponse(errResp:sub(3))
	assert(rsess == 9 and rok == false and rmsg == "err-detail")

	skynet.error("CLUSTER_CORE_BOUNDS_OK")
	skynet.exit()
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(cluster.New()),
		spacenet.WithFS(customFS),
		spacenet.WithStart("core_bounds"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "CLUSTER_CORE_BOUNDS_OK")
	if strings.Contains(logs.String(), "error :") {
		t.Fatalf("cluster core bounds test errored:\n%s", logs.String())
	}
}
