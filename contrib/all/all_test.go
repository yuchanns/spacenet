package all_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"net"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	stdruntime "runtime"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/all"
	sphttp "github.com/yuchanns/spacenet/contrib/http"
	"github.com/yuchanns/spacenet/contrib/share"
	"github.com/yuchanns/spacenet/contrib/socket"
	"github.com/yuchanns/spacenet/contrib/sproto"
	sptls "github.com/yuchanns/spacenet/contrib/tls"
)

const skynetAliasPreload = `
local function alias(name)
	if name == "skynet" or name:sub(1, 7) == "skynet." then
		return "spacenet" .. name:sub(7)
	end
	return name
end
local orig_require = _G.require
_G.require = function(name) return orig_require(alias(name)) end
local sr = orig_require("spacenet.require")
local orig_sr = sr.require
sr.require = function(name) return orig_sr(alias(name)) end
`

type onlyDirsFS struct {
	base fs.FS
	dirs []string
}

func (f onlyDirsFS) Open(name string) (fs.File, error) {
	if name == "." {
		return f.base.Open(name)
	}
	for _, d := range f.dirs {
		if name == d || strings.HasPrefix(name, d+"/") {
			return f.base.Open(name)
		}
	}
	return nil, fs.ErrNotExist
}

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

func waitLog(t *testing.T, buf *syncBuf, substr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
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

func generateSelfSignedCertFiles(t *testing.T) (certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath = filepath.Join(dir, "server-cert.pem")
	keyPath = filepath.Join(dir, "server-key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

func TestAllContribsLoadTogether(t *testing.T) {
	customFS := fstest.MapFS{
		"service/all_smoke.lua": &fstest.MapFile{Data: []byte(`
local spacenet = require "spacenet"
local socket = require "spacenet.socket"
local crypt = require "spacenet.crypt"
local tlshelper = require "http.tlshelper"
local httpc = require "http.httpc"
local cluster = require "spacenet.cluster"
local sharetable = require "spacenet.sharetable"
local sproto = require "sproto"
local memory = require "spacenet.memory"
local debugchannel = require "spacenet.debugchannel"

spacenet.start(function()
	assert(type(socket.listen) == "function")
	assert(type(crypt.sha1) == "function")
	assert(type(tlshelper.newctx) == "function")
	assert(type(httpc.get) == "function")
	assert(type(cluster.open) == "function")
	assert(type(sharetable.loadtable) == "function")
	assert(type(sproto.parse) == "function")
	assert(type(memory.info) == "function")
	assert(type(debugchannel.create) == "function")
	spacenet.error("ALL_CONTRIBS_OK")
	spacenet.exit()
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(2),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(all.Contribs()...),
		spacenet.WithFS(customFS),
		spacenet.WithStart("all_smoke"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "ALL_CONTRIBS_OK", 10*time.Second)
}

func TestZeroBloatModularity(t *testing.T) {
	checkDeps := func(dir string, forbidden []string) {
		t.Helper()
		cmd := exec.Command("go", "list", "-deps", ".")
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("go list -deps in %s failed: %v\n%s", dir, err, out)
		}
		deps := make(map[string]bool)
		for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
			deps[strings.TrimSpace(line)] = true
		}
		for _, bad := range forbidden {
			for d := range deps {
				if d == bad || strings.HasPrefix(d, bad+"/") {
					t.Errorf("module %s unexpectedly depends on %q (matched %q)", dir, d, bad)
				}
			}
		}
	}

	// Core must have zero networking, TLS/cipher/hash, or contrib dependencies.
	// (moelua's stdlib uses crypto/rand for math.randomseed default entropy.)
	checkDeps("../..", []string{
		"net",
		"crypto/tls",
		"crypto/x509",
		"crypto/aes",
		"crypto/des",
		"crypto/md5",
		"crypto/sha1",
		"crypto/hmac",
		"github.com/yuchanns/spacenet/contrib",
	})

	// contrib/sproto must have zero net/TLS/socket dependencies.
	checkDeps("../sproto", []string{
		"net",
		"crypto/tls",
		"crypto/x509",
		"github.com/yuchanns/spacenet/contrib/socket",
		"github.com/yuchanns/spacenet/contrib/http",
		"github.com/yuchanns/spacenet/contrib/tls",
	})

	// contrib/crypt must have zero net/socket/tls dependencies.
	checkDeps("../crypt", []string{
		"net",
		"crypto/tls",
		"github.com/yuchanns/spacenet/contrib/socket",
	})
}

func TestHTTPSAndWSSEndToEnd(t *testing.T) {
	certPath, keyPath := generateSelfSignedCertFiles(t)
	certSlash := filepath.ToSlash(certPath)
	keySlash := filepath.ToSlash(keyPath)
	httpsPort := freeTCPPort(t)
	wssPort := freeTCPPort(t)

	goTLS := httptest.NewTLSServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		_, _ = w.Write([]byte("from-go-tls:" + r.URL.Path))
	}))
	defer goTLS.Close()

	customFS := fstest.MapFS{
		"service/https_wss_main.lua": &fstest.MapFile{Data: []byte(fmt.Sprintf(`
local spacenet = require "spacenet"
local socket = require "spacenet.socket"
local httpd = require "http.httpd"
local httpc = require "http.httpc"
local websocket = require "http.websocket"
local tls = require "http.tlshelper"

local CERT = %q
local KEY = %q
local HTTPS_PORT = %d
local WSS_PORT = %d
local GO_TLS_URL = %q

spacenet.start(function()
	local srv_ctx = tls.newctx()
	srv_ctx:set_cert(CERT, KEY)

	-- 1. HTTPS server (httpd + tlshelper)
	local hfd = socket.listen("127.0.0.1", HTTPS_PORT)
	socket.start(hfd, function(id)
		spacenet.fork(function()
			socket.start(id)
			local tctx = tls.newtls("server", srv_ctx)
			local init = tls.init_responsefunc(id, tctx)
			local read = tls.readfunc(id, tctx)
			local write = tls.writefunc(id, tctx)
			local close = tls.closefunc(tctx)
			init()
			local code, url = httpd.read_request(read, 8192)
			if code == 200 then
				httpd.write_response(write, 200, "spacenet-https:" .. url)
			end
			socket.close(id)
			close()
		end)
	end)

	-- 2. WSS server (http.websocket + tlshelper)
	local wfd = socket.listen("127.0.0.1", WSS_PORT)
	socket.start(wfd, function(id)
		spacenet.fork(function()
			local handle = {}
			function handle.message(wsid, msg)
				websocket.write(wsid, "wss-echo:" .. msg)
			end
			websocket.accept(id, handle, "wss", nil)
		end)
	end)

	spacenet.error("TLS_SERVERS_READY")

	-- 3. httpc -> spacenet HTTPS server
	local code1, body1 = httpc.get("https://127.0.0.1:" .. HTTPS_PORT, "/hello")
	assert(code1 == 200 and body1 == "spacenet-https:/hello", "spacenet https failed: " .. tostring(code1) .. " " .. tostring(body1))

	-- 4. httpc -> Go httptest.NewTLSServer
	local code2, body2 = httpc.get(GO_TLS_URL, "/ping")
	assert(code2 == 200 and body2 == "from-go-tls:/ping", "go https failed: " .. tostring(code2) .. " " .. tostring(body2))

	-- 5. websocket client -> spacenet WSS server
	local ws = websocket.connect("wss://127.0.0.1:" .. WSS_PORT .. "/ws")
	websocket.write(ws, "secret-wss")
	local msg = websocket.read(ws)
	assert(msg == "wss-echo:secret-wss", "wss echo mismatch: " .. tostring(msg))
	websocket.close(ws)

	spacenet.error("HTTPS_WSS_ALL_OK")
end)
`, certSlash, keySlash, httpsPort, wssPort, goTLS.URL))},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(sphttp.New(), sptls.New()),
		spacenet.WithEnv("certfile", certSlash),
		spacenet.WithEnv("keyfile", keySlash),
		spacenet.WithFS(customFS),
		spacenet.WithStart("https_wss_main"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}

	waitLog(t, logs, "TLS_SERVERS_READY", 15*time.Second)

	// Verify Go's net/http TLS client against the spacenet HTTPS server.
	client := &stdhttp.Client{
		Timeout: 10 * time.Second,
		Transport: &stdhttp.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	resp, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/from-go-client", httpsPort))
	if err != nil {
		t.Fatalf("Go HTTPS GET failed: %v\nlogs:\n%s", err, logs.String())
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "spacenet-https:/from-go-client" {
		t.Fatalf("unexpected Go HTTPS response: %d %q", resp.StatusCode, string(body))
	}

	waitLog(t, logs, "HTTPS_WSS_ALL_OK", 15*time.Second)
}

func TestUpstreamGameServerWatchdogAgentSproto(t *testing.T) {
	gatePort := freeTCPPort(t)
	examplesFS := onlyDirsFS{
		base: os.DirFS("../../third_party/skynet"),
		dirs: []string{"examples"},
	}

	// Server uses unmodified upstream third_party/skynet/examples/{protoloader,simpledb,watchdog,agent,proto}.lua
	// via a user-side preload alias script.
	srvFS := fstest.MapFS{
		"skynet_alias.lua": &fstest.MapFile{Data: []byte(skynetAliasPreload)},
		"service/game_srv_main.lua": &fstest.MapFile{Data: []byte(fmt.Sprintf(`
local spacenet = require "spacenet"
spacenet.start(function()
	spacenet.uniqueservice("protoloader")
	spacenet.newservice("simpledb")
	local watchdog = spacenet.newservice("watchdog")
	spacenet.call(watchdog, "lua", "start", {
		address = "127.0.0.1",
		port = %d,
		maxclient = 64,
		nodelay = true,
	})
	spacenet.error("GAME_SERVER_READY")
end)
`, gatePort))},
	}

	srvLogs := &syncBuf{}
	srvNode, err := spacenet.New(
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(srvLogs),
		spacenet.WithContrib(socket.New(), sproto.New()),
		spacenet.WithFS(srvFS),
		spacenet.WithFS(examplesFS),
		spacenet.WithEnv("preload", "./skynet_alias.lua"),
		spacenet.WithEnv("luaservice", "./service/?.lua;./examples/?.lua"),
		spacenet.WithEnv("lua_path", "./lualib/?.lua;./lualib/?/init.lua;./examples/?.lua"),
		spacenet.WithStart("game_srv_main"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer srvNode.Close()
	if err := srvNode.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, srvLogs, "GAME_SERVER_READY", 15*time.Second)

	// Client node speaks 2-byte big-endian framed sproto against upstream watchdog+agent.
	cliFS := fstest.MapFS{
		"service/game_cli.lua": &fstest.MapFile{Data: []byte(fmt.Sprintf(`
local spacenet = require "spacenet"
local socket = require "spacenet.socket"
local sproto = require "sproto"
local proto = require "proto"

local function send_pkg(fd, data)
	socket.write(fd, string.pack(">s2", data))
end

local function recv_pkg(fd)
	local hdr = assert(socket.read(fd, 2))
	local sz = string.unpack(">H", hdr)
	return assert(socket.read(fd, sz))
end

spacenet.start(function()
	local host = sproto.new(proto.s2c):host "package"
	local req = host:attach(sproto.new(proto.c2s))
	local fd = assert(socket.open("127.0.0.1", %d))

	local function rpc(name, args, session)
		send_pkg(fd, req(name, args, session))
		while true do
			local pkt = recv_pkg(fd)
			local t, a, b = host:dispatch(pkt)
			if t == "RESPONSE" and a == session then
				return b
			end
			-- ignore server-initiated heartbeat REQUEST packets
		end
	end

	local h = rpc("handshake", nil, 1)
	assert(h and h.msg and h.msg:find("Welcome to skynet"), "unexpected handshake: " .. tostring(h and h.msg))

	send_pkg(fd, req("set", { what = "hero", value = "Motoko" }, 2))
	local g
	local sess = 3
	for _ = 1, 50 do
		g = rpc("get", { what = "hero" }, sess)
		sess = sess + 1
		if g and g.result == "Motoko" then
			break
		end
		spacenet.sleep(5)
	end
	assert(g and g.result == "Motoko", "unexpected get result: " .. tostring(g and g.result))

	send_pkg(fd, req("quit", nil, sess))
	socket.close(fd)
	spacenet.error("GAME_CLIENT_OK")
	spacenet.exit()
end)
`, gatePort))},
	}

	cliLogs := &syncBuf{}
	cliNode, err := spacenet.New(
		spacenet.WithThreads(2),
		spacenet.WithLogOutput(cliLogs),
		spacenet.WithContrib(socket.New(), sproto.New()),
		spacenet.WithFS(cliFS),
		spacenet.WithFS(examplesFS),
		spacenet.WithEnv("lua_path", "./lualib/?.lua;./lualib/?/init.lua;./examples/?.lua"),
		spacenet.WithStart("game_cli"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cliNode.Close()
	if err := cliNode.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, cliLogs, "GAME_CLIENT_OK", 15*time.Second)
}

func TestUpstreamShareMapWithSproto(t *testing.T) {
	spPath, err := filepath.Abs("../../third_party/skynet/test/sharemap.sp")
	if err != nil {
		t.Fatal(err)
	}
	customFS := fstest.MapFS{
		"service/sm_verify.lua": &fstest.MapFile{Data: []byte(fmt.Sprintf(`
local spacenet = require "spacenet"
local sharemap = require "spacenet.sharemap"
local mode = ...

if mode == "slave" then
	local reader
	spacenet.start(function()
		spacenet.dispatch("lua", function(_, _, cmd, ...)
			if cmd == "init" then
				reader = sharemap.reader(...)
				spacenet.ret()
			elseif cmd == "read" then
				reader:update()
				spacenet.ret(spacenet.pack(reader.x, reader.y, reader.s))
			end
		end)
	end)
else
	spacenet.start(function()
		sharemap.register(%q)
		local slave = spacenet.newservice("sm_verify", "slave")
		local writer = sharemap.writer("foobar", { x = 0, y = 0, s = "hello" })
		spacenet.call(slave, "lua", "init", "foobar", writer:copy())

		writer.x = 7
		writer.y = 42
		writer.s = "spacenet"
		writer:commit()

		local x, y, s = spacenet.call(slave, "lua", "read")
		assert(x == 7 and y == 42 and s == "spacenet", string.format("got %%s %%s %%s", x, y, s))
		spacenet.error("SHAREMAP_OK")
		spacenet.exit()
	end)
end
`, filepath.ToSlash(spPath)))},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(share.New(), sproto.New()),
		spacenet.WithFS(customFS),
		spacenet.WithStart("sm_verify"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "SHAREMAP_OK", 15*time.Second)
}

func TestScale1000ConcurrentGameAgents(t *testing.T) {
	const numAgents = 1000

	customFS := fstest.MapFS{
		"service/scale_agent.lua": &fstest.MapFile{Data: []byte(`
local spacenet = require "spacenet"
local id = tonumber(...)
local x, y = id, id * 2

spacenet.start(function()
	spacenet.dispatch("lua", function(_, _, cmd, dx, dy)
		if cmd == "move" then
			x = x + dx
			y = y + dy
			spacenet.ret(spacenet.pack(id, x, y))
		elseif cmd == "stop" then
			spacenet.ret(spacenet.pack(true))
			spacenet.exit()
		end
	end)
end)
`)},
		"service/scale_main.lua": &fstest.MapFile{Data: []byte(fmt.Sprintf(`
local spacenet = require "spacenet"

spacenet.start(function()
	local agents = {}
	for i = 1, %d do
		agents[i] = spacenet.newservice("scale_agent", i)
	end
	spacenet.error("SPAWN_1000_DONE")

	-- Fan out concurrent RPCs to all 1,000 isolated Lua VM actors
	local rpc_t0 = spacenet.hpc()
	local done = 0
	local main_co = coroutine.running()
	for i = 1, %d do
		spacenet.fork(function()
			local rid, rx, ry = spacenet.call(agents[i], "lua", "move", 3, 5)
			assert(rid == i and rx == i + 3 and ry == i * 2 + 5)
			done = done + 1
			if done == %d then
				spacenet.wakeup(main_co)
			end
		end)
	end
	if done < %d then
		spacenet.wait(main_co)
	end
	local rpc_ms = (spacenet.hpc() - rpc_t0) / 1e6
	spacenet.error(string.format("RPC_1000_DONE %%.2fms", rpc_ms))

	for i = 1, %d do
		spacenet.call(agents[i], "lua", "stop")
	end
	spacenet.error("STOP_1000_DONE")
	spacenet.exit()
end)
`, numAgents, numAgents, numAgents, numAgents, numAgents))},
	}

	stdruntime.GC()
	var before, after stdruntime.MemStats
	stdruntime.ReadMemStats(&before)

	logs := &syncBuf{}
	start := time.Now()
	node, err := spacenet.New(
		spacenet.WithThreads(8),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(all.Contribs()...),
		spacenet.WithFS(customFS),
		spacenet.WithStart("scale_main"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}

	waitLog(t, logs, "SPAWN_1000_DONE", 90*time.Second)
	spawnDur := time.Since(start)
	stdruntime.GC()
	stdruntime.ReadMemStats(&after)

	waitLog(t, logs, "RPC_1000_DONE", 90*time.Second)
	waitLog(t, logs, "STOP_1000_DONE", 90*time.Second)

	var rpcLine string
	for _, line := range strings.Split(logs.String(), "\n") {
		if idx := strings.Index(line, "RPC_1000_DONE "); idx >= 0 {
			rpcLine = strings.TrimSpace(line[idx+len("RPC_1000_DONE "):])
			break
		}
	}
	var deltaKB float64
	if after.HeapAlloc > before.HeapAlloc {
		deltaKB = float64(after.HeapAlloc-before.HeapAlloc) / 1024.0
	}
	t.Logf("1,000 Game Agents: spawn=%v (%.2f ms/agent), 1k concurrent RPC=%s, live HeapAlloc delta=%.1f MiB (%.1f KiB/agent)",
		spawnDur, float64(spawnDur.Milliseconds())/numAgents, rpcLine, deltaKB/1024.0, deltaKB/numAgents)
}
