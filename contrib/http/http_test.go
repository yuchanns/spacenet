package http_test

import (
	"bytes"
	"io"
	"io/fs"
	stdhttp "net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/yuchanns/spacenet"
	httpContrib "github.com/yuchanns/spacenet/contrib/http"
)

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

func TestSpacenetHTTPDAndHTTPCAndWebSocketAndGoBridge(t *testing.T) {
	// Local Go httptest backend for http.httpc client check.
	goSrv := httptest.NewServer(stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.Header().Set("X-From", "go-httptest")
		_, _ = io.WriteString(w, "hello-from-go:"+r.URL.Path)
	}))
	defer goSrv.Close()

	customFS := fstest.MapFS{
		"service/web_suite.lua": &fstest.MapFile{Data: []byte(`
local spacenet = require "spacenet"
require "spacenet.manager"
local socket = require "spacenet.socket"
local httpd = require "http.httpd"
local httpc = require "http.httpc"
local sockethelper = require "http.sockethelper"
local urllib = require "http.url"
local websocket = require "http.websocket"

spacenet.start(function()
	-- 1. Register Go net/http.Handler bridge target
	spacenet.dispatch("lua", function(session, source, req)
		local cookie = req.headers and req.headers["cookie"] or ""
		spacenet.retpack(201, "lua-handled:" .. req.method .. ":" .. req.path .. ":" .. req.body .. ":" .. cookie, {
			["Content-Type"] = "text/plain",
			["X-Actor"] = "spacenet",
			["Set-Cookie"] = { "a=1", "b=2" },
		})
	end)
	spacenet.register(".web_bridge")

	-- Verify http.tlshelper loads cleanly via declared contrib/tls dependency
	local tlshelper = require "http.tlshelper"
	assert(type(tlshelper.newctx) == "function")

	-- 2. Test http.httpc against Go httptest server
	local goHost = spacenet.getenv("go_http_host")
	local status, body = httpc.get(goHost, "/ping")
	assert(status == 200, "unexpected status: " .. tostring(status))
	assert(body == "hello-from-go:/ping", "unexpected body: " .. tostring(body))

	-- 3. Test http.httpd server + http.httpc client loopback
	local lid, _, port = socket.listen("127.0.0.1", 0)
	socket.start(lid, function(id)
		socket.start(id)
		local code, url, method, header, reqbody = httpd.read_request(sockethelper.readfunc(id), 8192)
		if code == 200 then
			local path, query = urllib.parse(url)
			local q = query and urllib.parse_query(query) or {}
			httpd.write_response(sockethelper.writefunc(id), 200, "httpd:" .. path .. ":" .. (q.k or ""))
		end
		socket.close(id)
	end)
	local s2, b2 = httpc.get("127.0.0.1:" .. port, "/hello?k=world")
	assert(s2 == 200 and b2 == "httpd:/hello:world", tostring(s2) .. ":" .. tostring(b2))
	socket.close(lid)

	-- 4. Test http.websocket server + client end-to-end
	local wlid, _, wport = socket.listen("127.0.0.1", 0)
	socket.start(wlid, function(id)
		local handle = {
			message = function(wsid, msg, msg_type)
				websocket.write(wsid, "ws-echo:" .. msg, msg_type)
			end,
		}
		websocket.accept(id, handle, "ws")
	end)
	local ws = websocket.connect("ws://127.0.0.1:" .. wport .. "/chat")
	websocket.write(ws, "hi-spacenet")
	local resp = websocket.read(ws)
	assert(resp == "ws-echo:hi-spacenet", "unexpected ws resp: " .. tostring(resp))
	websocket.close(ws)
	socket.close(wlid)

	-- 5. Run unmodified upstream testwebsocket.lua framing tests (7 cases) against contrib/http's websocket.lua
	local orig_loadfile = loadfile
	local wsEnv = setmetatable({
		loadfile = function(path, mode, fenv)
			if path == "lualib/http/websocket.lua" and type(fenv) == "table" and type(fenv.require) == "function" then
				local inner_req = fenv.require
				fenv.require = function(name)
					if name == "spacenet" or name:sub(1, 9) == "spacenet." then
						return inner_req("skynet" .. name:sub(9))
					end
					return inner_req(name)
				end
			end
			return orig_loadfile(path, mode, fenv)
		end,
	}, { __index = _G })
	assert(orig_loadfile("./test/testwebsocket.lua", "t", wsEnv))()

	spacenet.error("WEB_SUITE_OK")
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(httpContrib.New()), // auto-pulls socket + crypt + tls
		spacenet.WithFS(customFS),
		spacenet.WithFS(onlyDirsFS{base: os.DirFS("../../third_party/skynet"), dirs: []string{"test"}}),
		spacenet.WithEnv("go_http_host", strings.TrimPrefix(goSrv.URL, "http://")),
		spacenet.WithStart("web_suite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "WEB_SUITE_OK")

	// 6. Test Go net/http.Handler bridge -> Lua actor (including multi-value headers & max body limit)
	bridgeSrv := httptest.NewServer(httpContrib.NewHandler(node, ".web_bridge", httpContrib.WithMaxBodyBytes(32)))
	defer bridgeSrv.Close()

	req, err := stdhttp.NewRequest(stdhttp.MethodPost, bridgeSrv.URL+"/api/items", strings.NewReader("payload42"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Add("Cookie", "session=abc")
	req.Header.Add("Cookie", "theme=dark")
	resp, err := stdhttp.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 201 {
		t.Fatalf("bridge status = %d, want 201", resp.StatusCode)
	}
	if resp.Header.Get("X-Actor") != "spacenet" {
		t.Fatalf("missing X-Actor header: %v", resp.Header)
	}
	if cookies := resp.Header.Values("Set-Cookie"); len(cookies) != 2 || cookies[0] != "a=1" || cookies[1] != "b=2" {
		t.Fatalf("unexpected Set-Cookie headers: %v", cookies)
	}
	if string(bodyBytes) != "lua-handled:POST:/api/items:payload42:session=abc; theme=dark" {
		t.Fatalf("unexpected bridge body: %q", string(bodyBytes))
	}

	// Oversized body must return 413 Request Entity Too Large.
	bigResp, err := stdhttp.Post(bridgeSrv.URL+"/api/big", "text/plain", strings.NewReader(strings.Repeat("a", 64)))
	if err != nil {
		t.Fatal(err)
	}
	defer bigResp.Body.Close()
	if bigResp.StatusCode != stdhttp.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body status = %d, want 413", bigResp.StatusCode)
	}
}
