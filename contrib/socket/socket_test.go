package socket_test

import (
	"bytes"
	"encoding/binary"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/socket"
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

func TestTCPEchoTransferPauseAndUDP(t *testing.T) {
	customFS := fstest.MapFS{
		"service/sock_suite.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local socket = require "skynet.socket"

local mode, arg1 = ...
if mode == "echo_agent" then
	local fd = tonumber(arg1)
	skynet.start(function()
		socket.start(fd)
		socket.write(fd, "WELCOME\n")
		while true do
			local line = socket.readline(fd, "\n")
			if not line then
				socket.close(fd)
				skynet.exit()
				return
			end
			socket.write(fd, "ECHO:" .. line .. "\n")
			if line == "BYE" then
				socket.close(fd)
				skynet.exit()
				return
			end
		end
	end)
else
	skynet.start(function()
		-- 1. TCP listen on ephemeral port 0 + transfer accepted fd to agent service
		local lid, ip, port = socket.listen("127.0.0.1", 0)
		assert(port > 0, "ephemeral port must be > 0")
		socket.start(lid, function(id, addr)
			skynet.newservice("sock_suite", "echo_agent", id)
		end)

		local cfd = assert(socket.open("127.0.0.1", port))
		assert(socket.readline(cfd, "\n") == "WELCOME")
		socket.write(cfd, "hello\n")
		assert(socket.readline(cfd, "\n") == "ECHO:hello")
		socket.pause(cfd)
		socket.write(cfd, "BYE\n")
		assert(socket.readline(cfd, "\n") == "ECHO:BYE")
		socket.close(cfd)
		socket.close(lid)

		-- 2. UDP echo (IPv4)
		local got = 0
		local done = coroutine.running()
		local srv = socket.udp(function(str, from)
			local host, p = socket.udp_address(from)
			assert(host == "127.0.0.1" and p > 0)
			socket.sendto(srv, from, "OK:" .. str)
		end, "127.0.0.1", 0)

		local info = socket.netstat()
		local udpPort
		for _, item in ipairs(info) do
			if item.id == srv then
				local _, p = item.type == "UDP" and nil, nil
			end
		end
		socket.close(srv)

		-- Explicit local port UDP test
		local srv2
		srv2 = socket.udp(function(str, from)
			socket.sendto(srv2, from, "PONG:" .. str)
		end, "127.0.0.1", 18765)

		local cli = socket.udp(function(str, from)
			assert(str == "PONG:ping")
			got = got + 1
			if got == 3 then
				skynet.wakeup(done)
			end
		end)
		socket.udp_connect(cli, "127.0.0.1", 18765)
		for i = 1, 3 do
			socket.write(cli, "ping")
		end
		skynet.wait(done)
		socket.close(cli)
		socket.close(srv2)

		skynet.error("SOCKET_SUITE_OK")
		skynet.exit()
	end)
end
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(socket.New()),
		spacenet.WithFS(customFS),
		spacenet.WithStart("sock_suite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "SOCKET_SUITE_OK")
}

func TestDNSResolverWithMockUDPServer(t *testing.T) {
	// Spin up an offline in-process mock UDP DNS server responding 127.0.0.42 to any A query.
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	dnsPort := pc.LocalAddr().(*net.UDPAddr).Port

	go func() {
		buf := make([]byte, 2048)
		for {
			n, addr, rerr := pc.ReadFrom(buf)
			if rerr != nil {
				return
			}
			if n < 12 {
				continue
			}
			// Build minimal valid DNS response copying question section.
			resp := make([]byte, 0, n+16)
			resp = append(resp, buf[0], buf[1])             // TID
			resp = append(resp, 0x81, 0x80)                 // QR=1, RD=1, RA=1
			resp = append(resp, 0x00, 0x01, 0x00, 0x01)     // QDCOUNT=1, ANCOUNT=1
			resp = append(resp, 0x00, 0x00, 0x00, 0x00)     // NSCOUNT=0, ARCOUNT=0
			resp = append(resp, buf[12:n]...)               // Question
			resp = append(resp, 0xc0, 0x0c)                 // Name ptr to offset 12
			resp = binary.BigEndian.AppendUint16(resp, 1)   // TYPE A
			resp = binary.BigEndian.AppendUint16(resp, 1)   // CLASS IN
			resp = binary.BigEndian.AppendUint32(resp, 60)  // TTL 60s
			resp = binary.BigEndian.AppendUint16(resp, 4)   // RDLENGTH 4
			resp = append(resp, 127, 0, 0, 42)              // 127.0.0.42
			_, _ = pc.WriteTo(resp, addr)
		}
	}()

	customFS := fstest.MapFS{
		"service/dns_test.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local dns = require "skynet.dns"

skynet.start(function()
	local port = tonumber(skynet.getenv("mock_dns_port"))
	dns.DEFAULT_HOSTS = nil
	dns.server("127.0.0.1", port)
	local ip, ips = dns.resolve("spacenet.example")
	assert(ip == "127.0.0.42", "unexpected ip: " .. tostring(ip))
	assert(ips[1] == "127.0.0.42")
	skynet.error("DNS_OK")
	skynet.exit()
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(2),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(socket.New()),
		spacenet.WithFS(customFS),
		spacenet.WithFS(os.DirFS("../../third_party/skynet")),
		spacenet.WithEnv("mock_dns_port", stringInt(dnsPort)),
		spacenet.WithStart("dns_test"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "DNS_OK")
}

func stringInt(n int) string {
	return net.JoinHostPort("", "")[:0] + itoa(n)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestNetpackAndGateServer(t *testing.T) {
	customFS := fstest.MapFS{
		"service/gate_test.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local socket = require "skynet.socket"
local netpack = require "skynet.netpack"

skynet.start(function()
	local msg, sz = netpack.pack("hello-frame")
	local raw = netpack.tostring(msg, sz)
	assert(#raw == 2 + #"hello-frame")
	assert(raw:sub(3) == "hello-frame")

	-- Launch Skynet's embedded gate service on port 18888 via gateserver.lua
	local gate = skynet.newservice("gate")
	skynet.call(gate, "lua", "open", {
		address = "127.0.0.1",
		port = 18888,
		maxclient = 64,
		nodelay = true,
	})
	skynet.error("GATE_OPEN_OK")
	skynet.exit()
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(socket.New()),
		spacenet.WithFS(customFS),
		spacenet.WithStart("gate_test"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "GATE_OPEN_OK")
}
