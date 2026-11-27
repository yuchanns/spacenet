package socket_test

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"os"
	"strconv"
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

// TestGateClientResetReclaimsSlots verifies that TCP reset errors preserve the
// socket entry long enough for gateserver's ERROR -> socketdriver.shutdown(fd)
// to emit CLOSE and decrement client_number.
func TestGateClientResetReclaimsSlots(t *testing.T) {
	// Pick an ephemeral free port, then release it for gate to bind.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gatePort := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close()

	customFS := fstest.MapFS{
		"service/reset_gate.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local gateserver = require "snax.gateserver"

local handler = {}
local opens, closes = 0, 0
function handler.connect(fd, addr)
	opens = opens + 1
	gateserver.openclient(fd)
	skynet.error("GATE_CONN_" .. opens)
end
function handler.disconnect(fd)
	closes = closes + 1
	skynet.error("GATE_DISC_" .. closes)
end
function handler.message(fd, msg, sz)
	local netpack = require "skynet.netpack"
	netpack.tostring(msg, sz)
end
function handler.error(fd, msg)
	skynet.error("GATE_ERR:" .. tostring(msg))
end

gateserver.start(handler)
`)},
		"service/reset_main.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
skynet.start(function()
	local gate = skynet.newservice("reset_gate")
	skynet.call(gate, "lua", "open", {
		address = "127.0.0.1",
		port = tonumber(skynet.getenv("gate_port")),
		maxclient = 2,
		nodelay = true,
	})
	skynet.error("RESET_GATE_READY")
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(4),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(socket.New()),
		spacenet.WithFS(customFS),
		spacenet.WithEnv("gate_port", strconv.Itoa(gatePort)),
		spacenet.WithStart("reset_main"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "RESET_GATE_READY")

	// Trigger 4 sequential TCP resets against a gate with maxclient=2.
	// If client_number leaked on reset, connections 2..4 would be rejected.
	for i := 1; i <= 4; i++ {
		conn, derr := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(gatePort)), 2*time.Second)
		if derr != nil {
			t.Fatalf("dial %d: %v", i, derr)
		}
		waitLog(t, logs, "GATE_CONN_"+strconv.Itoa(i))
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
		_ = conn.Close()
		waitLog(t, logs, "GATE_DISC_"+strconv.Itoa(i))
	}
}

// TestWriteOverloadWarningAndRecovery verifies that exceeding the write queue
// warning threshold emits a positive WARNING followed by a recovery WARNING (size 0)
// once drained, and resets the threshold for subsequent overload episodes.
func TestWriteOverloadWarningAndRecovery(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	port := ln.Addr().(*net.TCPAddr).Port

	holdRead := make(chan struct{})
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		<-holdRead
		_, _ = io.Copy(io.Discard, conn)
	}()

	customFS := fstest.MapFS{
		"service/warn_test.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local socket = require "skynet.socket"

skynet.start(function()
	local port = tonumber(skynet.getenv("warn_port"))
	local id = assert(socket.open("127.0.0.1", port))
	local got_pos = 0
	local got_zero = 0
	local done = coroutine.running()
	socket.warning(id, function(fd, size)
		if size > 0 then
			got_pos = got_pos + 1
		elseif size == 0 then
			got_zero = got_zero + 1
			skynet.wakeup(done)
		end
	end)

	-- Episode 1: a single 128 KiB write deterministically exceeds defaultWarnSize (64 KiB).
	local chunk = string.rep("x", 128 * 1024)
	socket.write(id, chunk)
	skynet.error("WARN_ENQUEUED")
	if got_zero < 1 then
		skynet.wait(done)
	end
	assert(got_pos >= 1, "expected positive warning in episode 1")
	assert(got_zero >= 1, "expected zero recovery warning in episode 1")

	-- Episode 2: threshold must have reset so another 128 KiB write warns and recovers again.
	local prev_pos, prev_zero = got_pos, got_zero
	socket.write(id, chunk)
	if got_zero <= prev_zero then
		skynet.wait(done)
	end
	assert(got_pos > prev_pos, "expected positive warning after threshold reset")
	assert(got_zero > prev_zero, "expected zero recovery warning after threshold reset")
	socket.close(id)
	skynet.error("WARN_RECOVERY_OK")
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
		spacenet.WithEnv("warn_port", strconv.Itoa(port)),
		spacenet.WithStart("warn_test"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "WARN_ENQUEUED")
	close(holdRead)
	waitLog(t, logs, "WARN_RECOVERY_OK")
}

// TestBindNonZeroFDRejectedAndOrphanListenerRetired verifies both:
// 1. socketdriver.bind(nonzero) fails explicitly rather than returning a broken ID.
// 2. When a listener's owning actor exits, a subsequent accept closes the child
//    and retires the orphaned listener.
func TestBindNonZeroFDRejectedAndOrphanListenerRetired(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close()

	customFS := fstest.MapFS{
		"service/orphan_owner.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local socket = require "skynet.socket"
local driver = require "skynet.socketdriver"

skynet.start(function()
	local ok, err = pcall(driver.bind, 99)
	assert(not ok, "expected bind(99) to fail")
	skynet.error("BIND_NONZERO_ERR_OK")

	local port = tonumber(skynet.getenv("orphan_port"))
	local lid = socket.listen("127.0.0.1", port)
	socket.start(lid, function() end)
	skynet.error("ORPHAN_LISTENING")
	-- Exit without closing lid so the listener becomes orphaned.
	skynet.exit()
end)
`)},
		"service/orphan_main.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
skynet.start(function()
	skynet.newservice("orphan_owner")
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(2),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(socket.New()),
		spacenet.WithFS(customFS),
		spacenet.WithEnv("orphan_port", strconv.Itoa(port)),
		spacenet.WithStart("orphan_main"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "BIND_NONZERO_ERR_OK")
	waitLog(t, logs, "ORPHAN_LISTENING")
	// Wait briefly for orphan_owner to finish exiting.
	time.Sleep(150 * time.Millisecond)

	// Dialing the orphaned listener should trigger undeliverable ACCEPT, close the
	// accepted child immediately, and retire the listener.
	conn, derr := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 2*time.Second)
	if derr == nil {
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		var b [1]byte
		_, rerr := conn.Read(b[:])
		_ = conn.Close()
		if rerr == nil {
			t.Fatal("expected orphaned child connection to be closed")
		}
	}

	// Listener must now be retired, so another dial must fail.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c2, err2 := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), 200*time.Millisecond)
		if err2 != nil {
			return
		}
		_ = c2.Close()
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("orphaned listener remained open after undeliverable accept")
}

// TestUDPConnectCapturesPeerAtEnqueue verifies that calling udp_connect to switch
// destinations does not redirect datagrams that were already enqueued for the prior peer.
func TestUDPConnectCapturesPeerAtEnqueue(t *testing.T) {
	pcA, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pcA.Close()
	pcB, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pcB.Close()

	portA := pcA.LocalAddr().(*net.UDPAddr).Port
	portB := pcB.LocalAddr().(*net.UDPAddr).Port

	gotA := make(chan string, 16)
	gotB := make(chan string, 16)
	readLoop := func(pc net.PacketConn, ch chan string) {
		buf := make([]byte, 2048)
		for {
			n, _, rerr := pc.ReadFrom(buf)
			if rerr != nil {
				return
			}
			ch <- string(buf[:n])
		}
	}
	go readLoop(pcA, gotA)
	go readLoop(pcB, gotB)

	customFS := fstest.MapFS{
		"service/udp_cap.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local socket = require "skynet.socket"

skynet.start(function()
	local portA = tonumber(skynet.getenv("udp_port_a"))
	local portB = tonumber(skynet.getenv("udp_port_b"))
	local cli = socket.udp(function() end)
	socket.udp_connect(cli, "127.0.0.1", portA)
	for i = 1, 5 do
		socket.write(cli, "FOR_A_" .. i)
	end
	-- Switch peer immediately while the above writes are still queued in writerLoop.
	socket.udp_connect(cli, "127.0.0.1", portB)
	for i = 1, 5 do
		socket.write(cli, "FOR_B_" .. i)
	end
	skynet.sleep(20)
	socket.close(cli)
	skynet.error("UDP_CAP_DONE")
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
		spacenet.WithEnv("udp_port_a", strconv.Itoa(portA)),
		spacenet.WithEnv("udp_port_b", strconv.Itoa(portB)),
		spacenet.WithStart("udp_cap"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "UDP_CAP_DONE")

	drain := func(ch chan string, wantPrefix string, wantCount int) {
		t.Helper()
		seen := 0
		timeout := time.After(2 * time.Second)
		for seen < wantCount {
			select {
			case msg := <-ch:
				if !strings.HasPrefix(msg, wantPrefix) {
					t.Fatalf("misrouted datagram: got %q on peer expecting %q", msg, wantPrefix)
				}
				seen++
			case <-timeout:
				t.Fatalf("timed out waiting for %d datagrams with prefix %q (got %d)", wantCount, wantPrefix, seen)
			}
		}
	}
	drain(gotA, "FOR_A_", 5)
	drain(gotB, "FOR_B_", 5)
}
