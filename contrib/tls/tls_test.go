package tls_test

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/yuchanns/spacenet"
	tlsContrib "github.com/yuchanns/spacenet/contrib/tls"
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

func generateSelfSignedCert(t *testing.T) (string, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func TestTLSBIOHandshakeAndRoundTrip(t *testing.T) {
	certFile, keyFile := generateSelfSignedCert(t)

	customFS := fstest.MapFS{
		"service/tls_bio_test.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local ltls = require "ltls.c"

skynet.start(function()
	local cert = skynet.getenv("test_cert")
	local key = skynet.getenv("test_key")

	local sctx = ltls.newctx()
	sctx:set_cert(cert, key)
	local server = ltls.newtls("server", sctx)

	local cctx = ltls.newctx()
	local client = ltls.newtls("client", cctx, "localhost")

	-- Drive in-memory BIO handshake loop
	local c2s = client:handshake()
	local guard = 0
	while not (client:finished() and server:finished()) do
		guard = guard + 1
		assert(guard < 16, "handshake loop exceeded limit")
		local s2c
		if c2s and #c2s > 0 and not server:finished() then
			s2c = server:handshake(c2s)
		end
		c2s = nil
		if s2c and #s2c > 0 and not client:finished() then
			c2s = client:handshake(s2c)
		end
	end
	local sflush = server:write()
	if sflush and #sflush > 0 then
		client:read(sflush)
	end

	-- Bidirectional encrypted data exchange (including split records)
	local enc1 = client:write("Hello TLS from spacenet!")
	assert(#enc1 > 0)
	local half = math.floor(#enc1 / 2)
	local p1 = server:read(enc1:sub(1, half))
	local p2 = server:read(enc1:sub(half + 1))
	assert(p1 .. p2 == "Hello TLS from spacenet!")

	local enc2 = server:write("ACK:TLS13")
	assert(client:read(enc2) == "ACK:TLS13")

	client:close()
	server:close()
	skynet.error("TLS_BIO_OK")
	skynet.exit()
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(2),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(tlsContrib.New()),
		spacenet.WithFS(customFS),
		spacenet.WithEnv("enablessl", "true"),
		spacenet.WithEnv("test_cert", certFile),
		spacenet.WithEnv("test_key", keyFile),
		spacenet.WithStart("tls_bio_test"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), "TLS_BIO_OK") {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for TLS_BIO_OK\nlogs:\n%s", logs.String())
}

func TestPerNodeIsolationAndPreHandshakeGuards(t *testing.T) {
	sharedContrib := tlsContrib.New()

	// Node A calls ltls.init.c destructor; Node B reusing the same *contrib.Contrib
	// must remain initialized and functional, and read/write before handshake must fail.
	fsA := fstest.MapFS{
		"service/destructor.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local init = require "ltls.init.c"
skynet.start(function()
	init.destructor()
	skynet.error("NODE_A_DESTRUCTED")
	skynet.exit()
end)
`)},
	}
	logsA := &syncBuf{}
	nodeA, err := spacenet.New(
		spacenet.WithThreads(2),
		spacenet.WithLogOutput(logsA),
		spacenet.WithContrib(sharedContrib),
		spacenet.WithFS(fsA),
		spacenet.WithStart("destructor"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nodeA.Close()
	if err := nodeA.Start(); err != nil {
		t.Fatal(err)
	}
	deadlineA := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadlineA) && !strings.Contains(logsA.String(), "NODE_A_DESTRUCTED") {
		time.Sleep(15 * time.Millisecond)
	}
	if !strings.Contains(logsA.String(), "NODE_A_DESTRUCTED") {
		t.Fatalf("timeout waiting for NODE_A_DESTRUCTED\nlogs:\n%s", logsA.String())
	}

	fsB := fstest.MapFS{
		"service/check_b.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local ltls = require "ltls.c"
skynet.start(function()
	local ctx = ltls.newctx()
	local cli = ltls.newtls("client", ctx, "localhost")
	local okR = pcall(function() cli:read("") end)
	assert(not okR, "read before handshake must fail")
	local okW = pcall(function() cli:write("hi") end)
	assert(not okW, "write before handshake must fail")
	-- Start handshake without closing explicitly; Node.Close() must clean it up.
	local hs = cli:handshake()
	assert(hs and #hs > 0)
	skynet.error("NODE_B_OK")
	skynet.exit()
end)
`)},
	}
	logsB := &syncBuf{}
	nodeB, err := spacenet.New(
		spacenet.WithThreads(2),
		spacenet.WithLogOutput(logsB),
		spacenet.WithContrib(sharedContrib),
		spacenet.WithFS(fsB),
		spacenet.WithStart("check_b"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nodeB.Close()
	if err := nodeB.Start(); err != nil {
		t.Fatal(err)
	}
	deadlineB := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadlineB) && !strings.Contains(logsB.String(), "NODE_B_OK") {
		time.Sleep(15 * time.Millisecond)
	}
	if !strings.Contains(logsB.String(), "NODE_B_OK") {
		t.Fatalf("timeout waiting for NODE_B_OK\nlogs:\n%s", logsB.String())
	}
}
