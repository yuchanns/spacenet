package crypt_test

import (
	"bytes"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/crypt"
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

func TestUpstreamCryptAndSHA1Vectors(t *testing.T) {
	customFS := fstest.MapFS{
		"service/crypt_suite.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local crypt = require "skynet.crypt"
local md5 = require "md5"

skynet.start(function()
	-- Run upstream testcrypt.lua assertions
	dofile("./test/testcrypt.lua")

	-- Verify DH64 key exchange, hmac64, hmac64_md5, hashkey, hex, base64, xor, md5
	local k1 = crypt.randomkey()
	local k2 = crypt.randomkey()
	local e1 = crypt.dhexchange(k1)
	local e2 = crypt.dhexchange(k2)
	assert(crypt.dhsecret(e2, k1) == crypt.dhsecret(e1, k2))

	local a = crypt.hashkey("alice")
	local b = crypt.hashkey("bob")
	local h1 = crypt.hmac64_md5(a, b)
	local m = md5.sum((a..b):rep(3))
	local h2 = crypt.xor_str(m:sub(1,8), m:sub(9,16))
	assert(h1 == h2)

	assert(crypt.hexdecode(crypt.hexencode("\x00\x01\xfe\xff")) == "\x00\x01\xfe\xff")
	assert(crypt.base64decode(crypt.base64encode("spacenet-moelua")) == "spacenet-moelua")
	assert(md5.sumhexa("hello") == "5d41402abc4b2a76b9719d911017c592")
	local enc = md5.crypt("secret payload", "mykey", "12345678")
	assert(md5.decrypt(enc, "mykey") == "secret payload")

	-- Run upstream 199-vector SHA1 + HMAC-SHA1 suite (test/testsha.lua)
	assert(loadfile("./test/testsha.lua"))()

	skynet.error("CRYPT_ALL_PASSED")
	skynet.exit()
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(2),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(crypt.New()),
		spacenet.WithFS(customFS),
		spacenet.WithFS(os.DirFS("../../third_party/skynet")),
		spacenet.WithStart("crypt_suite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "CRYPT_ALL_PASSED")
}
