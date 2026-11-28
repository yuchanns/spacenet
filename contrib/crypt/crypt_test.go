package crypt_test

import (
	"bytes"
	"io/fs"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/crypt"
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

func TestNativeCryptAndMD5(t *testing.T) {
	customFS := fstest.MapFS{
		"service/crypt_native.lua": &fstest.MapFile{Data: []byte(`
local spacenet = require "spacenet"
local crypt = require "spacenet.crypt"
local md5 = require "md5"

spacenet.start(function()
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

	spacenet.error("CRYPT_NATIVE_OK")
	spacenet.exit()
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(2),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(crypt.New()),
		spacenet.WithFS(customFS),
		spacenet.WithStart("crypt_native"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "CRYPT_NATIVE_OK")
}

func TestUpstreamCryptAndSHA1Vectors(t *testing.T) {
	customFS := fstest.MapFS{
		"skynet_alias.lua": &fstest.MapFile{Data: []byte(skynetAliasPreload)},
		"service/crypt_suite.lua": &fstest.MapFile{Data: []byte(`
local spacenet = require "spacenet"

spacenet.start(function()
	-- Run unmodified upstream testcrypt.lua and 199-vector SHA1/HMAC-SHA1 suite via user preload alias.
	dofile("./test/testcrypt.lua")
	assert(loadfile("./test/testsha.lua"))()

	spacenet.error("CRYPT_ALL_PASSED")
	spacenet.exit()
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(2),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(crypt.New()),
		spacenet.WithFS(customFS),
		spacenet.WithFS(onlyDirsFS{base: os.DirFS("../../third_party/skynet"), dirs: []string{"test"}}),
		spacenet.WithEnv("preload", "./skynet_alias.lua"),
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
