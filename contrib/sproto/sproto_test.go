package sproto_test

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/sproto"
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

func TestSprotoEncodeDecodeRPCAndEmptyResponse(t *testing.T) {
	customFS := fstest.MapFS{
		"service/sp_test.lua": &fstest.MapFile{Data: []byte(`
local skynet = require "skynet"
local sproto = require "sproto"
local sprotoparser = require "sprotoparser"
local sprotoloader = require "sprotoloader"

skynet.start(function()
	local schema = sprotoparser.parse [[
.package {
	type 0 : integer
	session 1 : integer
}

.Phone {
	number 0 : string
	type 1 : integer
}

.Person {
	name 0 : string
	id 1 : integer
	email 2 : string
	phone 3 : *Phone
	scores 4 : *integer
	price 5 : integer(2)
	ratio 6 : double
	active 7 : boolean
	flags 8 : *boolean
	tags 9 : *string
	by_id 10 : *Phone(type)
}

foobar 1 {
	request {
		what 0 : string
	}
	response {
		ok 0 : boolean
		reply 1 : string
	}
}

ping 2 {
	response nil
}
]]

	-- Save & reload via sprotoloader (verifies per-node slot isolation)
	sprotoloader.save(schema, 1)
	local sp = sprotoloader.load(1)

	-- Full struct encode / pack / unpack / decode round-trip
	local person = {
		name = "Alice",
		id = 1000000000001, -- 64-bit integer
		email = "alice@spacenet.dev",
		phone = {
			{ number = "123-456", type = 1 },
			{ number = "789-012", type = 2 },
		},
		scores = { 10, -5, 1000000000002 },
		price = 19.95,
		ratio = 0.618,
		active = true,
		flags = { true, false, true },
		tags = { "hero", "mage" },
		by_id = {
			[1] = { number = "111", type = 1 },
			[2] = { number = "222", type = 2 },
		},
	}

	local bin = sp:encode("Person", person)
	local packed = sproto.pack(bin)
	local unpacked = sproto.unpack(packed)
	local decoded = sp:decode("Person", unpacked)
	local pdec = sp:pdecode("Person", sp:pencode("Person", person))
	assert(pdec.name == "Alice" and pdec.id == 1000000000001)

	assert(decoded.name == "Alice")
	assert(decoded.id == 1000000000001)
	assert(decoded.phone[2].number == "789-012")
	assert(decoded.scores[2] == -5 and decoded.scores[3] == 1000000000002)
	assert(math.abs(decoded.price - 19.95) < 0.001)
	assert(math.abs(decoded.ratio - 0.618) < 1e-9)
	assert(decoded.active == true)
	assert(decoded.flags[1] == true and decoded.flags[2] == false and decoded.flags[3] == true)
	assert(decoded.tags[2] == "mage")
	assert(decoded.by_id[1].number == "111" and decoded.by_id[2].number == "222")

	-- RPC host + attach (including empty response sentinel LightUDToken(0)!)
	local host = sp:host("package")
	local reqFn = host:attach(sp)

	local pkt1 = reqFn("foobar", { what = "hello" }, 1)
	local typ, name, args, respFn = host:dispatch(pkt1)
	assert(typ == "REQUEST" and name == "foobar" and args.what == "hello")
	local respPkt1 = respFn({ ok = true, reply = "world" })
	local rtyp, rses, rargs = host:dispatch(respPkt1)
	assert(rtyp == "RESPONSE" and rses == 1 and rargs.ok == true and rargs.reply == "world")

	-- Test 'response nil' (confirm protocol relying on truthy LightUDToken(0))
	local pkt2 = reqFn("ping", nil, 2)
	local t2, n2, _, respFn2 = host:dispatch(pkt2)
	assert(t2 == "REQUEST" and n2 == "ping" and respFn2 ~= nil)
	local respPkt2 = respFn2()
	local rt2, rs2 = host:dispatch(respPkt2)
	assert(rt2 == "RESPONSE" and rs2 == 2)

	-- Default table generation
	local def = sp:default("Person")
	assert(def.name == "" and def.id == 0 and def.active == false)

	skynet.error("SPROTO_ALL_OK")
	skynet.exit()
end)
`)},
	}

	logs := &syncBuf{}
	node, err := spacenet.New(
		spacenet.WithThreads(2),
		spacenet.WithLogOutput(logs),
		spacenet.WithContrib(sproto.New()),
		spacenet.WithFS(customFS),
		spacenet.WithStart("sp_test"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	if err := node.Start(); err != nil {
		t.Fatal(err)
	}
	waitLog(t, logs, "SPROTO_ALL_OK")
}
