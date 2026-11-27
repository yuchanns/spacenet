package runtime

import (
	"testing"

	"github.com/yuchanns/moelua"
)

func TestHandleRegistryWraparound(t *testing.T) {
	reg := NewHandleRegistry(0)

	// Force handleIndex right near 24-bit wraparound boundary (0xfffffd).
	reg.handleIndex = 0xfffffd

	var handles []uint32
	for i := 0; i < 6; i++ {
		a := &Actor{}
		h := reg.Register(a)
		if h == 0 {
			t.Fatalf("handle %d unexpectedly 0", i)
		}
		handles = append(handles, h)
	}
	// Expect 0xfffffd, 0xfffffe, 0xffffff, 1, 2, 3 (never 0).
	want := []uint32{0xfffffd, 0xfffffe, 0xffffff, 1, 2, 3}
	for i, w := range want {
		if handles[i] != w {
			t.Fatalf("wraparound handle[%d] = 0x%x, want 0x%x", i, handles[i], w)
		}
	}

	// Verify Grab/Retire and slot table reuse under churn.
	for _, h := range handles {
		a := reg.Grab(h)
		if a == nil || a.handle != h {
			t.Fatalf("Grab(0x%x) failed", h)
		}
		if retired := reg.Retire(h); retired == nil {
			t.Fatalf("Retire(0x%x) returned nil", h)
		}
		if reg.Grab(h) != nil {
			t.Fatalf("Grab(0x%x) after Retire should be nil", h)
		}
	}

	// Churn 5000 sequential register/retire cycles; slot table should stay tiny.
	for i := 0; i < 5000; i++ {
		a := &Actor{}
		h := reg.Register(a)
		reg.Retire(h)
	}
	if len(reg.slots) > 32 {
		t.Fatalf("handle slot table grew unexpectedly under churn: %d", len(reg.slots))
	}
}

func TestMsgRegistryGenerationSafety(t *testing.T) {
	r := NewMsgRegistry()
	tok1 := r.AllocCopy([]byte("hello"))
	if buf, ok := r.Borrow(tok1); !ok || string(buf) != "hello" {
		t.Fatalf("Borrow failed: %q %v", string(buf), ok)
	}
	if buf, ok := r.Take(tok1); !ok || string(buf) != "hello" {
		t.Fatalf("Take failed: %q %v", string(buf), ok)
	}
	// Double free / stale token must be rejected.
	if _, ok := r.Borrow(tok1); ok {
		t.Fatal("stale token Borrow succeeded")
	}
	if r.Free(tok1) {
		t.Fatal("stale token Free succeeded")
	}
	// Reallocated slot must get a fresh generation.
	tok2 := r.AllocCopy([]byte("world"))
	if tok2 == tok1 {
		t.Fatalf("reallocated token collided with stale token: 0x%x", tok2)
	}
	if buf, ok := r.Take(tok2); !ok || string(buf) != "world" {
		t.Fatalf("Take(tok2) failed: %q %v", string(buf), ok)
	}
}

func TestSeriRoundTripAndPairs(t *testing.T) {
	s := moelua.New()
	defer s.Close()

	res, err := s.DoString(`
		local proxy = setmetatable({}, {
			__pairs = function()
				local data = { a = 1, b = "two", c = true }
				return next, data, nil
			end
		})
		return nil, true, false, 0, 42, -99999, 1000000000000, 3.14159, "short", string.rep("x", 100), { 10, 20, 30, k = "v" }, proxy
	`)
	if err != nil {
		t.Fatal(err)
	}
	packed, err := PackLuaValues(s, res)
	if err != nil {
		t.Fatal(err)
	}
	unpacked, err := UnpackLuaValues(s, packed)
	if err != nil {
		t.Fatal(err)
	}
	if len(unpacked) != len(res) {
		t.Fatalf("unpacked len = %d, want %d", len(unpacked), len(res))
	}
	if !unpacked[0].IsNil() || !unpacked[1].IsTrue() || !unpacked[2].IsFalse() {
		t.Fatalf("nil/bool mismatch")
	}
	if unpacked[3].Int() != 0 || unpacked[4].Int() != 42 || unpacked[5].Int() != -99999 || unpacked[6].Int() != 1000000000000 {
		t.Fatalf("int mismatch")
	}
	if unpacked[7].Float() != 3.14159 {
		t.Fatalf("float mismatch: %v", unpacked[7].Float())
	}
	if str, _ := s.ToString(unpacked[8]); str != "short" {
		t.Fatalf("short string mismatch: %q", str)
	}
	if str, _ := s.ToString(unpacked[9]); len(str) != 100 {
		t.Fatalf("long string mismatch: len=%d", len(str))
	}
	tbl := s.TableOf(unpacked[10])
	if tbl.GetInt(1).Int() != 10 || tbl.GetInt(3).Int() != 30 {
		t.Fatalf("table array mismatch")
	}
	if v, _ := s.ToString(tbl.GetString("k")); v != "v" {
		t.Fatalf("table hash mismatch")
	}
	proxyTbl := s.TableOf(unpacked[11])
	if proxyTbl.GetString("a").Int() != 1 || !proxyTbl.GetString("c").IsTrue() {
		t.Fatalf("__pairs table mismatch")
	}
}
