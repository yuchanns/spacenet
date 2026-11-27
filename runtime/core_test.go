package runtime

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

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

type closeRecorder struct{ closed *atomic.Bool }

func (c closeRecorder) Close() error { c.closed.Store(true); return nil }

func TestInitContribFailureDoesNotDeadlock(t *testing.T) {
	var firstClosed atomic.Bool
	done := make(chan error, 1)
	go func() {
		_, err := NewNode(Config{
			LogOutput: io.Discard,
			InitHooks: []InitHook{
				{
					Name: "first",
					Fn: func(_ *Node) (io.Closer, error) {
						return closeRecorder{closed: &firstClosed}, nil
					},
				},
				{
					Name: "failing",
					Fn: func(_ *Node) (io.Closer, error) {
						return nil, errors.New("boom")
					},
				},
			},
		})
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected init error, got nil")
		}
		if !firstClosed.Load() {
			t.Fatal("expected first contrib closer to be called on partial init failure")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("NewNode deadlocked when contrib InitNode failed")
	}
}

func TestUnpackGoValuesRejectsNonComparableKey(t *testing.T) {
	s := moelua.New()
	defer s.Close()

	res, err := s.DoString(`return { [{1, 2}] = "val" }`)
	if err != nil {
		t.Fatal(err)
	}
	packed, err := PackLuaValues(s, res)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnpackGoValues(packed); err == nil {
		t.Fatal("expected error for non-comparable table key in UnpackGoValues")
	}
}

type dummyService struct {
	releaseCh chan struct{}
}

func (d *dummyService) Init(_ *Actor, _ string) error      { return nil }
func (d *dummyService) Dispatch(a *Actor, _ *Message) error {
	if d.releaseCh != nil {
		<-d.releaseCh
		a.Node().Kill(a, 0)
	}
	return nil
}
func (d *dummyService) Signal(_ *Actor, _ int) {}
func (d *dummyService) Close(_ *Actor)         {}

func TestWaitResetsAcrossSequentialLaunches(t *testing.T) {
	n, err := NewNode(Config{Threads: 2, LogOutput: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	ch1 := make(chan struct{})
	ch2 := make(chan struct{})
	n.svcMu.Lock()
	n.services["s1"] = func() ServiceHandler { return &dummyService{releaseCh: ch1} }
	n.services["s2"] = func() ServiceHandler { return &dummyService{releaseCh: ch2} }
	n.svcMu.Unlock()

	h1, err := n.Launch("s1", "")
	if err != nil {
		t.Fatal(err)
	}
	_ = n.Send(nil, 0, h1, PTypeText, 0, []byte("go"))
	close(ch1)
	ctx1, cancel1 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel1()
	if err := n.Wait(ctx1); err != nil {
		t.Fatalf("first Wait failed: %v", err)
	}

	// Launch a second service after activeActors reached 0; Wait must block until it exits.
	h2, err := n.Launch("s2", "")
	if err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() {
		ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel2()
		waitDone <- n.Wait(ctx2)
	}()

	select {
	case err := <-waitDone:
		t.Fatalf("second Wait returned prematurely while s2 was still active: %v", err)
	case <-time.After(80 * time.Millisecond):
	}

	_ = n.Send(nil, 0, h2, PTypeText, 0, []byte("go"))
	close(ch2)
	if err := <-waitDone; err != nil {
		t.Fatalf("second Wait failed: %v", err)
	}
}

func TestHarborDeliveryFailureToRetiredGlobalService(t *testing.T) {
	n, err := NewNode(Config{Threads: 2, LogOutput: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()

	n.svcMu.Lock()
	n.services["noop"] = func() ServiceHandler { return &dummyService{} }
	n.svcMu.Unlock()
	harborH, err := n.Launch("harbor", "")
	if err != nil {
		t.Fatal(err)
	}
	svcH, err := n.Launch("noop", "")
	if err != nil {
		t.Fatal(err)
	}
	// Register global name "GSVC" pointing to svcH.
	_ = n.Send(nil, svcH, harborH, PTypeHarbor, 0, []byte("N GSVC"))
	time.Sleep(50 * time.Millisecond)

	// Retire svcH; SendName to "GSVC" must return -1 instead of succeeding silently.
	n.Kill(nil, svcH)
	if ret := n.SendName(nil, 0, "GSVC", PTypeLua, 1, []byte("ping")); ret != -1 {
		t.Fatalf("SendName to retired global service returned %d, want -1", ret)
	}
}

func TestCallLuaUnblocksOnNodeClose(t *testing.T) {
	n, err := NewNode(Config{Threads: 2, LogOutput: io.Discard})
	if err != nil {
		t.Fatal(err)
	}
	// Service that consumes the message without replying.
	gotMsg := make(chan struct{}, 1)
	n.svcMu.Lock()
	n.services["silent"] = func() ServiceHandler {
		return &callbackService{onDispatch: func(_ *Actor, _ *Message) {
			select {
			case gotMsg <- struct{}{}:
			default:
			}
		}}
	}
	n.svcMu.Unlock()

	h, err := n.Launch("silent", "")
	if err != nil {
		t.Fatal(err)
	}
	n.handles.NameHandle(h, "silent")

	errCh := make(chan error, 1)
	go func() {
		_, cerr := n.CallLua(context.Background(), ".silent", "hello")
		errCh <- cerr
	}()

	select {
	case <-gotMsg:
	case <-time.After(2 * time.Second):
		t.Fatal("silent service did not receive message")
	}

	_ = n.Close()
	select {
	case cerr := <-errCh:
		if cerr == nil {
			t.Fatal("expected CallLua error on node close, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CallLua blocked after Node.Close()")
	}
}

type callbackService struct {
	onDispatch func(*Actor, *Message)
}

func (c *callbackService) Init(_ *Actor, _ string) error { return nil }
func (c *callbackService) Dispatch(a *Actor, m *Message) error {
	if c.onDispatch != nil {
		c.onDispatch(a, m)
	}
	return nil
}
func (c *callbackService) Signal(_ *Actor, _ int) {}
func (c *callbackService) Close(_ *Actor)         {}
