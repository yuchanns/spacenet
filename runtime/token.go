package runtime

import (
	"sync"
	"sync/atomic"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/moelua/vm"
)

// TokenKind identifies the subsystem owning a 64-bit LightUDTokenValue:
//
//	[8-bit Kind][24-bit Generation][32-bit Slot]
type TokenKind uint8

const (
	KindNull          TokenKind = 0x00
	KindMsgBuf        TokenKind = 0x01
	KindDispatchSlot  TokenKind = 0x02
	KindSocketEvt     TokenKind = 0x03
	KindMulticastPkg  TokenKind = 0x04
	KindShareMatrix   TokenKind = 0x05
	KindDatasheetDoc  TokenKind = 0x06
	KindShareDataNode TokenKind = 0x07
	KindSTMObject     TokenKind = 0x08
	KindDebugChan     TokenKind = 0x09
	KindSprotoObj     TokenKind = 0x0a
)

func MakeToken(kind TokenKind, gen uint32, slot uint32) uint64 {
	return (uint64(kind) << 56) | (uint64(gen&0xffffff) << 32) | uint64(slot)
}

func SplitToken(tok uint64) (kind TokenKind, gen uint32, slot uint32) {
	kind = TokenKind(tok >> 56)
	gen = uint32((tok >> 32) & 0xffffff)
	slot = uint32(tok)
	return
}

// LightUDToken wraps a 64-bit tagged token in a MoeLua lightuserdata Value.
// LightUDToken(0) is truthy in Lua, matching C lua_pushlightuserdata(L, NULL).
func LightUDToken(tok uint64) moelua.Value {
	return vm.LightUDTokenValue(tok)
}

// ExtractToken extracts the 64-bit token from a Lua lightuserdata Value.
func ExtractToken(v moelua.Value) (uint64, bool) {
	if !v.IsLightUD() {
		return 0, false
	}
	return uint64(v.LightUDAddr()), true
}

// msgShard holds a single shard of owned MsgBuf byte buffers.
const msgShardCount = 64

type msgSlot struct {
	gen  uint32
	data []byte
	used bool
}

type msgShard struct {
	mu    sync.Mutex
	slots []msgSlot
	free  []uint32
}

// MsgRegistry is a sharded, generation-checked registry for KindMsgBuf tokens.
// It enables safe zero-unsafe-pointer passing of Skynet (msg, sz) buffers across
// actors and Lua VMs while catching double-free / stale-token bugs under -race.
type MsgRegistry struct {
	shards [msgShardCount]msgShard
	next   atomic.Uint64
}

func NewMsgRegistry() *MsgRegistry {
	return &MsgRegistry{}
}

// Alloc registers buf (taking ownership of the slice) and returns a KindMsgBuf token.
func (r *MsgRegistry) Alloc(buf []byte) uint64 {
	shardIdx := uint32(r.next.Add(1) & (msgShardCount - 1))
	sh := &r.shards[shardIdx]
	sh.mu.Lock()
	var local uint32
	if n := len(sh.free); n > 0 {
		local = sh.free[n-1]
		sh.free = sh.free[:n-1]
		sh.slots[local].data = buf
		sh.slots[local].used = true
	} else {
		local = uint32(len(sh.slots))
		sh.slots = append(sh.slots, msgSlot{gen: 1, data: buf, used: true})
	}
	gen := sh.slots[local].gen
	sh.mu.Unlock()
	slot := (local << 6) | shardIdx
	return MakeToken(KindMsgBuf, gen, slot)
}

// AllocCopy copies src into a new owned buffer and returns its KindMsgBuf token.
func (r *MsgRegistry) AllocCopy(src []byte) uint64 {
	if len(src) == 0 {
		return r.Alloc(nil)
	}
	cp := make([]byte, len(src))
	copy(cp, src)
	return r.Alloc(cp)
}

// Borrow returns the underlying slice without releasing ownership.
func (r *MsgRegistry) Borrow(tok uint64) ([]byte, bool) {
	kind, gen, slot := SplitToken(tok)
	if kind != KindMsgBuf {
		return nil, false
	}
	shardIdx := slot & (msgShardCount - 1)
	local := slot >> 6
	sh := &r.shards[shardIdx]
	sh.mu.Lock()
	defer sh.mu.Unlock()
	if int(local) >= len(sh.slots) {
		return nil, false
	}
	s := &sh.slots[local]
	if !s.used || s.gen != gen {
		return nil, false
	}
	return s.data, true
}

// Take removes the token from the registry and returns its owned byte slice.
func (r *MsgRegistry) Take(tok uint64) ([]byte, bool) {
	kind, gen, slot := SplitToken(tok)
	if kind != KindMsgBuf {
		return nil, false
	}
	shardIdx := slot & (msgShardCount - 1)
	local := slot >> 6
	sh := &r.shards[shardIdx]
	sh.mu.Lock()
	if int(local) >= len(sh.slots) {
		sh.mu.Unlock()
		return nil, false
	}
	s := &sh.slots[local]
	if !s.used || s.gen != gen {
		sh.mu.Unlock()
		return nil, false
	}
	data := s.data
	s.data = nil
	s.used = false
	s.gen = (s.gen + 1) & 0xffffff
	if s.gen == 0 {
		s.gen = 1
	}
	sh.free = append(sh.free, local)
	sh.mu.Unlock()
	return data, true
}

// Free releases a KindMsgBuf token if still live.
func (r *MsgRegistry) Free(tok uint64) bool {
	_, ok := r.Take(tok)
	return ok
}

// ObjRegistry is a thread-safe generation-checked table for arbitrary Go objects
// tagged by TokenKind (used by socket, multicast, share, sproto, debug plugins).
type objSlot struct {
	kind TokenKind
	gen  uint32
	val  any
	used bool
}

type ObjRegistry struct {
	mu    sync.RWMutex
	slots []objSlot
	free  []uint32
}

func NewObjRegistry() *ObjRegistry {
	return &ObjRegistry{}
}

func (r *ObjRegistry) Put(kind TokenKind, val any) uint64 {
	r.mu.Lock()
	var slot uint32
	if n := len(r.free); n > 0 {
		slot = r.free[n-1]
		r.free = r.free[:n-1]
		r.slots[slot].kind = kind
		r.slots[slot].val = val
		r.slots[slot].used = true
	} else {
		slot = uint32(len(r.slots))
		r.slots = append(r.slots, objSlot{kind: kind, gen: 1, val: val, used: true})
	}
	gen := r.slots[slot].gen
	r.mu.Unlock()
	return MakeToken(kind, gen, slot)
}

func (r *ObjRegistry) Get(expectedKind TokenKind, tok uint64) (any, bool) {
	kind, gen, slot := SplitToken(tok)
	if kind != expectedKind {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if int(slot) >= len(r.slots) {
		return nil, false
	}
	s := &r.slots[slot]
	if !s.used || s.gen != gen || s.kind != expectedKind {
		return nil, false
	}
	return s.val, true
}

func (r *ObjRegistry) Delete(expectedKind TokenKind, tok uint64) (any, bool) {
	kind, gen, slot := SplitToken(tok)
	if kind != expectedKind {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if int(slot) >= len(r.slots) {
		return nil, false
	}
	s := &r.slots[slot]
	if !s.used || s.gen != gen || s.kind != expectedKind {
		return nil, false
	}
	val := s.val
	s.val = nil
	s.used = false
	s.gen = (s.gen + 1) & 0xffffff
	if s.gen == 0 {
		s.gen = 1
	}
	r.free = append(r.free, slot)
	return val, true
}

// DispatchBorrow holds per-actor transient borrow state active only during a
// single synchronous dispatch callback (zero global contention on hot RPCs).
type DispatchBorrow struct {
	mu      sync.Mutex
	gen     uint32
	active  bool
	data    []byte
	payload any
	actorID uint32
}

func (d *DispatchBorrow) Bind(actorID uint32, data []byte, payload any) uint64 {
	d.mu.Lock()
	d.gen = (d.gen + 1) & 0xffffff
	if d.gen == 0 {
		d.gen = 1
	}
	d.active = true
	d.data = data
	d.payload = payload
	d.actorID = actorID
	gen := d.gen
	d.mu.Unlock()
	return MakeToken(KindDispatchSlot, gen, actorID)
}

func (d *DispatchBorrow) Clear() {
	d.mu.Lock()
	d.active = false
	d.data = nil
	d.payload = nil
	d.mu.Unlock()
}

func (d *DispatchBorrow) Read(tok uint64) ([]byte, any, bool) {
	kind, gen, slot := SplitToken(tok)
	if kind != KindDispatchSlot {
		return nil, nil, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.active || d.gen != gen || d.actorID != slot {
		return nil, nil, false
	}
	return d.data, d.payload, true
}
