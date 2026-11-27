package runtime

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

const (
	handleMask     = 0xffffff
	handleMaxSlot  = 0x40000000
	defaultSlotCap = 16
)

// HandleRegistry manages 24-bit local actor handles plus local named services,
// matching skynet-src/skynet_handle.c's power-of-two masked slot table so
// sequential spawn/kill churn never inflates memory while handles wrap cleanly
// from 0xffffff back to 1 (never 0).
type HandleRegistry struct {
	mu          sync.RWMutex
	harbor      uint32 // shifted into top 8 bits
	handleIndex uint32
	slots       []*Actor
	names       map[string]uint32
}

func NewHandleRegistry(harbor uint8) *HandleRegistry {
	return &HandleRegistry{
		harbor:      uint32(harbor) << 24,
		handleIndex: 1,
		slots:       make([]*Actor, defaultSlotCap),
		names:       make(map[string]uint32),
	}
}

func (h *HandleRegistry) Harbor() uint8 {
	return uint8(h.harbor >> 24)
}

func (h *HandleRegistry) IsRemote(handle uint32) (harbor int, remote bool) {
	hb := handle & ^uint32(handleMask)
	return int(hb >> 24), hb != h.harbor && hb != 0
}

// Register assigns the next available 24-bit handle to actor.
func (h *HandleRegistry) Register(actor *Actor) uint32 {
	h.mu.Lock()
	defer h.mu.Unlock()
	for {
		slotCount := len(h.slots)
		for i := 0; i < slotCount; i++ {
			id := (i + int(h.handleIndex)) & handleMask
			if id == 0 {
				id = 1
			}
			hash := id & (slotCount - 1)
			if h.slots[hash] == nil {
				handle := uint32(id) | h.harbor
				h.slots[hash] = actor
				h.handleIndex = uint32((id + 1) & handleMask)
				if h.handleIndex == 0 {
					h.handleIndex = 1
				}
				actor.handle = handle
				return handle
			}
		}
		if slotCount*2 > handleMaxSlot {
			panic("spacenet: handle slot table exhausted")
		}
		newSlots := make([]*Actor, slotCount*2)
		for _, a := range h.slots {
			if a != nil {
				hash := int(a.handle) & (len(newSlots) - 1)
				newSlots[hash] = a
			}
		}
		h.slots = newSlots
	}
}

// Grab returns the live actor for handle with an incremented reference count,
// or nil if retired/missing. Callers must call actor.Release() when done.
func (h *HandleRegistry) Grab(handle uint32) *Actor {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if handle == 0 {
		return nil
	}
	hash := int(handle) & (len(h.slots) - 1)
	a := h.slots[hash]
	if a != nil && a.handle == handle {
		a.grab()
		return a
	}
	return nil
}

// Retire removes handle (and any names bound to it) and returns the retired actor
// (transferring the registry's reference to the caller, who must Release it).
func (h *HandleRegistry) Retire(handle uint32) *Actor {
	h.mu.Lock()
	defer h.mu.Unlock()
	if handle == 0 {
		return nil
	}
	hash := int(handle) & (len(h.slots) - 1)
	a := h.slots[hash]
	if a == nil || a.handle != handle {
		return nil
	}
	h.slots[hash] = nil
	for k, v := range h.names {
		if v == handle {
			delete(h.names, k)
		}
	}
	return a
}

// RetireAll removes and returns all currently registered non-reserved actors
// (or all actors including reserved ones when includeReserved is true),
// transferring each actor's registry reference to the caller.
func (h *HandleRegistry) RetireAll(includeReserved ...bool) []*Actor {
	all := len(includeReserved) > 0 && includeReserved[0]
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*Actor
	for i, a := range h.slots {
		if a != nil && (all || !a.reserved) {
			out = append(out, a)
			h.slots[i] = nil
			for k, v := range h.names {
				if v == a.handle {
					delete(h.names, k)
				}
			}
		}
	}
	if all {
		clear(h.names)
	}
	return out
}

// SnapshotAll returns all currently registered actors with incremented ref counts.
// Callers must Release each returned actor.
func (h *HandleRegistry) SnapshotAll() []*Actor {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*Actor, 0, 16)
	for _, a := range h.slots {
		if a != nil {
			a.grab()
			out = append(out, a)
		}
	}
	return out
}

// FindName looks up a local service name (without leading '.').
func (h *HandleRegistry) FindName(name string) uint32 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.names[name]
}

// NameHandle binds a local name (without leading '.') to handle if unused.
// Returns the registered name on success, or "" if already bound.
func (h *HandleRegistry) NameHandle(handle uint32, name string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.names[name]; exists {
		return ""
	}
	h.names[name] = handle
	return name
}

// FormatHandle formats handle as Skynet's ":xxxxxxxx" hex string.
func FormatHandle(handle uint32) string {
	return fmt.Sprintf(":%08x", handle)
}

// ParseHandle resolves ":hex" or ".localname" to a handle ID.
func (h *HandleRegistry) ParseHandle(s string) uint32 {
	if len(s) == 0 {
		return 0
	}
	if s[0] == ':' {
		v, err := strconv.ParseUint(strings.TrimSpace(s[1:]), 16, 32)
		if err != nil {
			return 0
		}
		return uint32(v)
	}
	if s[0] == '.' {
		return h.FindName(s[1:])
	}
	return 0
}
