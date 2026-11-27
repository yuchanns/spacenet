package runtime

import (
	"strings"
	"sync"
)

// localHarborService implements the standalone (harbor=0) global-name routing
// subset of Skynet's "harbor" service (without legacy multi-node TCP harbor
// networking). This allows unmodified bootstrap.lua, cdummy.lua, datacenterd.lua,
// service_mgr.lua, and testterm.lua to register and address global names like
// "DATACENTER" and "SERVICE" with zero network dependencies in Core.
type localHarborService struct {
	mu      sync.Mutex
	names   map[string]uint32
	pending map[string][]Message
}

func newLocalHarborService() ServiceHandler {
	return &localHarborService{
		names:   make(map[string]uint32),
		pending: make(map[string][]Message),
	}
}

func (h *localHarborService) Init(actor *Actor, _ string) error {
	actor.node.setHarbor(h)
	return nil
}

func (h *localHarborService) Dispatch(actor *Actor, msg *Message) error {
	if msg.Type != PTypeHarbor {
		return nil
	}
	cmd := strings.TrimSpace(string(msg.Data))
	if strings.HasPrefix(cmd, "N ") {
		name := strings.TrimSpace(cmd[2:])
		if name == "" {
			return nil
		}
		h.mu.Lock()
		h.names[name] = msg.Source
		queued := h.pending[name]
		delete(h.pending, name)
		h.mu.Unlock()

		for _, qm := range queued {
			actor.node.pushMessage(msg.Source, qm)
		}
	}
	return nil
}

func (h *localHarborService) routeGlobal(n *Node, name string, msg Message) {
	h.mu.Lock()
	if dest, ok := h.names[name]; ok && dest != 0 {
		h.mu.Unlock()
		n.pushMessage(dest, msg)
		return
	}
	h.pending[name] = append(h.pending[name], msg)
	h.mu.Unlock()
}

func (h *localHarborService) Signal(_ *Actor, _ int) {}

func (h *localHarborService) Close(actor *Actor) {
	actor.node.clearHarbor(h)
	h.mu.Lock()
	h.names = nil
	h.pending = nil
	h.mu.Unlock()
}
