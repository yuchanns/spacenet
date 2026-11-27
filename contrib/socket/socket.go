// Package socket provides Skynet-compatible TCP/UDP networking
// (skynet.socketdriver, skynet.netpack, skynet.socket, skynet.socketchannel,
// skynet.dns, snax.gateserver/msgserver/loginserver, and gate.lua).
package socket

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/contrib/crypt"
	"github.com/yuchanns/spacenet/runtime"
	"github.com/yuchanns/spacenet/runtime/contrib"
)

// Skynet socket message types (matching skynet-src/skynet_socket.h).
const (
	SocketTypeData    = 1
	SocketTypeConnect = 2
	SocketTypeClose   = 3
	SocketTypeAccept  = 4
	SocketTypeError   = 5
	SocketTypeUDP     = 6
	SocketTypeWarning = 7

	udpProtoIPv4 = 1
	udpProtoIPv6 = 2

	defaultWarnSize = 64 * 1024
)

const extKey = "contrib.socket.manager"

// SocketEvent is the typed Go payload carried by PTYPE_SOCKET messages.
type SocketEvent struct {
	Type    int
	ID      int
	UD      int
	Data    []byte
	Text    string
	UDPAddr []byte
}

// New returns the socket contrib. It declares contrib/crypt as a dependency so
// bundled modules (snax.loginserver, snax.msgserver) work out of the box.
func New() *contrib.Contrib {
	return contrib.New("socket",
		contrib.WithDeps(crypt.New()),
		contrib.WithInit(func(n *runtime.Node) (io.Closer, error) {
			mgr := newManager(n)
			n.SetExt(extKey, mgr)
			return mgr, nil
		}),
		contrib.WithFS(subFS),
		contrib.WithPreload("skynet.socketdriver", preloadSocketDriver),
		contrib.WithPreload("skynet.netpack", preloadNetpack),
	)
}

func managerFromState(s *moelua.State) (*manager, *runtime.Actor, error) {
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, nil, err
	}
	v := actor.Node().Ext(extKey)
	mgr, ok := v.(*manager)
	if !ok || mgr == nil {
		return nil, nil, errors.New("socket contrib not initialized on node")
	}
	return mgr, actor, nil
}

type sockKind int

const (
	kindTCP sockKind = iota + 1
	kindListen
	kindUDP
	kindBind
)

type writeItem struct {
	data []byte
	addr *net.UDPAddr // non-nil for unconnected UDP sendto
}

type sockEntry struct {
	id   int
	kind sockKind

	mu        sync.Mutex
	owner     uint32
	conn         net.Conn
	listener     net.Listener
	pktConn      net.PacketConn
	bindFile     *os.File
	bindPollable bool
	bindDone     chan struct{}
	udpPeer      *net.UDPAddr
	localAddr    string
	peerAddr     string

	started       bool
	readerRunning bool
	paused        bool
	pauseCond     *sync.Cond
	stopCh        chan struct{}
	readClosed    bool
	errored       bool
	closing       bool
	closed        bool
	closeSent     bool

	highQ    []writeItem
	lowQ     []writeItem
	wbSize   int64
	warnSize int64
	writing  bool

	statRead  atomic.Uint64
	statWrite atomic.Uint64
	statRTime atomic.Uint64
	statWTime atomic.Uint64
}

type manager struct {
	node   *runtime.Node
	nextID atomic.Int32

	mu      sync.RWMutex
	sockets map[int]*sockEntry
	closed  atomic.Bool

	stdinMu          sync.Mutex
	stdinEntry       *sockEntry
	stdinDone        chan struct{}
	stdinPumpFile    *os.File
	stdinPumpRunning bool
}

func newManager(n *runtime.Node) *manager {
	return &manager{
		node:    n,
		sockets: make(map[int]*sockEntry),
	}
}

func (m *manager) allocEntry(kind sockKind, owner uint32) *sockEntry {
	id := int(m.nextID.Add(1))
	e := &sockEntry{
		id:     id,
		kind:   kind,
		owner:  owner,
		stopCh: make(chan struct{}),
	}
	e.pauseCond = sync.NewCond(&e.mu)
	m.mu.Lock()
	m.sockets[id] = e
	m.mu.Unlock()
	return e
}

func (m *manager) get(id int) *sockEntry {
	m.mu.RLock()
	e := m.sockets[id]
	m.mu.RUnlock()
	return e
}

func (m *manager) remove(id int) {
	m.mu.Lock()
	delete(m.sockets, id)
	m.mu.Unlock()
}

func (m *manager) Close() error {
	if !m.closed.CompareAndSwap(false, true) {
		return nil
	}
	m.mu.Lock()
	all := make([]*sockEntry, 0, len(m.sockets))
	for _, e := range m.sockets {
		all = append(all, e)
	}
	clear(m.sockets)
	m.mu.Unlock()

	for _, e := range all {
		e.forceClose(m, false)
	}
	return nil
}

func (m *manager) emit(owner uint32, ev *SocketEvent) bool {
	if m.closed.Load() || owner == 0 {
		return false
	}
	return m.node.PushTypedMessage(owner, runtime.Message{
		Source:  0,
		Session: 0,
		Type:    runtime.PTypeSocket,
		Payload: ev,
	})
}

// failTransport marks the entry errored, closes its underlying connection,
// drops queued writes, and emits SocketTypeError while keeping the entry
// registered so a subsequent shutdown/close from Lua can deliver SocketTypeClose
// (matching Skynet's socket_server.c). If the owner cannot be reached, the entry
// is retired immediately.
func (e *sockEntry) failTransport(m *manager, err error) {
	e.mu.Lock()
	if e.closed || e.closing || e.errored {
		e.mu.Unlock()
		return
	}
	e.errored = true
	conn := e.conn
	owner := e.owner
	e.highQ = nil
	e.lowQ = nil
	e.wbSize = 0
	e.warnSize = 0
	e.pauseCond.Broadcast()
	e.mu.Unlock()

	if conn != nil {
		_ = conn.Close()
	}
	if !m.emit(owner, &SocketEvent{
		Type: SocketTypeError,
		ID:   e.id,
		Text: err.Error(),
	}) {
		e.forceClose(m, false)
	}
}

// halfCloseRead reports remote read-side EOF while preserving the socket entry
// and underlying TCP connection so the owner can flush queued writes or write a
// final response before closing (matching Skynet's SOCKET_TYPE_HALFCLOSE_READ).
func (e *sockEntry) halfCloseRead(m *manager) {
	e.mu.Lock()
	if e.closed || e.readClosed {
		e.mu.Unlock()
		return
	}
	e.readClosed = true
	owner := e.owner
	sendClose := !e.closeSent
	if sendClose {
		e.closeSent = true
	}
	e.mu.Unlock()

	if sendClose {
		if !m.emit(owner, &SocketEvent{
			Type: SocketTypeClose,
			ID:   e.id,
		}) {
			e.forceClose(m, false)
		}
	}
}

func (e *sockEntry) forceClose(m *manager, notify bool) {
	e.mu.Lock()
	alreadyClosed := e.closed
	e.closed = true
	e.closing = true
	e.paused = false
	e.pauseCond.Broadcast()
	conn := e.conn
	ln := e.listener
	pc := e.pktConn
	bf := e.bindFile
	bp := e.bindPollable
	owner := e.owner
	sendClose := notify && !e.closeSent
	if sendClose {
		e.closeSent = true
	}
	e.highQ = nil
	e.lowQ = nil
	e.wbSize = 0
	e.mu.Unlock()

	if !alreadyClosed {
		close(e.stopCh)
		if conn != nil {
			_ = conn.Close()
		}
		if ln != nil {
			_ = ln.Close()
		}
		if pc != nil {
			_ = pc.Close()
		}
		if bf != nil && bp {
			_ = bf.SetReadDeadline(time.Now())
		}
		if e.kind == kindBind {
			m.stdinMu.Lock()
			if m.stdinEntry == e {
				m.stdinEntry = nil
			}
			m.stdinMu.Unlock()
		}
		m.remove(e.id)
	}
	if sendClose {
		m.emit(owner, &SocketEvent{
			Type: SocketTypeClose,
			ID:   e.id,
		})
	}
}

func (e *sockEntry) closeGraceful(m *manager) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return
	}
	e.closing = true
	if !e.writing && len(e.highQ) == 0 && len(e.lowQ) == 0 {
		e.mu.Unlock()
		e.forceClose(m, true)
		return
	}
	e.mu.Unlock()
}

func (e *sockEntry) enqueueWrite(m *manager, item writeItem, low bool) bool {
	e.mu.Lock()
	if e.closed || e.closing || e.errored {
		e.mu.Unlock()
		return false
	}
	if e.kind == kindUDP && item.addr == nil && e.udpPeer != nil {
		peerCopy := *e.udpPeer
		item.addr = &peerCopy
	}
	if low {
		e.lowQ = append(e.lowQ, item)
	} else {
		e.highQ = append(e.highQ, item)
	}
	e.wbSize += int64(len(item.data))
	var warnKB int
	if e.wbSize >= defaultWarnSize && e.wbSize >= e.warnSize {
		warnKB = int((e.wbSize + 1023) / 1024)
		if e.warnSize == 0 {
			e.warnSize = defaultWarnSize * 2
		} else {
			for e.warnSize <= e.wbSize {
				e.warnSize *= 2
			}
		}
	}
	owner := e.owner
	spawn := false
	if !e.writing {
		e.writing = true
		spawn = true
	}
	e.mu.Unlock()

	if warnKB > 0 {
		m.emit(owner, &SocketEvent{
			Type: SocketTypeWarning,
			ID:   e.id,
			UD:   warnKB,
		})
	}
	if spawn {
		go e.writerLoop(m)
	}
	return true
}

func (e *sockEntry) writerLoop(m *manager) {
	for {
		e.mu.Lock()
		if e.closed || e.errored {
			e.writing = false
			closing := e.closing && !e.closed
			e.mu.Unlock()
			if closing {
				e.forceClose(m, true)
			}
			return
		}
		var item writeItem
		hasItem := false
		if len(e.highQ) > 0 {
			item = e.highQ[0]
			e.highQ[0] = writeItem{}
			e.highQ = e.highQ[1:]
			hasItem = true
		} else if len(e.lowQ) > 0 {
			item = e.lowQ[0]
			e.lowQ[0] = writeItem{}
			e.lowQ = e.lowQ[1:]
			hasItem = true
		}
		if !hasItem {
			e.writing = false
			closing := e.closing
			emitRecovery := !closing && !e.errored && e.warnSize > 0
			if emitRecovery {
				e.warnSize = 0
			}
			owner := e.owner
			e.mu.Unlock()
			if emitRecovery {
				m.emit(owner, &SocketEvent{
					Type: SocketTypeWarning,
					ID:   e.id,
					UD:   0,
				})
			}
			if closing {
				e.forceClose(m, true)
			}
			return
		}
		e.wbSize -= int64(len(item.data))
		if e.wbSize < 0 {
			e.wbSize = 0
		}
		conn := e.conn
		pc := e.pktConn
		e.mu.Unlock()

		var n int
		var err error
		if e.kind == kindUDP {
			if pc != nil && item.addr != nil {
				n, err = pc.WriteTo(item.data, item.addr)
			} else if conn != nil {
				n, err = conn.Write(item.data)
			} else {
				err = errors.New("udp socket not bound or connected")
			}
		} else if conn != nil {
			n, err = conn.Write(item.data)
		} else {
			err = errors.New("socket not connected")
		}

		if n > 0 {
			e.statWrite.Add(uint64(n))
			e.statWTime.Store(m.node.Timer().Now())
		}
		if err != nil && e.kind != kindUDP {
			e.mu.Lock()
			e.writing = false
			closed := e.closed
			errored := e.errored
			closing := e.closing
			e.mu.Unlock()
			if closing && !closed {
				e.forceClose(m, true)
			} else if !closed && !errored {
				e.failTransport(m, err)
			}
			return
		}
	}
}

func (e *sockEntry) startTCPReader(m *manager) {
	e.mu.Lock()
	if e.closed || e.errored || e.readerRunning || e.conn == nil {
		e.mu.Unlock()
		return
	}
	e.readerRunning = true
	conn := e.conn
	e.mu.Unlock()

	go func() {
		buf := make([]byte, 32*1024)
		for {
			e.mu.Lock()
			for e.paused && !e.closed && !e.errored {
				e.pauseCond.Wait()
			}
			if e.closed || e.errored {
				e.mu.Unlock()
				return
			}
			e.mu.Unlock()

			n, err := conn.Read(buf)
			e.mu.Lock()
			owner := e.owner
			closed := e.closed
			errored := e.errored
			closing := e.closing
			writing := e.writing
			e.mu.Unlock()

			if n > 0 && !closed && !errored {
				e.statRead.Add(uint64(n))
				e.statRTime.Store(m.node.Timer().Now())
				cp := make([]byte, n)
				copy(cp, buf[:n])
				if !m.emit(owner, &SocketEvent{
					Type: SocketTypeData,
					ID:   e.id,
					UD:   n,
					Data: cp,
				}) {
					e.forceClose(m, false)
					return
				}
			}
			if err != nil {
				if closed || errored {
					return
				}
				if closing {
					if !writing {
						e.forceClose(m, true)
					}
					return
				}
				if errors.Is(err, io.EOF) {
					e.halfCloseRead(m)
				} else {
					e.failTransport(m, err)
				}
				return
			}
		}
	}()
}

func (e *sockEntry) startAcceptLoop(m *manager) {
	e.mu.Lock()
	if e.closed || e.readerRunning || e.listener == nil {
		e.mu.Unlock()
		return
	}
	e.readerRunning = true
	ln := e.listener
	e.mu.Unlock()

	go func() {
		var tempDelay time.Duration
		for {
			conn, err := ln.Accept()
			e.mu.Lock()
			owner := e.owner
			closing := e.closing || e.closed
			e.mu.Unlock()
			if err != nil {
				if closing || errors.Is(err, net.ErrClosed) {
					e.forceClose(m, true)
					return
				}
				if !m.emit(owner, &SocketEvent{
					Type: SocketTypeError,
					ID:   e.id,
					Text: err.Error(),
				}) {
					e.forceClose(m, false)
					return
				}
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
					if tempDelay > time.Second {
						tempDelay = time.Second
					}
				}
				timer := time.NewTimer(tempDelay)
				select {
				case <-timer.C:
				case <-e.stopCh:
					timer.Stop()
					return
				}
				continue
			}
			tempDelay = 0
			e.statRead.Add(1)
			e.statRTime.Store(m.node.Timer().Now())

			child := m.allocEntry(kindTCP, owner)
			child.conn = conn
			child.peerAddr = conn.RemoteAddr().String()
			child.paused = true // 0 reader goroutines until driver.start(newid)!

			if !m.emit(owner, &SocketEvent{
				Type: SocketTypeAccept,
				ID:   e.id,
				UD:   child.id,
				Text: child.peerAddr,
			}) {
				child.forceClose(m, false)
				e.forceClose(m, false)
				return
			}
		}
	}()
}

func (e *sockEntry) startUDPReader(m *manager) {
	e.mu.Lock()
	if e.closed || e.readerRunning || e.pktConn == nil {
		e.mu.Unlock()
		return
	}
	e.readerRunning = true
	pc := e.pktConn
	e.mu.Unlock()

	go func() {
		buf := make([]byte, 65536)
		for {
			n, addr, err := pc.ReadFrom(buf)
			e.mu.Lock()
			owner := e.owner
			closing := e.closing || e.closed
			e.mu.Unlock()
			if err != nil {
				if !closing {
					e.forceClose(m, true)
				}
				return
			}
			e.statRead.Add(uint64(n))
			e.statRTime.Store(m.node.Timer().Now())
			cp := make([]byte, n)
			copy(cp, buf[:n])
			var encodedAddr []byte
			if uaddr, ok := addr.(*net.UDPAddr); ok {
				encodedAddr = encodeUDPAddr(uaddr)
			}
			if !m.emit(owner, &SocketEvent{
				Type:    SocketTypeUDP,
				ID:      e.id,
				UD:      n,
				Data:    cp,
				UDPAddr: encodedAddr,
			}) {
				e.forceClose(m, false)
				return
			}
		}
	}()
}

func encodeUDPAddr(u *net.UDPAddr) []byte {
	if ip4 := u.IP.To4(); ip4 != nil {
		out := make([]byte, 1+2+4)
		out[0] = udpProtoIPv4
		binary.BigEndian.PutUint16(out[1:3], uint16(u.Port))
		copy(out[3:7], ip4)
		return out
	}
	ip16 := u.IP.To16()
	out := make([]byte, 1+2+16)
	out[0] = udpProtoIPv6
	binary.BigEndian.PutUint16(out[1:3], uint16(u.Port))
	copy(out[3:19], ip16)
	return out
}

func decodeUDPAddr(raw []byte) (*net.UDPAddr, error) {
	if len(raw) == 1+2+4 {
		port := int(binary.BigEndian.Uint16(raw[1:3]))
		ip := make(net.IP, 4)
		copy(ip, raw[3:7])
		return &net.UDPAddr{IP: ip, Port: port}, nil
	}
	if len(raw) == 1+2+16 {
		port := int(binary.BigEndian.Uint16(raw[1:3]))
		ip := make(net.IP, 16)
		copy(ip, raw[3:19])
		return &net.UDPAddr{IP: ip, Port: port}, nil
	}
	return nil, errors.New("Invalid udp address")
}

func extractSocketEvent(node *runtime.Node, actor *runtime.Actor, v moelua.Value) (*SocketEvent, error) {
	tok, ok := runtime.ExtractToken(v)
	if !ok || tok == 0 {
		return nil, errors.New("invalid socket message token")
	}
	kind, _, _ := runtime.SplitToken(tok)
	switch kind {
	case runtime.KindDispatchSlot:
		if _, payload, ok := actor.Borrow().Read(tok); ok {
			if ev, ok := payload.(*SocketEvent); ok {
				return ev, nil
			}
		}
	case runtime.KindSocketEvt:
		if payload, ok := node.Objs().Get(runtime.KindSocketEvt, tok); ok {
			if ev, ok := payload.(*SocketEvent); ok {
				return ev, nil
			}
		}
	}
	return nil, errors.New("not a socket event")
}

func toInt(v moelua.Value) int64 {
	if v.IsInt() {
		return v.Int()
	}
	if v.IsFloat() {
		return int64(v.Float())
	}
	return 0
}

func toIntState(s *moelua.State, v moelua.Value) int64 {
	if v.IsInt() {
		return v.Int()
	}
	if v.IsFloat() {
		return int64(v.Float())
	}
	if str, ok := s.ToString(v); ok {
		if iv, err := strconv.ParseInt(strings.TrimSpace(str), 10, 64); err == nil {
			return iv
		}
	}
	return 0
}

func parseHostPort(s *moelua.State, args []moelua.Value, addrIdx, portIdx int) (string, int, error) {
	if addrIdx >= len(args) {
		return "", 0, errors.New("missing address")
	}
	addr, ok := s.ToString(args[addrIdx])
	if !ok {
		return "", 0, errors.New("address must be a string")
	}
	if portIdx < len(args) && !args[portIdx].IsNil() {
		return addr, int(toIntState(s, args[portIdx])), nil
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("Invalid address %s.", addr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return "", 0, fmt.Errorf("Invalid port in address %s.", addr)
	}
	return host, port, nil
}

func extractSendPayload(node *runtime.Node, actor *runtime.Actor, s *moelua.State, args []moelua.Value, idx int) ([]byte, error) {
	if idx >= len(args) {
		return nil, errors.New("missing send buffer")
	}
	v := args[idx]
	switch {
	case v.IsString():
		str, _ := s.ToString(v)
		return []byte(str), nil
	case v.IsTable():
		tbl := s.TableOf(v)
		var out []byte
		for i := int64(1); ; i++ {
			elem := tbl.GetInt(i)
			if elem.IsNil() {
				break
			}
			str, ok := s.ToString(elem)
			if !ok {
				return nil, errors.New("Invalid strings table")
			}
			out = append(out, str...)
		}
		return out, nil
	case v.IsLightUD():
		tok, _ := runtime.ExtractToken(v)
		if tok == 0 {
			return nil, nil
		}
		sz := -1
		if idx+1 < len(args) && args[idx+1].IsNumber() {
			sz = int(toInt(args[idx+1]))
		}
		kind, _, _ := runtime.SplitToken(tok)
		switch kind {
		case runtime.KindMsgBuf:
			buf, ok := node.Msgs().Take(tok)
			if !ok {
				return nil, nil
			}
			if sz >= 0 && sz < len(buf) {
				return buf[:sz], nil
			}
			return buf, nil
		case runtime.KindDispatchSlot:
			if data, _, ok := actor.Borrow().Read(tok); ok {
				if sz >= 0 && sz < len(data) {
					data = data[:sz]
				}
				cp := make([]byte, len(data))
				copy(cp, data)
				return cp, nil
			}
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported send buffer type %s", v.TypeName())
	}
}

func preloadSocketDriver(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	mgr, _, err := managerFromState(s)
	if err != nil {
		return nil, err
	}

	tbl := s.NewTable(0, 30)
	funcs := map[string]moelua.GoFunc{
		"buffer":      mgr.lNewBuffer,
		"push":        mgr.lPushBuffer,
		"pop":         mgr.lPopBuffer,
		"drop":        mgr.lDrop,
		"readall":     mgr.lReadAll,
		"clear":       mgr.lClearBuffer,
		"readline":    mgr.lReadLine,
		"str2p":       mgr.lStr2P,
		"header":      mgr.lHeader,
		"info":        mgr.lInfo,
		"unpack":      mgr.lUnpack,
		"connect":     mgr.lConnect,
		"close":       mgr.lClose,
		"shutdown":    mgr.lShutdown,
		"listen":      mgr.lListen,
		"send":        mgr.lSend,
		"lsend":       mgr.lSendLow,
		"bind":        mgr.lBind,
		"start":       mgr.lStart,
		"pause":       mgr.lPause,
		"nodelay":     mgr.lNoDelay,
		"udp":         mgr.lUDP,
		"udp_connect": mgr.lUDPConnect,
		"udp_dial":    mgr.lUDPDial,
		"udp_listen":  mgr.lUDPListen,
		"udp_send":    mgr.lUDPSend,
		"udp_address": mgr.lUDPAddress,
		"resolve":     mgr.lResolve,
	}
	for name, fn := range funcs {
		_ = tbl.SetString(name, s.NewFunc("socketdriver."+name, fn))
	}
	return []moelua.Value{tbl.Value()}, nil
}

func (m *manager) lUnpack(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("unpack requires (msg, sz)")
	}
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, err
	}
	ev, err := extractSocketEvent(m.node, actor, args[0])
	if err != nil {
		return nil, err
	}

	var fourth moelua.Value
	if ev.Type == SocketTypeData || ev.Type == SocketTypeUDP {
		tok := m.node.Msgs().Alloc(ev.Data)
		fourth = runtime.LightUDToken(tok)
	} else {
		fourth = s.String(ev.Text)
	}

	if ev.Type == SocketTypeUDP && len(ev.UDPAddr) > 0 {
		return []moelua.Value{
			moelua.Int(int64(ev.Type)),
			moelua.Int(int64(ev.ID)),
			moelua.Int(int64(ev.UD)),
			fourth,
			s.String(string(ev.UDPAddr)),
		}, nil
	}
	return []moelua.Value{
		moelua.Int(int64(ev.Type)),
		moelua.Int(int64(ev.ID)),
		moelua.Int(int64(ev.UD)),
		fourth,
	}, nil
}

func (m *manager) lListen(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	_, actor, err := managerFromState(s)
	if err != nil {
		return nil, err
	}
	host, port, err := parseHostPort(s, args, 0, 1)
	if err != nil {
		return nil, err
	}
	if host == "" {
		host = "0.0.0.0"
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("Listen error: %w", err)
	}
	actualAddr := ln.Addr().(*net.TCPAddr)
	actualIP := actualAddr.IP.String()
	actualPort := actualAddr.Port

	e := m.allocEntry(kindListen, actor.Handle())
	e.listener = ln
	e.localAddr = actualIP

	// CRITICAL Skynet contract: emit CONNECT with actual IP & port immediately before driver.start!
	m.emit(actor.Handle(), &SocketEvent{
		Type: SocketTypeConnect,
		ID:   e.id,
		UD:   actualPort,
		Text: actualIP,
	})
	return []moelua.Value{moelua.Int(int64(e.id))}, nil
}

func (m *manager) lConnect(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	_, actor, err := managerFromState(s)
	if err != nil {
		return nil, err
	}
	host, port, err := parseHostPort(s, args, 0, 1)
	if err != nil {
		return nil, err
	}
	if port == 0 {
		return nil, errors.New("Invalid port")
	}
	e := m.allocEntry(kindTCP, actor.Handle())
	target := net.JoinHostPort(host, strconv.Itoa(port))
	owner := actor.Handle()

	go func() {
		conn, derr := net.Dial("tcp", target)
		if derr != nil {
			e.failTransport(m, derr)
			return
		}
		e.mu.Lock()
		if e.closed || e.closing || e.errored {
			e.mu.Unlock()
			_ = conn.Close()
			return
		}
		e.conn = conn
		e.peerAddr = conn.RemoteAddr().String()
		e.started = true
		e.mu.Unlock()

		if !m.emit(owner, &SocketEvent{
			Type: SocketTypeConnect,
			ID:   e.id,
			Text: e.peerAddr,
		}) {
			e.forceClose(m, false)
			return
		}
		e.startTCPReader(m)
	}()

	return []moelua.Value{moelua.Int(int64(e.id))}, nil
}

func (m *manager) lStart(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	_, actor, err := managerFromState(s)
	if err != nil {
		return nil, err
	}
	if len(args) < 1 {
		return nil, errors.New("start requires socket id")
	}
	id := int(toInt(args[0]))
	e := m.get(id)
	if e == nil {
		return nil, nil
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, nil
	}
	if e.errored || e.readClosed {
		e.mu.Unlock()
		m.emit(actor.Handle(), &SocketEvent{
			Type: SocketTypeError,
			ID:   e.id,
			Text: "socket closed",
		})
		return nil, nil
	}
	e.owner = actor.Handle()
	e.started = true
	wasPaused := e.paused
	e.paused = false
	e.pauseCond.Broadcast()
	kind := e.kind
	peer := e.peerAddr
	local := e.localAddr
	ln := e.listener
	e.mu.Unlock()

	switch kind {
	case kindListen:
		port := 0
		if ln != nil {
			if ta, ok := ln.Addr().(*net.TCPAddr); ok {
				port = ta.Port
			}
		}
		m.emit(actor.Handle(), &SocketEvent{
			Type: SocketTypeConnect,
			ID:   e.id,
			UD:   port,
			Text: local,
		})
		e.startAcceptLoop(m)
	case kindTCP:
		m.emit(actor.Handle(), &SocketEvent{
			Type: SocketTypeConnect,
			ID:   e.id,
			Text: peer,
		})
		if wasPaused || !e.readerRunning {
			e.startTCPReader(m)
		}
	case kindBind:
		m.emit(actor.Handle(), &SocketEvent{
			Type: SocketTypeConnect,
			ID:   e.id,
			Text: "bind",
		})
	}
	return nil, nil
}

func (m *manager) lPause(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("pause requires socket id")
	}
	e := m.get(int(toInt(args[0])))
	if e != nil {
		e.mu.Lock()
		e.paused = true
		e.mu.Unlock()
	}
	return nil, nil
}

func (m *manager) lClose(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("close requires socket id")
	}
	e := m.get(int(toInt(args[0])))
	if e != nil {
		e.closeGraceful(m)
	}
	return nil, nil
}

func (m *manager) lShutdown(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("shutdown requires socket id")
	}
	e := m.get(int(toInt(args[0])))
	if e != nil {
		e.forceClose(m, true)
	}
	return nil, nil
}

func (m *manager) lNoDelay(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, nil
	}
	e := m.get(int(toInt(args[0])))
	if e != nil {
		e.mu.Lock()
		conn := e.conn
		e.mu.Unlock()
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
	}
	return nil, nil
}

func (m *manager) lSend(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	return m.sendImpl(s, args, false)
}

func (m *manager) lSendLow(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	return m.sendImpl(s, args, true)
}

func (m *manager) sendImpl(s *moelua.State, args []moelua.Value, low bool) ([]moelua.Value, error) {
	_, actor, err := managerFromState(s)
	if err != nil {
		return nil, err
	}
	if len(args) < 2 {
		return nil, errors.New("send requires (id, data)")
	}
	id := int(toInt(args[0]))
	data, err := extractSendPayload(m.node, actor, s, args, 1)
	if err != nil {
		return nil, err
	}
	e := m.get(id)
	if e == nil {
		return []moelua.Value{moelua.Bool(false)}, nil
	}
	ok := e.enqueueWrite(m, writeItem{data: data}, low)
	return []moelua.Value{moelua.Bool(ok)}, nil
}

func (m *manager) lBind(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	_, actor, err := managerFromState(s)
	if err != nil {
		return nil, err
	}
	if len(args) < 1 {
		return nil, errors.New("bind requires os fd")
	}
	fd := int(toInt(args[0]))
	if fd != 0 {
		return nil, fmt.Errorf("socket.bind: binding os fd %d is not supported (only stdin fd 0 is supported)", fd)
	}
	stdin := os.Stdin
	if stdin == nil {
		return nil, errors.New("socket.bind: stdin is unavailable")
	}
	fi, statErr := stdin.Stat()
	if statErr != nil {
		return nil, fmt.Errorf("socket.bind: stdin is unreadable: %w", statErr)
	}

	// Retire any prior bound stdin entry and wait for a pollable reader to finish
	// before clearing the read deadline for the new socket.
	m.stdinMu.Lock()
	prev := m.stdinEntry
	prevDone := m.stdinDone
	m.stdinEntry = nil
	m.stdinDone = nil
	m.stdinMu.Unlock()
	if prev != nil {
		prev.forceClose(m, false)
	}
	if prevDone != nil {
		select {
		case <-prevDone:
		case <-time.After(500 * time.Millisecond):
		}
	}

	pollable := stdin.SetReadDeadline(time.Time{}) == nil
	e := m.allocEntry(kindBind, actor.Handle())
	e.bindFile = stdin
	e.bindPollable = pollable
	owner := actor.Handle()

	if !m.emit(owner, &SocketEvent{
		Type: SocketTypeConnect,
		ID:   e.id,
		Text: "bind",
	}) {
		e.forceClose(m, false)
		return []moelua.Value{moelua.Int(int64(e.id))}, nil
	}

	switch {
	case pollable:
		done := make(chan struct{})
		e.bindDone = done
		m.stdinMu.Lock()
		m.stdinEntry = e
		m.stdinDone = done
		m.stdinMu.Unlock()

		go func() {
			defer func() {
				_ = stdin.SetReadDeadline(time.Time{})
				close(done)
			}()
			buf := make([]byte, 4096)
			for {
				e.mu.Lock()
				closed := e.closed || e.errored
				e.mu.Unlock()
				if closed {
					return
				}
				n, rerr := stdin.Read(buf)
				e.mu.Lock()
				closed = e.closed || e.errored
				curOwner := e.owner
				e.mu.Unlock()
				if closed {
					return
				}
				if n > 0 {
					cp := make([]byte, n)
					copy(cp, buf[:n])
					if !m.emit(curOwner, &SocketEvent{
						Type: SocketTypeData,
						ID:   e.id,
						UD:   n,
						Data: cp,
					}) {
						e.forceClose(m, false)
						return
					}
				}
				if rerr != nil {
					if errors.Is(rerr, os.ErrDeadlineExceeded) {
						e.mu.Lock()
						closed = e.closed || e.errored
						e.mu.Unlock()
						if closed {
							return
						}
						continue
					}
					if errors.Is(rerr, io.EOF) {
						e.forceClose(m, true)
					} else {
						e.failTransport(m, rerr)
					}
					return
				}
			}
		}()

	case fi.Mode().IsRegular():
		m.stdinMu.Lock()
		m.stdinEntry = e
		m.stdinMu.Unlock()

		go func() {
			buf := make([]byte, 4096)
			for {
				e.mu.Lock()
				closed := e.closed || e.errored
				e.mu.Unlock()
				if closed {
					return
				}
				n, rerr := stdin.Read(buf)
				e.mu.Lock()
				closed = e.closed || e.errored
				curOwner := e.owner
				e.mu.Unlock()
				if closed {
					return
				}
				if n > 0 {
					cp := make([]byte, n)
					copy(cp, buf[:n])
					if !m.emit(curOwner, &SocketEvent{
						Type: SocketTypeData,
						ID:   e.id,
						UD:   n,
						Data: cp,
					}) {
						e.forceClose(m, false)
						return
					}
				}
				if rerr != nil {
					if errors.Is(rerr, io.EOF) {
						e.forceClose(m, true)
					} else {
						e.failTransport(m, rerr)
					}
					return
				}
			}
		}()

	default:
		// Non-pollable pipe or console (e.g. Windows anonymous pipe): run a single
		// dispatcher per *os.File so closing and reopening socket.stdin() never
		// leaves competing readers stealing input from the newer socket.
		m.stdinMu.Lock()
		m.stdinEntry = e
		startPump := !m.stdinPumpRunning || m.stdinPumpFile != stdin
		if startPump {
			m.stdinPumpRunning = true
			m.stdinPumpFile = stdin
		}
		m.stdinMu.Unlock()

		if startPump {
			go func(f *os.File) {
				defer func() {
					m.stdinMu.Lock()
					if m.stdinPumpFile == f {
						m.stdinPumpRunning = false
						m.stdinPumpFile = nil
					}
					m.stdinMu.Unlock()
				}()
				buf := make([]byte, 4096)
				for {
					if m.closed.Load() {
						return
					}
					n, rerr := f.Read(buf)
					if m.closed.Load() {
						return
					}
					m.stdinMu.Lock()
					cur := m.stdinEntry
					m.stdinMu.Unlock()
					if cur != nil && cur.bindFile == f {
						cur.mu.Lock()
						closed := cur.closed || cur.errored
						curOwner := cur.owner
						cur.mu.Unlock()
						if !closed && n > 0 {
							cp := make([]byte, n)
							copy(cp, buf[:n])
							if !m.emit(curOwner, &SocketEvent{
								Type: SocketTypeData,
								ID:   cur.id,
								UD:   n,
								Data: cp,
							}) {
								cur.forceClose(m, false)
							}
						}
						if !closed && rerr != nil {
							if errors.Is(rerr, io.EOF) {
								cur.forceClose(m, true)
							} else {
								cur.failTransport(m, rerr)
							}
						}
					}
					if rerr != nil {
						return
					}
				}
			}(stdin)
		}
	}
	return []moelua.Value{moelua.Int(int64(e.id))}, nil
}

func (m *manager) lUDP(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	_, actor, err := managerFromState(s)
	if err != nil {
		return nil, err
	}
	var host string
	var port int
	if len(args) >= 1 && !args[0].IsNil() {
		host, port, err = parseHostPort(s, args, 0, 1)
		if err != nil {
			return nil, err
		}
	}
	listenAddr := ":0"
	if host != "" || port != 0 {
		listenAddr = net.JoinHostPort(host, strconv.Itoa(port))
	}
	pc, err := net.ListenPacket("udp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("udp init failed: %w", err)
	}
	e := m.allocEntry(kindUDP, actor.Handle())
	e.pktConn = pc
	e.localAddr = pc.LocalAddr().String()
	e.startUDPReader(m)
	return []moelua.Value{moelua.Int(int64(e.id))}, nil
}

func (m *manager) lUDPConnect(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	_, actor, err := managerFromState(s)
	if err != nil {
		return nil, err
	}
	if len(args) < 2 {
		return nil, errors.New("udp_connect requires (id, addr [, port])")
	}
	id := int(toInt(args[0]))
	host, port, err := parseHostPort(s, args, 1, 2)
	if err != nil {
		return nil, err
	}
	raddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("udp connect failed: %w", err)
	}
	e := m.get(id)
	if e == nil || e.kind != kindUDP {
		return nil, errors.New("udp connect failed: invalid socket")
	}
	e.mu.Lock()
	e.owner = actor.Handle()
	e.udpPeer = raddr
	e.peerAddr = raddr.String()
	e.mu.Unlock()
	e.startUDPReader(m)
	return nil, nil
}

func (m *manager) lUDPDial(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	_, actor, err := managerFromState(s)
	if err != nil {
		return nil, err
	}
	host, port, err := parseHostPort(s, args, 0, 1)
	if err != nil {
		return nil, err
	}
	raddr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("udp dial host failed: %w", err)
	}
	network := "udp4"
	if raddr.IP != nil && raddr.IP.To4() == nil {
		network = "udp6"
	}
	pc, err := net.ListenPacket(network, ":0")
	if err != nil {
		return nil, fmt.Errorf("udp dial host failed: %w", err)
	}
	e := m.allocEntry(kindUDP, actor.Handle())
	e.pktConn = pc
	e.udpPeer = raddr
	e.localAddr = pc.LocalAddr().String()
	e.peerAddr = raddr.String()
	e.startUDPReader(m)
	return []moelua.Value{moelua.Int(int64(e.id))}, nil
}

func (m *manager) lUDPListen(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	_, actor, err := managerFromState(s)
	if err != nil {
		return nil, err
	}
	host, port, err := parseHostPort(s, args, 0, 1)
	if err != nil {
		return nil, err
	}
	pc, err := net.ListenPacket("udp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("udp listen host failed: %w", err)
	}
	e := m.allocEntry(kindUDP, actor.Handle())
	e.pktConn = pc
	e.localAddr = pc.LocalAddr().String()
	e.startUDPReader(m)
	return []moelua.Value{moelua.Int(int64(e.id))}, nil
}

func (m *manager) lUDPSend(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	_, actor, err := managerFromState(s)
	if err != nil {
		return nil, err
	}
	if len(args) < 3 {
		return nil, errors.New("udp_send requires (id, address, data)")
	}
	id := int(toInt(args[0]))
	addrStr, ok := s.ToString(args[1])
	if !ok {
		return nil, errors.New("bad argument #2 to 'udp_send' (string expected)")
	}
	target, err := decodeUDPAddr([]byte(addrStr))
	if err != nil {
		return nil, err
	}
	data, err := extractSendPayload(m.node, actor, s, args, 2)
	if err != nil {
		return nil, err
	}
	e := m.get(id)
	if e == nil || e.kind != kindUDP {
		return []moelua.Value{moelua.Bool(false)}, nil
	}
	okSend := e.enqueueWrite(m, writeItem{data: data, addr: target}, false)
	return []moelua.Value{moelua.Bool(okSend)}, nil
}

func (m *manager) lUDPAddress(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("udp_address requires address string")
	}
	raw, ok := s.ToString(args[0])
	if !ok {
		return nil, errors.New("Invalid udp address")
	}
	u, err := decodeUDPAddr([]byte(raw))
	if err != nil {
		return nil, err
	}
	return []moelua.Value{s.String(u.IP.String()), moelua.Int(int64(u.Port))}, nil
}

func (m *manager) lResolve(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("resolve requires host string")
	}
	host, ok := s.ToString(args[0])
	if !ok {
		return nil, errors.New("resolve requires host string")
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}
	tbl := s.NewTable(len(ips), 0)
	for i, ip := range ips {
		_ = tbl.SetInt(int64(i+1), s.String(ip.String()))
	}
	return []moelua.Value{tbl.Value()}, nil
}

func (m *manager) lInfo(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	m.mu.RLock()
	list := make([]*sockEntry, 0, len(m.sockets))
	for _, e := range m.sockets {
		list = append(list, e)
	}
	m.mu.RUnlock()

	out := s.NewTable(len(list), 0)
	for i, e := range list {
		e.mu.Lock()
		row := s.NewTable(0, 12)
		_ = row.SetString("id", moelua.Int(int64(e.id)))
		_ = row.SetString("address", moelua.Int(int64(e.owner)))
		switch {
		case e.closing:
			_ = row.SetString("type", s.String("CLOSING"))
		case e.kind == kindListen:
			_ = row.SetString("type", s.String("LISTEN"))
			_ = row.SetString("accept", moelua.Int(int64(e.statRead.Load())))
			_ = row.SetString("rtime", moelua.Int(int64(e.statRTime.Load())))
			if e.localAddr != "" {
				_ = row.SetString("sock", s.String(e.localAddr))
			}
			e.mu.Unlock()
			_ = out.SetInt(int64(i+1), row.Value())
			continue
		case e.kind == kindTCP:
			_ = row.SetString("type", s.String("TCP"))
		case e.kind == kindUDP:
			_ = row.SetString("type", s.String("UDP"))
		case e.kind == kindBind:
			_ = row.SetString("type", s.String("BIND"))
		default:
			_ = row.SetString("type", s.String("UNKNOWN"))
		}
		_ = row.SetString("read", moelua.Int(int64(e.statRead.Load())))
		_ = row.SetString("write", moelua.Int(int64(e.statWrite.Load())))
		_ = row.SetString("wbuffer", moelua.Int(e.wbSize))
		_ = row.SetString("rtime", moelua.Int(int64(e.statRTime.Load())))
		_ = row.SetString("wtime", moelua.Int(int64(e.statWTime.Load())))
		_ = row.SetString("reading", moelua.Bool(e.readerRunning && !e.paused))
		_ = row.SetString("writing", moelua.Bool(e.writing))
		if e.peerAddr != "" {
			_ = row.SetString("peer", s.String(e.peerAddr))
		}
		e.mu.Unlock()
		_ = out.SetInt(int64(i+1), row.Value())
	}
	return []moelua.Value{out.Value()}, nil
}

// unused import guard
var _ = strings.TrimSpace
