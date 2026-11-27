package socket

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/runtime"
)

const metaNetpackQueue = "spacenet_netpack_queue"

type netpackItem struct {
	fd   int
	data []byte
}

type netpackUncomplete struct {
	fd     int
	header byte
	read   int // -1 means only 1 header byte read so far
	buf    []byte
}

type netpackQueue struct {
	items      []netpackItem
	uncomplete map[int]*netpackUncomplete
}

func newNetpackQueue() *netpackQueue {
	return &netpackQueue{
		uncomplete: make(map[int]*netpackUncomplete),
	}
}

func (q *netpackQueue) clear() {
	q.items = nil
	clear(q.uncomplete)
}

func (q *netpackQueue) push(fd int, payload []byte) {
	cp := make([]byte, len(payload))
	copy(cp, payload)
	q.items = append(q.items, netpackItem{fd: fd, data: cp})
}

func (q *netpackQueue) pushOwned(fd int, payload []byte) {
	q.items = append(q.items, netpackItem{fd: fd, data: payload})
}

func (q *netpackQueue) pushMore(fd int, buf []byte) {
	for len(buf) > 0 {
		if len(buf) == 1 {
			q.uncomplete[fd] = &netpackUncomplete{
				fd:     fd,
				read:   -1,
				header: buf[0],
			}
			return
		}
		packSize := int(binary.BigEndian.Uint16(buf[:2]))
		buf = buf[2:]
		if len(buf) < packSize {
			uc := &netpackUncomplete{
				fd:   fd,
				read: len(buf),
				buf:  make([]byte, packSize),
			}
			copy(uc.buf, buf)
			q.uncomplete[fd] = uc
			return
		}
		q.push(fd, buf[:packSize])
		buf = buf[packSize:]
	}
}

func preloadNetpack(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, err
	}
	node := actor.Node()

	tbl := s.NewTable(0, 6)
	_ = tbl.SetString("pop", s.NewFunc("netpack.pop", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) == 0 || args[0].IsNil() {
			return nil, nil
		}
		v, ok := st.CheckUserdata(args[0], metaNetpackQueue)
		if !ok {
			return nil, nil
		}
		q := v.(*netpackQueue)
		if len(q.items) == 0 {
			return nil, nil
		}
		item := q.items[0]
		q.items[0] = netpackItem{}
		q.items = q.items[1:]
		tok := node.Msgs().Alloc(item.data)
		return []moelua.Value{
			moelua.Int(int64(item.fd)),
			runtime.LightUDToken(tok),
			moelua.Int(int64(len(item.data))),
		}, nil
	}))

	_ = tbl.SetString("clear", s.NewFunc("netpack.clear", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) == 0 || args[0].IsNil() {
			return nil, nil
		}
		if v, ok := st.CheckUserdata(args[0], metaNetpackQueue); ok {
			v.(*netpackQueue).clear()
		}
		return nil, nil
	}))

	_ = tbl.SetString("pack", s.NewFunc("netpack.pack", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) == 0 {
			return nil, errors.New("netpack.pack requires payload")
		}
		var data []byte
		if args[0].IsLightUD() {
			if len(args) < 2 {
				return nil, errors.New("netpack.pack(msg, sz) requires size")
			}
			sz := int(toInt(args[1]))
			tok, _ := runtime.ExtractToken(args[0])
			if tok != 0 {
				if b, ok := node.Msgs().Borrow(tok); ok {
					if sz <= len(b) {
						data = b[:sz]
					} else {
						data = b
					}
				} else if a, aerr := runtime.ActorFromState(st); aerr == nil {
					if b, _, ok := a.Borrow().Read(tok); ok {
						if sz <= len(b) {
							data = b[:sz]
						} else {
							data = b
						}
					}
				}
			}
		} else {
			str, ok := st.ToString(args[0])
			if !ok {
				return nil, errors.New("bad argument #1 to 'pack' (string or lightuserdata expected)")
			}
			data = []byte(str)
		}
		if len(data) >= 0x10000 {
			return nil, fmt.Errorf("Invalid size (too long) of data : %d", len(data))
		}
		buf := make([]byte, len(data)+2)
		binary.BigEndian.PutUint16(buf[:2], uint16(len(data)))
		copy(buf[2:], data)
		tok := node.Msgs().Alloc(buf)
		return []moelua.Value{runtime.LightUDToken(tok), moelua.Int(int64(len(buf)))}, nil
	}))

	// CRITICAL Skynet contract: netpack.tostring(msg, sz) FREES msg (unlike skynet.tostring)!
	_ = tbl.SetString("tostring", s.NewFunc("netpack.tostring", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 2 {
			return nil, errors.New("netpack.tostring requires (msg, sz)")
		}
		sz := int(toInt(args[1]))
		tok, ok := runtime.ExtractToken(args[0])
		if !ok || tok == 0 {
			return []moelua.Value{st.String("")}, nil
		}
		if data, ok := node.Msgs().Take(tok); ok {
			if sz <= len(data) {
				data = data[:sz]
			}
			return []moelua.Value{st.String(string(data))}, nil
		}
		if a, aerr := runtime.ActorFromState(st); aerr == nil {
			if data, _, ok := a.Borrow().Read(tok); ok {
				if sz <= len(data) {
					data = data[:sz]
				}
				return []moelua.Value{st.String(string(data))}, nil
			}
		}
		return []moelua.Value{st.String("")}, nil
	}))

	_ = tbl.SetString("filter", s.NewFunc("netpack.filter", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 3 {
			return nil, errors.New("netpack.filter requires (queue, msg, sz)")
		}
		a, err := runtime.ActorFromState(st)
		if err != nil {
			return nil, err
		}
		ev, err := extractSocketEvent(node, a, args[1])
		if err != nil {
			return nil, err
		}

		qVal := args[0]
		var q *netpackQueue
		if !qVal.IsNil() {
			if v, ok := st.CheckUserdata(qVal, metaNetpackQueue); ok {
				q = v.(*netpackQueue)
			}
		}
		ensureQueue := func() *netpackQueue {
			if q != nil {
				return q
			}
			q = newNetpackQueue()
			mt, _ := st.NewMetatable(metaNetpackQueue)
			qVal, _ = st.NewUserdataWithMeta(q, mt)
			return q
		}

		switch ev.Type {
		case SocketTypeData:
			fd := ev.ID
			buf := ev.Data
			if len(buf) == 0 {
				return []moelua.Value{qVal}, nil
			}
			var uc *netpackUncomplete
			if q != nil {
				uc = q.uncomplete[fd]
				if uc != nil {
					delete(q.uncomplete, fd)
				}
			}
			if uc != nil {
				if uc.read < 0 {
					packSize := (int(uc.header) << 8) | int(buf[0])
					buf = buf[1:]
					uc.buf = make([]byte, packSize)
					uc.read = 0
				}
				need := len(uc.buf) - uc.read
				if len(buf) < need {
					copy(uc.buf[uc.read:], buf)
					uc.read += len(buf)
					ensureQueue().uncomplete[fd] = uc
					return []moelua.Value{qVal}, nil
				}
				copy(uc.buf[uc.read:], buf[:need])
				buf = buf[need:]
				if len(buf) == 0 {
					tok := node.Msgs().Alloc(uc.buf)
					return []moelua.Value{
						qVal,
						st.String("data"),
						moelua.Int(int64(fd)),
						runtime.LightUDToken(tok),
						moelua.Int(int64(len(uc.buf))),
					}, nil
				}
				nq := ensureQueue()
				nq.pushOwned(fd, uc.buf)
				nq.pushMore(fd, buf)
				return []moelua.Value{qVal, st.String("more")}, nil
			}
			if len(buf) == 1 {
				ensureQueue().uncomplete[fd] = &netpackUncomplete{
					fd:     fd,
					read:   -1,
					header: buf[0],
				}
				return []moelua.Value{qVal}, nil
			}
			packSize := int(binary.BigEndian.Uint16(buf[:2]))
			buf = buf[2:]
			if len(buf) < packSize {
				uc := &netpackUncomplete{
					fd:   fd,
					read: len(buf),
					buf:  make([]byte, packSize),
				}
				copy(uc.buf, buf)
				ensureQueue().uncomplete[fd] = uc
				return []moelua.Value{qVal}, nil
			}
			if len(buf) == packSize {
				cp := make([]byte, packSize)
				copy(cp, buf)
				tok := node.Msgs().Alloc(cp)
				return []moelua.Value{
					qVal,
					st.String("data"),
					moelua.Int(int64(fd)),
					runtime.LightUDToken(tok),
					moelua.Int(int64(packSize)),
				}, nil
			}
			nq := ensureQueue()
			nq.push(fd, buf[:packSize])
			nq.pushMore(fd, buf[packSize:])
			return []moelua.Value{qVal, st.String("more")}, nil

		case SocketTypeConnect:
			return []moelua.Value{qVal, st.String("init"), moelua.Int(int64(ev.ID)), st.String(ev.Text), moelua.Int(int64(ev.UD))}, nil

		case SocketTypeClose:
			if q != nil {
				delete(q.uncomplete, ev.ID)
			}
			return []moelua.Value{qVal, st.String("close"), moelua.Int(int64(ev.ID))}, nil

		case SocketTypeAccept:
			return []moelua.Value{qVal, st.String("open"), moelua.Int(int64(ev.UD)), st.String(ev.Text)}, nil

		case SocketTypeError:
			if q != nil {
				delete(q.uncomplete, ev.ID)
			}
			return []moelua.Value{qVal, st.String("error"), moelua.Int(int64(ev.ID)), st.String(ev.Text)}, nil

		case SocketTypeWarning:
			return []moelua.Value{qVal, st.String("warning"), moelua.Int(int64(ev.ID)), moelua.Int(int64(ev.UD))}, nil

		default:
			return []moelua.Value{qVal}, nil
		}
	}))

	return []moelua.Value{tbl.Value()}, nil
}
