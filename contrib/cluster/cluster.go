// Package cluster provides Skynet's skynet.cluster.core binary framing protocol
// and embedded cluster Lua services (skynet.cluster, clusterd, clusteragent,
// clustersender, clusterproxy).
package cluster

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/contrib/socket"
	"github.com/yuchanns/spacenet/runtime"
	"github.com/yuchanns/spacenet/runtime/contrib"
)

const multiPart = 0x8000

// New returns the cluster contrib, declaring a dependency on contrib/socket.
func New() *contrib.Contrib {
	return contrib.New("cluster",
		contrib.WithDeps(socket.New()),
		contrib.WithFS(subFS),
		contrib.WithPreload("skynet.cluster.core", preloadClusterCore),
	)
}

func preloadClusterCore(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, err
	}
	node := actor.Node()

	tbl := s.NewTable(0, 10)
	_ = tbl.SetString("packrequest", s.NewFunc("cluster.packrequest", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return packRequest(node, st, args, false)
	}))
	_ = tbl.SetString("packpush", s.NewFunc("cluster.packpush", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return packRequest(node, st, args, true)
	}))
	_ = tbl.SetString("packtrace", s.NewFunc("cluster.packtrace", lPackTrace))
	_ = tbl.SetString("unpackrequest", s.NewFunc("cluster.unpackrequest", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return unpackRequest(node, st, args)
	}))
	_ = tbl.SetString("packresponse", s.NewFunc("cluster.packresponse", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return packResponse(node, st, args)
	}))
	_ = tbl.SetString("unpackresponse", s.NewFunc("cluster.unpackresponse", lUnpackResponse))
	_ = tbl.SetString("append", s.NewFunc("cluster.append", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return lAppend(node, st, args)
	}))
	_ = tbl.SetString("concat", s.NewFunc("cluster.concat", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		return lConcat(node, st, args)
	}))
	_ = tbl.SetString("isname", s.NewFunc("cluster.isname", lIsName))
	_ = tbl.SetString("nodename", s.NewFunc("cluster.nodename", lNodeName))
	return []moelua.Value{tbl.Value()}, nil
}

func toIntArg(v moelua.Value) (int64, bool) {
	if v.IsInt() {
		return v.Int(), true
	}
	if v.IsFloat() {
		f := v.Float()
		if math.IsNaN(f) || math.IsInf(f, 0) || math.Trunc(f) != f {
			return 0, false
		}
		if f < -9223372036854775808.0 || f >= 9223372036854775808.0 {
			return 0, false
		}
		return int64(f), true
	}
	return 0, false
}

func putHeader(buf []byte, sz int) {
	buf[0] = byte(sz >> 8)
	buf[1] = byte(sz)
}

// packRequest ALWAYS takes & frees the msg lightuserdata (even on error),
// matching lualib-src/lua-cluster.c:packrequest.
func packRequest(node *runtime.Node, s *moelua.State, args []moelua.Value, isPush bool) ([]moelua.Value, error) {
	if len(args) < 4 {
		return nil, errors.New("packrequest requires (addr, session, msg, sz)")
	}
	tok, ok := runtime.ExtractToken(args[2])
	if !ok || tok == 0 {
		return nil, errors.New("Invalid request message")
	}
	// Always consume ownership of msg buffer before validating remaining args.
	msg, taken := node.Msgs().Take(tok)
	if !taken {
		return nil, errors.New("Invalid request message")
	}

	rawSz, ok := toIntArg(args[3])
	if !ok || rawSz < 0 || rawSz > math.MaxUint32 {
		return nil, fmt.Errorf("Invalid request size %v", args[3])
	}
	if rawSz > int64(len(msg)) {
		return nil, fmt.Errorf("Invalid request size %d (buffer len %d)", rawSz, len(msg))
	}
	msg = msg[:rawSz]
	sz := uint32(rawSz)

	rawSess, ok := toIntArg(args[1])
	if !ok || rawSess <= 0 || rawSess > math.MaxInt32 {
		return nil, fmt.Errorf("Invalid request session %d", rawSess)
	}
	session := uint32(rawSess)

	var firstPkt []byte
	var multiParts int

	if args[0].IsNumber() {
		rawAddr, ok := toIntArg(args[0])
		if !ok {
			return nil, errors.New("Invalid numeric cluster address")
		}
		addr := uint32(rawAddr)
		if sz < multiPart {
			firstPkt = make([]byte, int(sz)+11)
			putHeader(firstPkt, int(sz)+9)
			firstPkt[2] = 0
			binary.LittleEndian.PutUint32(firstPkt[3:7], addr)
			sessVal := session
			if isPush {
				sessVal = 0
			}
			binary.LittleEndian.PutUint32(firstPkt[7:11], sessVal)
			copy(firstPkt[11:], msg)
		} else {
			multiParts = int((sz-1)/multiPart + 1)
			firstPkt = make([]byte, 15)
			putHeader(firstPkt, 13)
			if isPush {
				firstPkt[2] = 0x41
			} else {
				firstPkt[2] = 1
			}
			binary.LittleEndian.PutUint32(firstPkt[3:7], addr)
			binary.LittleEndian.PutUint32(firstPkt[7:11], session)
			binary.LittleEndian.PutUint32(firstPkt[11:15], sz)
		}
	} else {
		if !args[0].IsString() {
			return nil, fmt.Errorf("name is not a string, it's a %s", args[0].TypeName())
		}
		name, _ := s.ToString(args[0])
		if len(name) < 1 || len(name) > 255 {
			return nil, fmt.Errorf("name length is invalid, must be between 1 and 255 characters: %s", name)
		}
		nlen := len(name)
		if sz < multiPart {
			firstPkt = make([]byte, int(sz)+8+nlen)
			putHeader(firstPkt, int(sz)+6+nlen)
			firstPkt[2] = 0x80
			firstPkt[3] = byte(nlen)
			copy(firstPkt[4:4+nlen], name)
			sessVal := session
			if isPush {
				sessVal = 0
			}
			binary.LittleEndian.PutUint32(firstPkt[4+nlen:8+nlen], sessVal)
			copy(firstPkt[8+nlen:], msg)
		} else {
			multiParts = int((sz-1)/multiPart + 1)
			firstPkt = make([]byte, 12+nlen)
			putHeader(firstPkt, 10+nlen)
			if isPush {
				firstPkt[2] = 0xc1
			} else {
				firstPkt[2] = 0x81
			}
			firstPkt[3] = byte(nlen)
			copy(firstPkt[4:4+nlen], name)
			binary.LittleEndian.PutUint32(firstPkt[4+nlen:8+nlen], session)
			binary.LittleEndian.PutUint32(firstPkt[8+nlen:12+nlen], sz)
		}
	}

	nextSess := session + 1
	if nextSess > math.MaxInt32 {
		nextSess = 1
	}

	if multiParts > 0 {
		tbl := s.NewTable(multiParts, 0)
		tblPin := s.Pin(tbl.Value())
		defer tblPin.Release()

		rem := msg
		for i := 0; i < multiParts; i++ {
			var chunk []byte
			var tag byte
			if len(rem) > multiPart {
				chunk = rem[:multiPart]
				rem = rem[multiPart:]
				tag = 2
			} else {
				chunk = rem
				rem = nil
				tag = 3
			}
			pkt := make([]byte, len(chunk)+7)
			putHeader(pkt, len(chunk)+5)
			pkt[2] = tag
			binary.LittleEndian.PutUint32(pkt[3:7], session)
			copy(pkt[7:], chunk)
			_ = tbl.SetInt(int64(i+1), s.String(string(pkt)))
		}
		firstVal := s.String(string(firstPkt))
		return []moelua.Value{firstVal, moelua.Int(int64(nextSess)), tbl.Value()}, nil
	}
	return []moelua.Value{s.String(string(firstPkt)), moelua.Int(int64(nextSess))}, nil
}

func lPackTrace(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("packtrace requires tag string")
	}
	if !args[0].IsString() {
		return nil, errors.New("bad argument #1 to 'packtrace' (string expected)")
	}
	tag, _ := s.ToString(args[0])
	if len(tag) > multiPart {
		return nil, fmt.Errorf("trace tag is too long : %d", len(tag))
	}
	buf := make([]byte, len(tag)+3)
	putHeader(buf, len(tag)+1)
	buf[2] = 4
	copy(buf[3:], tag)
	return []moelua.Value{s.String(string(buf))}, nil
}

func readInputBytes(node *runtime.Node, s *moelua.State, args []moelua.Value, idx int) ([]byte, error) {
	if idx >= len(args) {
		return nil, errors.New("missing input buffer")
	}
	v := args[idx]
	if v.IsString() {
		str, _ := s.ToString(v)
		return []byte(str), nil
	}
	if v.IsLightUD() {
		if idx+1 >= len(args) {
			return nil, errors.New("lightuserdata requires size argument")
		}
		rawSz, ok := toIntArg(args[idx+1])
		if !ok || rawSz < 0 {
			return nil, errors.New("invalid lightuserdata buffer size")
		}
		tok, _ := runtime.ExtractToken(v)
		if tok == 0 {
			if rawSz > 0 {
				return nil, errors.New("invalid lightuserdata buffer")
			}
			return nil, nil
		}
		kind, _, _ := runtime.SplitToken(tok)
		switch kind {
		case runtime.KindMsgBuf:
			if b, ok := node.Msgs().Borrow(tok); ok {
				if rawSz > int64(len(b)) {
					return nil, errors.New("lightuserdata size exceeds buffer length")
				}
				return b[:rawSz], nil
			}
		case runtime.KindDispatchSlot:
			if a, err := runtime.ActorFromState(s); err == nil {
				if b, _, ok := a.Borrow().Read(tok); ok {
					if rawSz > int64(len(b)) {
						return nil, errors.New("lightuserdata size exceeds buffer length")
					}
					return b[:rawSz], nil
				}
			}
		}
		return nil, errors.New("invalid lightuserdata buffer")
	}
	return nil, fmt.Errorf("expected string or lightuserdata, got %s", v.TypeName())
}

func unpackRequest(node *runtime.Node, s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	msg, err := readInputBytes(node, s, args, 0)
	if err != nil {
		return nil, err
	}
	sz := len(msg)
	if sz == 0 {
		return nil, errors.New("Invalid req package. size == 0")
	}
	switch msg[0] {
	case 0:
		if sz < 9 {
			return nil, fmt.Errorf("Invalid cluster message (size=%d)", sz)
		}
		addr := binary.LittleEndian.Uint32(msg[1:5])
		session := binary.LittleEndian.Uint32(msg[5:9])
		tok := node.Msgs().AllocCopy(msg[9:])
		if session == 0 {
			return []moelua.Value{
				moelua.Int(int64(addr)),
				moelua.Int(0),
				runtime.LightUDToken(tok),
				moelua.Int(int64(sz - 9)),
				moelua.Nil(),
				moelua.Bool(true),
			}, nil
		}
		return []moelua.Value{
			moelua.Int(int64(addr)),
			moelua.Int(int64(session)),
			runtime.LightUDToken(tok),
			moelua.Int(int64(sz - 9)),
		}, nil

	case 1, 0x41:
		if sz != 13 {
			return nil, fmt.Errorf("Invalid cluster message size %d (multi req must be 13)", sz)
		}
		addr := binary.LittleEndian.Uint32(msg[1:5])
		session := binary.LittleEndian.Uint32(msg[5:9])
		totalSz := binary.LittleEndian.Uint32(msg[9:13])
		isPush := msg[0] == 0x41
		return []moelua.Value{
			moelua.Int(int64(addr)),
			moelua.Int(int64(session)),
			moelua.Nil(),
			moelua.Int(int64(totalSz)),
			moelua.Bool(true),
			moelua.Bool(isPush),
		}, nil

	case 2, 3:
		if sz < 5 {
			return nil, errors.New("Invalid cluster multi part message")
		}
		padding := msg[0] == 2
		session := binary.LittleEndian.Uint32(msg[1:5])
		tok := node.Msgs().AllocCopy(msg[5:])
		return []moelua.Value{
			moelua.Bool(false),
			moelua.Int(int64(session)),
			runtime.LightUDToken(tok),
			moelua.Int(int64(sz - 5)),
			moelua.Bool(padding),
		}, nil

	case 4:
		return []moelua.Value{s.String(string(msg[1:]))}, nil

	case 0x80:
		if sz < 2 {
			return nil, fmt.Errorf("Invalid cluster message (size=%d)", sz)
		}
		namesz := int(msg[1])
		if sz < namesz+6 {
			return nil, fmt.Errorf("Invalid cluster message (size=%d)", sz)
		}
		name := string(msg[2 : 2+namesz])
		session := binary.LittleEndian.Uint32(msg[2+namesz : 6+namesz])
		payload := msg[6+namesz:]
		tok := node.Msgs().AllocCopy(payload)
		if session == 0 {
			return []moelua.Value{
				s.String(name),
				moelua.Int(0),
				runtime.LightUDToken(tok),
				moelua.Int(int64(len(payload))),
				moelua.Nil(),
				moelua.Bool(true),
			}, nil
		}
		return []moelua.Value{
			s.String(name),
			moelua.Int(int64(session)),
			runtime.LightUDToken(tok),
			moelua.Int(int64(len(payload))),
		}, nil

	case 0x81, 0xc1:
		if sz < 2 {
			return nil, fmt.Errorf("Invalid cluster message (size=%d)", sz)
		}
		namesz := int(msg[1])
		if sz < namesz+10 {
			return nil, fmt.Errorf("Invalid cluster message (size=%d)", sz)
		}
		name := string(msg[2 : 2+namesz])
		session := binary.LittleEndian.Uint32(msg[2+namesz : 6+namesz])
		totalSz := binary.LittleEndian.Uint32(msg[6+namesz : 10+namesz])
		isPush := msg[0] == 0xc1
		return []moelua.Value{
			s.String(name),
			moelua.Int(int64(session)),
			moelua.Nil(),
			moelua.Int(int64(totalSz)),
			moelua.Bool(true),
			moelua.Bool(isPush),
		}, nil

	default:
		return nil, fmt.Errorf("Invalid req package type %d", msg[0])
	}
}

// packResponse BORROWS msg (does NOT free), matching lua-cluster.c:lpackresponse.
func packResponse(node *runtime.Node, s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 3 {
		return nil, errors.New("packresponse requires (session, ok, msg [, sz])")
	}
	rawSess, ok := toIntArg(args[0])
	if !ok {
		return nil, errors.New("bad argument #1 to 'packresponse' (integer expected)")
	}
	session := uint32(rawSess)
	okFlag := !args[1].IsFalsy()
	msg, err := readInputBytes(node, s, args, 2)
	if err != nil {
		return nil, err
	}
	if uint64(len(msg)) > math.MaxUint32 {
		return nil, errors.New("cluster response exceeds maximum 32-bit size")
	}

	if !okFlag {
		if len(msg) > multiPart {
			msg = msg[:multiPart]
		}
	} else if len(msg) > multiPart {
		parts := (len(msg)-1)/multiPart + 1
		tbl := s.NewTable(parts+1, 0)
		tblPin := s.Pin(tbl.Value())
		defer tblPin.Release()

		begin := make([]byte, 11)
		putHeader(begin, 9)
		binary.LittleEndian.PutUint32(begin[2:6], session)
		begin[6] = 2
		binary.LittleEndian.PutUint32(begin[7:11], uint32(len(msg)))
		_ = tbl.SetInt(1, s.String(string(begin)))

		rem := msg
		for i := 0; i < parts; i++ {
			var chunk []byte
			var tag byte
			if len(rem) > multiPart {
				chunk = rem[:multiPart]
				rem = rem[multiPart:]
				tag = 3
			} else {
				chunk = rem
				rem = nil
				tag = 4
			}
			pkt := make([]byte, len(chunk)+7)
			putHeader(pkt, len(chunk)+5)
			binary.LittleEndian.PutUint32(pkt[2:6], session)
			pkt[6] = tag
			copy(pkt[7:], chunk)
			_ = tbl.SetInt(int64(i+2), s.String(string(pkt)))
		}
		return []moelua.Value{tbl.Value()}, nil
	}

	pkt := make([]byte, len(msg)+7)
	putHeader(pkt, len(msg)+5)
	binary.LittleEndian.PutUint32(pkt[2:6], session)
	if okFlag {
		pkt[6] = 1
	} else {
		pkt[6] = 0
	}
	copy(pkt[7:], msg)
	return []moelua.Value{s.String(string(pkt))}, nil
}

func lUnpackResponse(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("unpackresponse requires string")
	}
	if !args[0].IsString() {
		return nil, errors.New("bad argument #1 to 'unpackresponse' (string expected)")
	}
	buf, _ := s.ToString(args[0])
	if len(buf) < 5 {
		return nil, nil
	}
	raw := []byte(buf)
	session := binary.LittleEndian.Uint32(raw[0:4])
	switch raw[4] {
	case 0:
		return []moelua.Value{moelua.Int(int64(session)), moelua.Bool(false), s.String(string(raw[5:]))}, nil
	case 1, 4:
		return []moelua.Value{moelua.Int(int64(session)), moelua.Bool(true), s.String(string(raw[5:]))}, nil
	case 2:
		if len(raw) != 9 {
			return nil, nil
		}
		sz := binary.LittleEndian.Uint32(raw[5:9])
		return []moelua.Value{moelua.Int(int64(session)), moelua.Bool(true), moelua.Int(int64(sz)), moelua.Bool(true)}, nil
	case 3:
		return []moelua.Value{moelua.Int(int64(session)), moelua.Bool(true), s.String(string(raw[5:])), moelua.Bool(true)}, nil
	default:
		return nil, nil
	}
}

func lAppend(node *runtime.Node, s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 3 || !args[0].IsTable() {
		return nil, errors.New("append requires (table, msg, sz)")
	}
	tbl := s.TableOf(args[0])
	n := tbl.Len()
	if args[1].IsNil() {
		_ = tbl.SetInt(n+1, args[2])
		return nil, nil
	}
	tok, ok := runtime.ExtractToken(args[1])
	if !ok || tok == 0 {
		return nil, errors.New("Need lightuserdata")
	}
	// Always consume ownership of the buffer, matching lua-cluster.c:lappend.
	data, taken := node.Msgs().Take(tok)
	if !taken {
		return nil, errors.New("invalid lightuserdata buffer")
	}
	rawSz, ok := toIntArg(args[2])
	if !ok || rawSz < 0 || rawSz > int64(len(data)) {
		return nil, errors.New("invalid append size")
	}
	_ = tbl.SetInt(n+1, s.String(string(data[:rawSz])))
	return nil, nil
}

func lConcat(node *runtime.Node, s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 || !args[0].IsTable() {
		return nil, nil
	}
	tbl := s.TableOf(args[0])
	v1 := tbl.GetInt(1)
	rawSz, ok := toIntArg(v1)
	if !ok || rawSz < 0 || rawSz > math.MaxInt32 {
		return nil, nil
	}

	// Validate total chunk size before allocating to avoid OOM on forged headers.
	var total int64
	var parts []string
	for i := int64(2); ; i++ {
		elem := tbl.GetInt(i)
		if !elem.IsString() {
			break
		}
		str, _ := s.ToString(elem)
		total += int64(len(str))
		if total > rawSz {
			return nil, nil
		}
		parts = append(parts, str)
	}
	if total != rawSz {
		return nil, nil
	}

	buf := make([]byte, 0, int(total))
	for _, part := range parts {
		buf = append(buf, part...)
	}
	tok := node.Msgs().Alloc(buf)
	return []moelua.Value{runtime.LightUDToken(tok), moelua.Int(total)}, nil
}

func lIsName(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 || !args[0].IsString() {
		return nil, nil
	}
	if name, ok := s.ToString(args[0]); ok && strings.HasPrefix(name, "@") {
		return []moelua.Value{moelua.Bool(true)}, nil
	}
	return nil, nil
}

func lNodeName(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	pid := os.Getpid()
	host, err := os.Hostname()
	if err != nil || host == "" {
		return []moelua.Value{s.String(fmt.Sprintf("noname%d", pid))}, nil
	}
	b := []byte(host)
	for i, c := range b {
		if c <= ' ' {
			b[i] = '_'
		}
	}
	return []moelua.Value{s.String(fmt.Sprintf("%s%d", string(b), pid))}, nil
}
