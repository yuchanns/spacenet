package socket

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/runtime"
)

const metaSocketBuffer = "spacenet_socket_buffer"

// socketBuffer is a pure-Go ring/chunk buffer implementing Skynet's
// socketdriver.buffer / push / pop / readall / clear / readline semantics.
type socketBuffer struct {
	chunks [][]byte
	offset int
	size   int
}

func (sb *socketBuffer) push(data []byte) int {
	if len(data) > 0 {
		sb.chunks = append(sb.chunks, data)
		sb.size += len(data)
	}
	return sb.size
}

func (sb *socketBuffer) clear() {
	sb.chunks = nil
	sb.offset = 0
	sb.size = 0
}

func (sb *socketBuffer) popBytes(sz int, skip int) []byte {
	if sz <= 0 || sb.size < sz {
		return nil
	}
	want := sz - skip
	if want < 0 {
		want = 0
	}
	out := make([]byte, 0, want)
	rem := sz
	for rem > 0 && len(sb.chunks) > 0 {
		cur := sb.chunks[0]
		avail := len(cur) - sb.offset
		if avail >= rem {
			take := rem
			if len(out) < want {
				copyN := min(take, want-len(out))
				out = append(out, cur[sb.offset:sb.offset+copyN]...)
			}
			sb.offset += take
			if sb.offset == len(cur) {
				sb.chunks[0] = nil
				sb.chunks = sb.chunks[1:]
				sb.offset = 0
			}
			rem = 0
		} else {
			if len(out) < want {
				copyN := min(avail, want-len(out))
				out = append(out, cur[sb.offset:sb.offset+copyN]...)
			}
			sb.chunks[0] = nil
			sb.chunks = sb.chunks[1:]
			sb.offset = 0
			rem -= avail
		}
	}
	sb.size -= sz
	return out
}

func (sb *socketBuffer) readAll() []byte {
	if sb.size == 0 {
		return nil
	}
	out := make([]byte, 0, sb.size)
	for i, ch := range sb.chunks {
		if i == 0 {
			out = append(out, ch[sb.offset:]...)
		} else {
			out = append(out, ch...)
		}
	}
	sb.clear()
	return out
}

func (sb *socketBuffer) findSep(sep []byte) int {
	seplen := len(sep)
	if seplen == 0 || sb.size < seplen || len(sb.chunks) == 0 {
		return -1
	}
	// Fast path: single chunk.
	if len(sb.chunks) == 1 {
		return bytes.Index(sb.chunks[0][sb.offset:], sep)
	}
	// Cross-chunk scan without copying.
	ci := 0
	off := sb.offset
	maxPos := sb.size - seplen
	for pos := 0; pos <= maxPos; pos++ {
		if sb.matchAt(ci, off, sep) {
			return pos
		}
		off++
		if off >= len(sb.chunks[ci]) {
			ci++
			off = 0
		}
	}
	return -1
}

func (sb *socketBuffer) matchAt(ci, off int, sep []byte) bool {
	rem := sep
	for len(rem) > 0 {
		if ci >= len(sb.chunks) {
			return false
		}
		avail := sb.chunks[ci][off:]
		if len(avail) >= len(rem) {
			return bytes.Equal(avail[:len(rem)], rem)
		}
		if !bytes.Equal(avail, rem[:len(avail)]) {
			return false
		}
		rem = rem[len(avail):]
		ci++
		off = 0
	}
	return true
}

func checkSocketBuffer(s *moelua.State, args []moelua.Value) (*socketBuffer, error) {
	if len(args) == 0 || !args[0].IsUserdata() {
		return nil, errors.New("need buffer object at param 1")
	}
	buf, ok := s.CheckUserdata[*socketBuffer](args[0], metaSocketBuffer)
	if !ok {
		return nil, errors.New("need buffer object at param 1")
	}
	return buf, nil
}

func (m *manager) lNewBuffer(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	mt, _ := s.NewMetatable(metaSocketBuffer)
	ud, err := s.NewUserdataWithMeta(&socketBuffer{}, mt)
	if err != nil {
		return nil, err
	}
	return []moelua.Value{ud}, nil
}

func (m *manager) lPushBuffer(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	sb, err := checkSocketBuffer(s, args)
	if err != nil {
		return nil, err
	}
	if len(args) < 4 {
		return nil, errors.New("push requires (buffer, pool, msg, size)")
	}
	if !args[1].IsTable() {
		return nil, errors.New("bad argument #2 to 'push' (table expected)")
	}
	tok, ok := runtime.ExtractToken(args[2])
	if !ok || tok == 0 {
		return nil, errors.New("need message block at param 3")
	}
	sz := int(toInt(args[3]))
	data, ok := m.node.Msgs().Take(tok)
	if !ok {
		return nil, errors.New("invalid or freed message block at param 3")
	}
	if sz < len(data) {
		data = data[:sz]
	}
	total := sb.push(data)
	return []moelua.Value{moelua.Int(int64(total))}, nil
}

func (m *manager) lPopBuffer(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	sb, err := checkSocketBuffer(s, args)
	if err != nil {
		return nil, err
	}
	if len(args) < 3 || !args[1].IsTable() {
		return nil, errors.New("pop requires (buffer, pool, sz)")
	}
	sz := int(toInt(args[2]))
	if sb.size < sz || sz == 0 {
		return []moelua.Value{moelua.Nil(), moelua.Int(int64(sb.size))}, nil
	}
	out := sb.popBytes(sz, 0)
	return []moelua.Value{s.String(string(out)), moelua.Int(int64(sb.size))}, nil
}

func (m *manager) lClearBuffer(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) == 0 || args[0].IsNil() {
		return nil, nil
	}
	sb, err := checkSocketBuffer(s, args)
	if err != nil {
		return nil, err
	}
	sb.clear()
	return nil, nil
}

func (m *manager) lReadAll(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	sb, err := checkSocketBuffer(s, args)
	if err != nil {
		return nil, err
	}
	out := sb.readAll()
	return []moelua.Value{s.String(string(out))}, nil
}

func (m *manager) lDrop(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 2 {
		return nil, errors.New("drop requires (msg, sz)")
	}
	if tok, ok := runtime.ExtractToken(args[0]); ok && tok != 0 {
		m.node.Msgs().Free(tok)
	}
	return nil, nil
}

func (m *manager) lReadLine(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	sb, err := checkSocketBuffer(s, args)
	if err != nil {
		return nil, err
	}
	if len(args) < 3 {
		return nil, errors.New("readline requires (buffer, pool_or_nil, sep)")
	}
	checkOnly := !args[1].IsTable()
	sep, ok := s.ToString(args[2])
	if !ok {
		return nil, errors.New("bad argument #3 to 'readline' (string expected)")
	}
	pos := sb.findSep([]byte(sep))
	if pos < 0 {
		return nil, nil
	}
	if checkOnly {
		return []moelua.Value{moelua.Bool(true)}, nil
	}
	out := sb.popBytes(pos+len(sep), len(sep))
	return []moelua.Value{s.String(string(out))}, nil
}

func (m *manager) lStr2P(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("bad argument #1 to 'str2p' (string expected)")
	}
	str, ok := s.ToString(args[0])
	if !ok {
		return nil, errors.New("bad argument #1 to 'str2p' (string expected)")
	}
	tok := m.node.Msgs().AllocCopy([]byte(str))
	return []moelua.Value{runtime.LightUDToken(tok), moelua.Int(int64(len(str)))}, nil
}

func (m *manager) lHeader(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if len(args) < 1 {
		return nil, errors.New("bad argument #1 to 'header' (string expected)")
	}
	str, ok := s.ToString(args[0])
	if !ok {
		return nil, errors.New("bad argument #1 to 'header' (string expected)")
	}
	if len(str) < 1 || len(str) > 4 {
		return nil, fmt.Errorf("Invalid read %s", str)
	}
	var sz uint64
	for i := 0; i < len(str); i++ {
		sz = (sz << 8) | uint64(str[i])
	}
	return []moelua.Value{moelua.Int(int64(sz))}, nil
}
