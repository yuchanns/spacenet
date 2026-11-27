package runtime

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/moelua/vm"
)

// Skynet lua-seri wire format constants (matching lualib-src/lua-seri.c).
const (
	seriTypeNil         = 0
	seriTypeBoolean     = 1
	seriTypeNumber      = 2
	seriTypeUserdata    = 3
	seriTypeShortString = 4
	seriTypeLongString  = 5
	seriTypeTable       = 6

	seriNumberZero  = 0
	seriNumberByte  = 1
	seriNumberWord  = 2
	seriNumberDword = 4
	seriNumberQword = 6
	seriNumberReal  = 8

	seriMaxCookie = 32
	seriMaxDepth  = 32
)

func combineType(t, v byte) byte {
	return t | (v << 3)
}

// PackLuaValues serializes Lua values into Skynet's exact binary wire format.
func PackLuaValues(s *moelua.State, vals []moelua.Value) ([]byte, error) {
	raw := s.Raw()
	var stack [64]byte
	buf := stack[:0]
	var err error
	for _, v := range vals {
		buf, err = packOneLua(s, raw, buf, v, 0)
		if err != nil {
			return nil, err
		}
	}
	if len(buf) == 0 {
		return nil, nil
	}
	if cap(buf) <= len(stack) {
		out := make([]byte, len(buf))
		copy(out, buf)
		return out, nil
	}
	return buf, nil
}

func packOneLua(s *moelua.State, raw *vm.State, buf []byte, v moelua.Value, depth int) ([]byte, error) {
	if depth > seriMaxDepth {
		return nil, errors.New("serialize can't pack too depth table")
	}
	switch {
	case v.IsNil():
		return append(buf, seriTypeNil), nil
	case v.IsBool():
		b := byte(0)
		if v.IsTrue() {
			b = 1
		}
		return append(buf, combineType(seriTypeBoolean, b)), nil
	case v.IsInt():
		return appendSeriInt(buf, v.Int()), nil
	case v.IsFloat():
		return appendSeriReal(buf, v.Float()), nil
	case v.IsString():
		return appendSeriString(buf, raw.AsString(v).Str()), nil
	case v.IsLightUD():
		tok, _ := ExtractToken(v)
		buf = append(buf, seriTypeUserdata)
		return binary.LittleEndian.AppendUint64(buf, tok), nil
	case v.IsTable():
		return packTableLua(s, raw, buf, v, raw.AsTable(v), depth+1)
	default:
		return nil, fmt.Errorf("Unsupport type %s to serialize", v.TypeName())
	}
}

func appendSeriInt(buf []byte, v int64) []byte {
	if v == 0 {
		return append(buf, combineType(seriTypeNumber, seriNumberZero))
	}
	if v != int64(int32(v)) {
		buf = append(buf, combineType(seriTypeNumber, seriNumberQword))
		return binary.LittleEndian.AppendUint64(buf, uint64(v))
	}
	if v < 0 {
		buf = append(buf, combineType(seriTypeNumber, seriNumberDword))
		return binary.LittleEndian.AppendUint32(buf, uint32(int32(v)))
	}
	if v < 0x100 {
		return append(buf, combineType(seriTypeNumber, seriNumberByte), byte(v))
	}
	if v < 0x10000 {
		buf = append(buf, combineType(seriTypeNumber, seriNumberWord))
		return binary.LittleEndian.AppendUint16(buf, uint16(v))
	}
	buf = append(buf, combineType(seriTypeNumber, seriNumberDword))
	return binary.LittleEndian.AppendUint32(buf, uint32(v))
}

func appendSeriReal(buf []byte, f float64) []byte {
	buf = append(buf, combineType(seriTypeNumber, seriNumberReal))
	return binary.LittleEndian.AppendUint64(buf, math.Float64bits(f))
}

func appendSeriString(buf []byte, str string) []byte {
	n := len(str)
	if n < seriMaxCookie {
		buf = append(buf, combineType(seriTypeShortString, byte(n)))
		return append(buf, str...)
	}
	if n < 0x10000 {
		buf = append(buf, combineType(seriTypeLongString, 2))
		buf = binary.LittleEndian.AppendUint16(buf, uint16(n))
	} else {
		buf = append(buf, combineType(seriTypeLongString, 4))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(n))
	}
	return append(buf, str...)
}

func packTableLua(s *moelua.State, raw *vm.State, buf []byte, tblVal moelua.Value, tbl *vm.Table, depth int) ([]byte, error) {
	// Check __pairs metamethod first, matching lua-seri.c:wb_table.
	if mt := tbl.Metatable(raw); mt != nil {
		if ls, ok := raw.LookupString("__pairs"); ok {
			if pairsFn := mt.RawGetStr(raw, ls); !pairsFn.IsNil() {
				return packTableMetaPairs(s, raw, buf, pairsFn, tblVal, depth)
			}
		}
	}

	arrSize := tbl.Len(raw)
	if arrSize >= seriMaxCookie-1 {
		buf = append(buf, combineType(seriTypeTable, seriMaxCookie-1))
		buf = appendSeriInt(buf, arrSize)
	} else {
		buf = append(buf, combineType(seriTypeTable, byte(arrSize)))
	}

	var err error
	for i := int64(1); i <= arrSize; i++ {
		val := tbl.RawGetInt(raw, i)
		buf, err = packOneLua(s, raw, buf, val, depth)
		if err != nil {
			return nil, err
		}
	}

	key := vm.NilValue()
	for {
		nk, nv, nerr := tbl.Next(raw, key)
		if nerr != nil {
			return nil, nerr
		}
		if nk.IsNil() {
			break
		}
		key = nk
		if nk.IsInt() {
			x := nk.Int()
			if x > 0 && x <= arrSize {
				continue
			}
		}
		buf, err = packOneLua(s, raw, buf, nk, depth)
		if err != nil {
			return nil, err
		}
		buf, err = packOneLua(s, raw, buf, nv, depth)
		if err != nil {
			return nil, err
		}
	}
	return append(buf, seriTypeNil), nil
}

func packTableMetaPairs(s *moelua.State, raw *vm.State, buf []byte, pairsFn, tblVal moelua.Value, depth int) ([]byte, error) {
	buf = append(buf, combineType(seriTypeTable, 0))
	iterRes, err := s.Call(pairsFn, tblVal)
	if err != nil {
		return nil, err
	}
	var iterFn, stateVal, ctrlVal moelua.Value
	if len(iterRes) > 0 {
		iterFn = iterRes[0]
	}
	if len(iterRes) > 1 {
		stateVal = iterRes[1]
	}
	if len(iterRes) > 2 {
		ctrlVal = iterRes[2]
	}
	// Pin iterator state across loop iterations so post-call GC steps cannot
	// sweep fresh closures/tables returned by custom __pairs metamethods.
	iterRef := raw.Ref(iterFn)
	defer raw.Unref(iterRef)
	stateRef := raw.Ref(stateVal)
	defer raw.Unref(stateRef)
	ctrlRef := raw.Ref(ctrlVal)
	defer func() { raw.Unref(ctrlRef) }()
	for {
		pair, err := s.Call(iterFn, stateVal, ctrlVal)
		if err != nil {
			return nil, err
		}
		if len(pair) == 0 || pair[0].IsNil() {
			break
		}
		k := pair[0]
		v := moelua.Nil()
		if len(pair) > 1 {
			v = pair[1]
		}
		kRef := raw.Ref(k)
		vRef := raw.Ref(v)
		buf, err = packOneLua(s, raw, buf, k, depth)
		if err == nil {
			buf, err = packOneLua(s, raw, buf, v, depth)
		}
		raw.Unref(vRef)
		if err != nil {
			raw.Unref(kRef)
			return nil, err
		}
		raw.Unref(ctrlRef)
		ctrlRef = kRef
		ctrlVal = k
	}
	return append(buf, seriTypeNil), nil
}

type seriReader struct {
	buf []byte
	pos int
}

func (r *seriReader) readByte() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, fmt.Errorf("Invalid serialize stream %d", len(r.buf)-r.pos)
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *seriReader) readSlice(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.buf) {
		return nil, fmt.Errorf("Invalid serialize stream %d", len(r.buf)-r.pos)
	}
	sl := r.buf[r.pos : r.pos+n]
	r.pos += n
	return sl, nil
}

func (r *seriReader) readInt(cookie byte) (int64, error) {
	switch cookie {
	case seriNumberZero:
		return 0, nil
	case seriNumberByte:
		b, err := r.readByte()
		return int64(b), err
	case seriNumberWord:
		sl, err := r.readSlice(2)
		if err != nil {
			return 0, err
		}
		return int64(binary.LittleEndian.Uint16(sl)), nil
	case seriNumberDword:
		sl, err := r.readSlice(4)
		if err != nil {
			return 0, err
		}
		return int64(int32(binary.LittleEndian.Uint32(sl))), nil
	case seriNumberQword:
		sl, err := r.readSlice(8)
		if err != nil {
			return 0, err
		}
		return int64(binary.LittleEndian.Uint64(sl)), nil
	default:
		return 0, fmt.Errorf("Invalid serialize stream integer cookie %d", cookie)
	}
}

// UnpackLuaValues decodes all values from a Skynet lua-seri byte stream.
func UnpackLuaValues(s *moelua.State, data []byte) ([]moelua.Value, error) {
	if len(data) == 0 {
		return nil, nil
	}
	raw := s.Raw()
	r := seriReader{buf: data}
	out := make([]moelua.Value, 0, 4)
	for r.pos < len(r.buf) {
		v, err := unpackOneLua(raw, &r)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// UnpackLuaValuesN decodes up to limit values starting at byte offset,
// returning the next byte offset and decoded values (matching skynet.unpackn).
func UnpackLuaValuesN(s *moelua.State, data []byte, offset int, limit int) (int, []moelua.Value, error) {
	if offset < 0 || offset > len(data) {
		return 0, nil, fmt.Errorf("Invalid offset %d for buffer size %d", offset, len(data))
	}
	if limit < 0 {
		return 0, nil, fmt.Errorf("Invalid unpack count %d", limit)
	}
	raw := s.Raw()
	r := seriReader{buf: data, pos: offset}
	out := make([]moelua.Value, 0, min(limit, 8))
	for i := 0; i < limit && r.pos < len(r.buf); i++ {
		v, err := unpackOneLua(raw, &r)
		if err != nil {
			return 0, nil, err
		}
		out = append(out, v)
	}
	return r.pos, out, nil
}

func unpackOneLua(raw *vm.State, r *seriReader) (moelua.Value, error) {
	b, err := r.readByte()
	if err != nil {
		return moelua.Nil(), err
	}
	t := b & 0x7
	cookie := b >> 3
	switch t {
	case seriTypeNil:
		return moelua.Nil(), nil
	case seriTypeBoolean:
		return moelua.Bool(cookie != 0), nil
	case seriTypeNumber:
		if cookie == seriNumberReal {
			sl, err := r.readSlice(8)
			if err != nil {
				return moelua.Nil(), err
			}
			return moelua.Float(math.Float64frombits(binary.LittleEndian.Uint64(sl))), nil
		}
		v, err := r.readInt(cookie)
		if err != nil {
			return moelua.Nil(), err
		}
		return moelua.Int(v), nil
	case seriTypeUserdata:
		sl, err := r.readSlice(8)
		if err != nil {
			return moelua.Nil(), err
		}
		return LightUDToken(binary.LittleEndian.Uint64(sl)), nil
	case seriTypeShortString:
		sl, err := r.readSlice(int(cookie))
		if err != nil {
			return moelua.Nil(), err
		}
		ls, err := raw.TryIntern(string(sl))
		if err != nil {
			return moelua.Nil(), err
		}
		return vm.StringValue(ls), nil
	case seriTypeLongString:
		var n int
		if cookie == 2 {
			sl, err := r.readSlice(2)
			if err != nil {
				return moelua.Nil(), err
			}
			n = int(binary.LittleEndian.Uint16(sl))
		} else if cookie == 4 {
			sl, err := r.readSlice(4)
			if err != nil {
				return moelua.Nil(), err
			}
			n = int(binary.LittleEndian.Uint32(sl))
		} else {
			return moelua.Nil(), fmt.Errorf("Invalid serialize stream long string cookie %d", cookie)
		}
		sl, err := r.readSlice(n)
		if err != nil {
			return moelua.Nil(), err
		}
		ls, err := raw.TryIntern(string(sl))
		if err != nil {
			return moelua.Nil(), err
		}
		return vm.StringValue(ls), nil
	case seriTypeTable:
		arrSize := int64(cookie)
		if cookie == seriMaxCookie-1 {
			tb, err := r.readByte()
			if err != nil {
				return moelua.Nil(), err
			}
			if (tb&7) != seriTypeNumber || (tb>>3) == seriNumberReal {
				return moelua.Nil(), errors.New("Invalid serialize stream table array size")
			}
			arrSize, err = r.readInt(tb >> 3)
			if err != nil {
				return moelua.Nil(), err
			}
		}
		if arrSize < 0 || arrSize > 1<<24 {
			return moelua.Nil(), fmt.Errorf("Invalid serialize stream array size %d", arrSize)
		}
		tbl, err := raw.TryNewTable(int(arrSize), 0)
		if err != nil {
			return moelua.Nil(), err
		}
		tblVal := vm.TableValue(tbl)
		tblRef := raw.Ref(tblVal)
		defer raw.Unref(tblRef)
		for i := int64(1); i <= arrSize; i++ {
			v, err := unpackOneLua(raw, r)
			if err != nil {
				return moelua.Nil(), err
			}
			if v.IsGCObject() {
				vref := raw.Ref(v)
				tbl.RawSetInt(raw, i, v)
				raw.Unref(vref)
			} else {
				tbl.RawSetInt(raw, i, v)
			}
			if err := raw.ConsumeMemErr(); err != nil {
				return moelua.Nil(), err
			}
		}
		for {
			k, err := unpackOneLua(raw, r)
			if err != nil {
				return moelua.Nil(), err
			}
			if k.IsNil() {
				return tblVal, nil
			}
			kref := 0
			if k.IsGCObject() {
				kref = raw.Ref(k)
			}
			v, err := unpackOneLua(raw, r)
			if err != nil {
				if kref != 0 {
					raw.Unref(kref)
				}
				return moelua.Nil(), err
			}
			vref := 0
			if v.IsGCObject() {
				vref = raw.Ref(v)
			}
			err = tbl.RawSet(raw, k, v)
			if vref != 0 {
				raw.Unref(vref)
			}
			if kref != 0 {
				raw.Unref(kref)
			}
			if err != nil {
				_ = raw.ConsumeMemErr()
				return moelua.Nil(), err
			}
			if err := raw.ConsumeMemErr(); err != nil {
				return moelua.Nil(), err
			}
		}
	default:
		return moelua.Nil(), fmt.Errorf("Invalid serialize stream type %d", t)
	}
}

// PackGoValues serializes native Go values (nil, bool, int/int64/uint32, float64,
// string, []any, map[string]any) into Skynet's lua-seri binary format for Go <-> Lua RPC.
func PackGoValues(vals ...any) ([]byte, error) {
	buf := make([]byte, 0, 64)
	var err error
	for _, v := range vals {
		buf, err = packOneGo(buf, v, 0)
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func packOneGo(buf []byte, v any, depth int) ([]byte, error) {
	if depth > seriMaxDepth {
		return nil, errors.New("serialize can't pack too depth table")
	}
	switch x := v.(type) {
	case nil:
		return append(buf, seriTypeNil), nil
	case bool:
		if x {
			return append(buf, combineType(seriTypeBoolean, 1)), nil
		}
		return append(buf, combineType(seriTypeBoolean, 0)), nil
	case int:
		return appendSeriInt(buf, int64(x)), nil
	case int32:
		return appendSeriInt(buf, int64(x)), nil
	case int64:
		return appendSeriInt(buf, x), nil
	case uint32:
		return appendSeriInt(buf, int64(x)), nil
	case uint64:
		return appendSeriInt(buf, int64(x)), nil
	case float64:
		return appendSeriReal(buf, x), nil
	case float32:
		return appendSeriReal(buf, float64(x)), nil
	case string:
		return appendSeriString(buf, x), nil
	case []byte:
		return appendSeriString(buf, string(x)), nil
	case []any:
		n := len(x)
		if n >= seriMaxCookie-1 {
			buf = append(buf, combineType(seriTypeTable, seriMaxCookie-1))
			buf = appendSeriInt(buf, int64(n))
		} else {
			buf = append(buf, combineType(seriTypeTable, byte(n)))
		}
		var err error
		for _, elem := range x {
			buf, err = packOneGo(buf, elem, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return append(buf, seriTypeNil), nil
	case map[string]any:
		buf = append(buf, combineType(seriTypeTable, 0))
		var err error
		for k, val := range x {
			buf = appendSeriString(buf, k)
			buf, err = packOneGo(buf, val, depth+1)
			if err != nil {
				return nil, err
			}
		}
		return append(buf, seriTypeNil), nil
	default:
		return nil, fmt.Errorf("unsupported Go type %T to serialize", v)
	}
}

// UnpackGoValues decodes Skynet lua-seri bytes into Go values (nil, bool, int64,
// float64, string, uint64 token, map[any]any).
func UnpackGoValues(data []byte) ([]any, error) {
	r := seriReader{buf: data}
	var out []any
	for r.pos < len(r.buf) {
		v, err := unpackOneGo(&r)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func unpackOneGo(r *seriReader) (any, error) {
	b, err := r.readByte()
	if err != nil {
		return nil, err
	}
	t := b & 0x7
	cookie := b >> 3
	switch t {
	case seriTypeNil:
		return nil, nil
	case seriTypeBoolean:
		return cookie != 0, nil
	case seriTypeNumber:
		if cookie == seriNumberReal {
			sl, err := r.readSlice(8)
			if err != nil {
				return nil, err
			}
			return math.Float64frombits(binary.LittleEndian.Uint64(sl)), nil
		}
		return r.readInt(cookie)
	case seriTypeUserdata:
		sl, err := r.readSlice(8)
		if err != nil {
			return nil, err
		}
		return binary.LittleEndian.Uint64(sl), nil
	case seriTypeShortString:
		sl, err := r.readSlice(int(cookie))
		if err != nil {
			return nil, err
		}
		return string(sl), nil
	case seriTypeLongString:
		var n int
		if cookie == 2 {
			sl, err := r.readSlice(2)
			if err != nil {
				return nil, err
			}
			n = int(binary.LittleEndian.Uint16(sl))
		} else if cookie == 4 {
			sl, err := r.readSlice(4)
			if err != nil {
				return nil, err
			}
			n = int(binary.LittleEndian.Uint32(sl))
		} else {
			return nil, fmt.Errorf("Invalid serialize stream long string cookie %d", cookie)
		}
		sl, err := r.readSlice(n)
		if err != nil {
			return nil, err
		}
		return string(sl), nil
	case seriTypeTable:
		arrSize := int64(cookie)
		if cookie == seriMaxCookie-1 {
			tb, err := r.readByte()
			if err != nil {
				return nil, err
			}
			arrSize, err = r.readInt(tb >> 3)
			if err != nil {
				return nil, err
			}
		}
		m := make(map[any]any, arrSize)
		for i := int64(1); i <= arrSize; i++ {
			v, err := unpackOneGo(r)
			if err != nil {
				return nil, err
			}
			m[i] = v
		}
		for {
			k, err := unpackOneGo(r)
			if err != nil {
				return nil, err
			}
			if k == nil {
				return m, nil
			}
			v, err := unpackOneGo(r)
			if err != nil {
				return nil, err
			}
			if !reflect.TypeOf(k).Comparable() {
				return nil, fmt.Errorf("spacenet: non-comparable Lua table key of type %T", k)
			}
			m[k] = v
		}
	default:
		return nil, fmt.Errorf("Invalid serialize stream type %d", t)
	}
}
