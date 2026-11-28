package sproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/moelua/vm"
	"github.com/yuchanns/spacenet/runtime"
	"github.com/yuchanns/spacenet/runtime/contrib"
)

const (
	maxGlobalSproto = 16
	maxDeepLevel    = 64
	extKey          = "contrib.sproto.store"
)

type nodeStore struct {
	mu          sync.RWMutex
	slots       [maxGlobalSproto]uint64
	typeToks    map[*Type]uint64
	schemaTypes map[uint64][]*Type
}

func newNodeStore() *nodeStore {
	return &nodeStore{
		typeToks:    make(map[*Type]uint64),
		schemaTypes: make(map[uint64][]*Type),
	}
}

func (st *nodeStore) typeToken(node *runtime.Node, schemaTok uint64, tp *Type) uint64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	if tok, ok := st.typeToks[tp]; ok {
		return tok
	}
	tok := node.Objs().Put(runtime.KindSprotoObj, tp)
	st.typeToks[tp] = tok
	st.schemaTypes[schemaTok] = append(st.schemaTypes[schemaTok], tp)
	return tok
}

func (st *nodeStore) deleteSchema(node *runtime.Node, schemaTok uint64) {
	st.mu.Lock()
	for i := range st.slots {
		if st.slots[i] == schemaTok {
			st.slots[i] = 0
		}
	}
	types := st.schemaTypes[schemaTok]
	delete(st.schemaTypes, schemaTok)
	toks := make([]uint64, 0, len(types))
	for _, tp := range types {
		if tok, ok := st.typeToks[tp]; ok {
			toks = append(toks, tok)
			delete(st.typeToks, tp)
		}
	}
	st.mu.Unlock()

	for _, tok := range toks {
		node.Objs().Delete(runtime.KindSprotoObj, tok)
	}
	node.Objs().Delete(runtime.KindSprotoObj, schemaTok)
}

// New returns the sproto contrib (providing sproto.core, sproto, sprotoparser, sprotoloader).
func New() *contrib.Contrib {
	return contrib.New("sproto",
		contrib.WithInit(func(n *runtime.Node) (io.Closer, error) {
			n.SetExt(extKey, newNodeStore())
			return nil, nil
		}),
		contrib.WithFS(subFS),
		contrib.WithPreload("sproto.core", preloadSprotoCore),
		contrib.WithPreload("sprotoparser", preloadSprotoParser),
	)
}

func getStore(n *runtime.Node) (*nodeStore, error) {
	st, ok := n.Ext[*nodeStore](extKey)
	if !ok || st == nil {
		return nil, errors.New("sproto contrib not initialized on node")
	}
	return st, nil
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

func preloadSprotoCore(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, err
	}
	node := actor.Node()
	store, err := getStore(node)
	if err != nil {
		return nil, err
	}

	readBuf := func(st *moelua.State, args []moelua.Value, idx int) ([]byte, error) {
		if idx >= len(args) {
			return nil, errors.New("Need a string or userdata")
		}
		v := args[idx]
		if v.IsString() {
			return []byte(st.Raw().AsString(v).Str()), nil
		}
		if v.IsLightUD() {
			if idx+1 >= len(args) {
				return nil, errors.New("Need buffer size")
			}
			sz := int(toInt(args[idx+1]))
			if sz < 0 {
				return nil, errors.New("negative buffer size")
			}
			tok, _ := runtime.ExtractToken(v)
			if tok == 0 {
				return nil, nil
			}
			if b, ok := node.Msgs().Borrow(tok); ok {
				if sz < len(b) {
					return b[:sz], nil
				}
				return b, nil
			}
			if b, _, ok := actor.Borrow().Read(tok); ok {
				if sz < len(b) {
					return b[:sz], nil
				}
				return b, nil
			}
			return nil, errors.New("invalid lightuserdata buffer")
		}
		return nil, errors.New("Need a string or userdata")
	}

	tbl := s.NewTable(0, 12)

	_ = tbl.SetString("newproto", s.NewFunc("sproto.newproto", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("newproto requires schema string")
		}
		bin, ok := st.ToString(args[0])
		if !ok {
			return nil, errors.New("bad argument #1 to 'newproto' (string expected)")
		}
		sc, err := ParseSchema([]byte(bin))
		if err != nil {
			return nil, nil
		}
		tok := node.Objs().Put(runtime.KindSprotoObj, sc)
		return []moelua.Value{runtime.LightUDToken(tok)}, nil
	}))

	_ = tbl.SetString("deleteproto", s.NewFunc("sproto.deleteproto", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("Need a sproto object")
		}
		tok, ok := runtime.ExtractToken(args[0])
		if !ok || tok == 0 {
			return nil, errors.New("Need a sproto object")
		}
		store.deleteSchema(node, tok)
		return nil, nil
	}))

	_ = tbl.SetString("dumpproto", s.NewFunc("sproto.dumpproto", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("Need a sproto_type object")
		}
		return nil, nil
	}))

	_ = tbl.SetString("querytype", s.NewFunc("sproto.querytype", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 2 {
			return nil, errors.New("querytype requires (sp, typename)")
		}
		tok, ok := runtime.ExtractToken(args[0])
		if !ok || tok == 0 {
			return nil, errors.New("Need a sproto object")
		}
		v, ok := node.Objs().Get(runtime.KindSprotoObj, tok)
		if !ok {
			return nil, errors.New("Need a sproto object")
		}
		sc, ok := v.(*Schema)
		if !ok {
			return nil, errors.New("Need a sproto object")
		}
		name, ok := st.ToString(args[1])
		if !ok {
			return nil, errors.New("bad argument #2 to 'querytype' (string expected)")
		}
		tp := sc.QueryType(name)
		if tp == nil {
			return nil, nil
		}
		tpTok := store.typeToken(node, tok, tp)
		return []moelua.Value{runtime.LightUDToken(tpTok)}, nil
	}))

	_ = tbl.SetString("protocol", s.NewFunc("sproto.protocol", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 2 {
			return nil, errors.New("protocol requires (sp, tag_or_name)")
		}
		tok, ok := runtime.ExtractToken(args[0])
		if !ok || tok == 0 {
			return nil, errors.New("Need a sproto_type object")
		}
		v, ok := node.Objs().Get(runtime.KindSprotoObj, tok)
		if !ok {
			return nil, errors.New("Need a sproto_type object")
		}
		sc, ok := v.(*Schema)
		if !ok {
			return nil, errors.New("Need a sproto_type object")
		}

		var proto *Protocol
		var first moelua.Value
		if args[1].IsNumber() {
			tag := int(toInt(args[1]))
			proto = sc.QueryProtocolByTag(tag)
			if proto == nil {
				return nil, nil
			}
			first = st.String(proto.Name)
		} else {
			name, ok := st.ToString(args[1])
			if !ok {
				return nil, errors.New("Should be number or string")
			}
			proto = sc.QueryProtocolByName(name)
			if proto == nil {
				return nil, nil
			}
			first = moelua.Int(int64(proto.Tag))
		}

		reqVal := moelua.Nil()
		if proto.Req != nil {
			reqTok := store.typeToken(node, tok, proto.Req)
			reqVal = runtime.LightUDToken(reqTok)
		}
		respVal := moelua.Nil()
		if proto.Resp != nil {
			respTok := store.typeToken(node, tok, proto.Resp)
			respVal = runtime.LightUDToken(respTok)
		} else if proto.Confirm {
			// CRITICAL: LightUDToken(0) is truthy in Lua, matching lua_pushlightuserdata(L, NULL)!
			respVal = runtime.LightUDToken(0)
		}
		return []moelua.Value{first, reqVal, respVal}, nil
	}))

	_ = tbl.SetString("saveproto", s.NewFunc("sproto.saveproto", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("saveproto requires sproto pointer")
		}
		tok, ok := runtime.ExtractToken(args[0])
		if !ok {
			return nil, errors.New("invalid sproto pointer")
		}
		idx := 0
		if len(args) >= 2 && !args[1].IsNil() {
			idx = int(toInt(args[1]))
		}
		if idx < 0 || idx >= maxGlobalSproto {
			return nil, fmt.Errorf("Invalid global slot index %d", idx)
		}
		store.mu.Lock()
		store.slots[idx] = tok
		store.mu.Unlock()
		return nil, nil
	}))

	_ = tbl.SetString("loadproto", s.NewFunc("sproto.loadproto", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		idx := 0
		if len(args) >= 1 && !args[0].IsNil() {
			idx = int(toInt(args[0]))
		}
		if idx < 0 || idx >= maxGlobalSproto {
			return nil, fmt.Errorf("Invalid global slot index %d", idx)
		}
		store.mu.RLock()
		tok := store.slots[idx]
		store.mu.RUnlock()
		if tok == 0 {
			return nil, fmt.Errorf("nil sproto at index %d", idx)
		}
		return []moelua.Value{runtime.LightUDToken(tok)}, nil
	}))

	_ = tbl.SetString("encode", s.NewFunc("sproto.encode", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 2 {
			return nil, errors.New("encode requires (type, table)")
		}
		tok, ok := runtime.ExtractToken(args[0])
		if !ok || tok == 0 {
			// Response nil sentinel (LightUDToken(0))
			if !args[1].IsNil() {
				return nil, errors.New("bad argument #2 to 'encode' (nil expected)")
			}
			return []moelua.Value{st.String("")}, nil
		}
		v, ok := node.Objs().Get(runtime.KindSprotoObj, tok)
		if !ok {
			return nil, errors.New("invalid sproto_type")
		}
		tp, ok := v.(*Type)
		if !ok {
			return nil, errors.New("invalid sproto_type")
		}
		if !args[1].IsTable() {
			return nil, fmt.Errorf("bad argument #2 to 'encode' (table expected, got %s)", args[1].TypeName())
		}
		encoded, err := encodeTable(st, tp, args[1], 0)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{st.String(string(encoded))}, nil
	}))

	_ = tbl.SetString("decode", s.NewFunc("sproto.decode", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 2 {
			return nil, errors.New("decode requires (type, buffer)")
		}
		tok, ok := runtime.ExtractToken(args[0])
		if !ok || tok == 0 {
			return nil, nil
		}
		v, ok := node.Objs().Get(runtime.KindSprotoObj, tok)
		if !ok {
			return nil, errors.New("invalid sproto_type")
		}
		tp, ok := v.(*Type)
		if !ok {
			return nil, errors.New("invalid sproto_type")
		}
		buf, err := readBuf(st, args, 1)
		if err != nil {
			return nil, err
		}
		raw := st.Raw()
		var target *vm.Table
		var targetVal moelua.Value
		if args[len(args)-1].IsTable() {
			targetVal = args[len(args)-1]
			target = raw.AsTable(targetVal)
		} else {
			var terr error
			target, terr = raw.TryNewTable(0, len(tp.Fields))
			if terr != nil {
				return nil, terr
			}
			targetVal = vm.TableValue(target)
		}
		tref := raw.Ref(targetVal)
		nread, err := decodeTable(raw, tp, buf, target, 0)
		raw.Unref(tref)
		if err != nil {
			return nil, fmt.Errorf("decode error: %w", err)
		}
		return []moelua.Value{targetVal, moelua.Int(int64(nread))}, nil
	}))

	_ = tbl.SetString("pack", s.NewFunc("sproto.pack", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		buf, err := readBuf(st, args, 0)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{st.String(string(Pack(buf)))}, nil
	}))

	_ = tbl.SetString("unpack", s.NewFunc("sproto.unpack", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		buf, err := readBuf(st, args, 0)
		if err != nil {
			return nil, err
		}
		out, err := Unpack(buf)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{st.String(string(out))}, nil
	}))

	_ = tbl.SetString("default", s.NewFunc("sproto.default", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("Need a sproto_type object")
		}
		tok, ok := runtime.ExtractToken(args[0])
		if !ok || tok == 0 {
			return nil, errors.New("Need a sproto_type object")
		}
		v, ok := node.Objs().Get(runtime.KindSprotoObj, tok)
		if !ok {
			return nil, errors.New("Need a sproto_type object")
		}
		tp, ok := v.(*Type)
		if !ok {
			return nil, errors.New("Need a sproto_type object")
		}
		out := st.NewTable(0, len(tp.Fields))
		for i := range tp.Fields {
			f := &tp.Fields[i]
			baseType := f.Type &^ TypeArray
			defVal := func(inArray bool) moelua.Value {
				switch baseType {
				case TypeInteger:
					if f.Extra > 0 {
						return moelua.Float(0.0)
					}
					return moelua.Int(0)
				case TypeDouble:
					return moelua.Float(0.0)
				case TypeBoolean:
					return moelua.Bool(false)
				case TypeString:
					return st.String("")
				case TypeStruct:
					if inArray {
						return st.String(f.St.Name)
					}
					sub := st.NewTable(0, 1)
					_ = sub.SetString("__type", st.String(f.St.Name))
					return sub.Value()
				default:
					return moelua.Nil()
				}
			}
			if f.Type&TypeArray != 0 {
				arr := st.NewTable(0, 1)
				_ = arr.SetString("__array", defVal(true))
				_ = out.SetString(f.Name, arr.Value())
			} else {
				_ = out.SetString(f.Name, defVal(false))
			}
		}
		return []moelua.Value{out.Value()}, nil
	}))

	return []moelua.Value{tbl.Value()}, nil
}

// ---------- Lua <-> Sproto Encoder ----------

func getLuaField(s *moelua.State, raw *vm.State, tblVal moelua.Value, name string) (moelua.Value, error) {
	const maxIndexLoop = 100
	cur := tblVal
	for step := 0; step < maxIndexLoop; step++ {
		if cur.IsTable() {
			t := raw.AsTable(cur)
			if ls, ok := raw.LookupString(name); ok {
				if v := t.RawGetStr(raw, ls); !v.IsNil() {
					return v, nil
				}
			}
			mt := t.Metatable(raw)
			if mt == nil {
				return moelua.Nil(), nil
			}
			idxLs, ok := raw.LookupString("__index")
			if !ok {
				return moelua.Nil(), nil
			}
			idxVal := mt.RawGetStr(raw, idxLs)
			if idxVal.IsNil() {
				return moelua.Nil(), nil
			}
			if idxVal.IsFunction() {
				return s.Call1(idxVal, cur, s.String(name))
			}
			cur = idxVal
			continue
		}
		idxVal := s.GetMetafield(cur, "__index")
		if idxVal.IsNil() {
			return moelua.Nil(), nil
		}
		if idxVal.IsFunction() {
			return s.Call1(idxVal, cur, s.String(name))
		}
		cur = idxVal
	}
	return moelua.Nil(), errors.New("'__index' chain too long; possible loop")
}

func appendLenStr(dst []byte, s string) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(s)))
	return append(dst, s...)
}

func encodeTable(s *moelua.State, tp *Type, tblVal moelua.Value, depth int) ([]byte, error) {
	if depth >= maxDeepLevel {
		return nil, errors.New("The table is too deep")
	}
	if !tblVal.IsTable() && !(tblVal.IsUserdata() && !s.GetMetafield(tblVal, "__index").IsNil()) {
		return nil, fmt.Errorf("expected table for struct %s, got %s", tp.Name, tblVal.TypeName())
	}

	raw := s.Raw()
	var recStack [16]uint16
	records := recStack[:0]
	var dataStack [128]byte
	data := dataStack[:0]
	lastTag := -1

	appendRecord := func(tag int, val uint16) error {
		skip := tag - lastTag - 1
		if skip > 0 {
			enc := (skip-1)*2 + 1
			if enc > 0xffff {
				return errors.New("sproto: tag gap too large")
			}
			records = append(records, uint16(enc))
		}
		records = append(records, val)
		lastTag = tag
		return nil
	}

	for i := range tp.Fields {
		f := &tp.Fields[i]
		fv, err := getLuaField(s, raw, tblVal, f.Name)
		if err != nil {
			return nil, err
		}
		if fv.IsNil() {
			continue
		}

		if f.Type&TypeArray != 0 {
			fvRef := 0
			if fv.IsGCObject() {
				fvRef = raw.Ref(fv)
			}
			arrBytes, err := encodeArrayField(s, raw, tp, f, fv, depth)
			if fvRef != 0 {
				raw.Unref(fvRef)
			}
			if err != nil {
				return nil, err
			}
			if err := appendRecord(f.Tag, 0); err != nil {
				return nil, err
			}
			data = appendLenBytes(data, arrBytes)
			continue
		}

		switch f.Type {
		case TypeInteger:
			v, err := luaToSprotoInt(tp, f, fv)
			if err != nil {
				return nil, err
			}
			vh := v >> 31
			if vh == 0 || vh == -1 {
				u32 := uint32(v)
				if u32 < 0x7fff {
					if err := appendRecord(f.Tag, uint16((u32+1)*2)); err != nil {
						return nil, err
					}
				} else {
					if err := appendRecord(f.Tag, 0); err != nil {
						return nil, err
					}
					data = binary.LittleEndian.AppendUint32(data, 4)
					data = binary.LittleEndian.AppendUint32(data, u32)
				}
			} else {
				if err := appendRecord(f.Tag, 0); err != nil {
					return nil, err
				}
				data = binary.LittleEndian.AppendUint32(data, 8)
				data = binary.LittleEndian.AppendUint64(data, uint64(v))
			}

		case TypeDouble:
			var f64 float64
			if fv.IsInt() {
				f64 = float64(fv.Int())
			} else if fv.IsFloat() {
				f64 = fv.Float()
			} else {
				return nil, fmt.Errorf("%s.%s is not a number", tp.Name, f.Name)
			}
			if err := appendRecord(f.Tag, 0); err != nil {
				return nil, err
			}
			data = binary.LittleEndian.AppendUint32(data, 8)
			data = binary.LittleEndian.AppendUint64(data, math.Float64bits(f64))

		case TypeBoolean:
			if !fv.IsBool() {
				return nil, fmt.Errorf("%s.%s[0] is not a boolean (Is a %s)", tp.Name, f.Name, fv.TypeName())
			}
			b := uint16(0)
			if fv.IsTrue() {
				b = 1
			}
			if err := appendRecord(f.Tag, (b+1)*2); err != nil {
				return nil, err
			}

		case TypeString:
			if !fv.IsString() {
				return nil, fmt.Errorf("%s.%s[0] is not a string (Is a %s)", tp.Name, f.Name, fv.TypeName())
			}
			if err := appendRecord(f.Tag, 0); err != nil {
				return nil, err
			}
			data = appendLenStr(data, raw.AsString(fv).Str())

		case TypeStruct:
			fvRef := 0
			if fv.IsGCObject() {
				fvRef = raw.Ref(fv)
			}
			sub, err := encodeTable(s, f.St, fv, depth+1)
			if fvRef != 0 {
				raw.Unref(fvRef)
			}
			if err != nil {
				return nil, err
			}
			if err := appendRecord(f.Tag, 0); err != nil {
				return nil, err
			}
			data = appendLenBytes(data, sub)
		}
	}

	if len(records) > 0xffff {
		return nil, errors.New("sproto: too many fields")
	}
	out := make([]byte, 0, 2+len(records)*2+len(data))
	out = binary.LittleEndian.AppendUint16(out, uint16(len(records)))
	for _, r := range records {
		out = binary.LittleEndian.AppendUint16(out, r)
	}
	out = append(out, data...)
	return out, nil
}

func luaToSprotoInt(tp *Type, f *Field, v moelua.Value) (int64, error) {
	if f.Extra > 0 {
		var num float64
		if v.IsInt() {
			num = float64(v.Int())
		} else if v.IsFloat() {
			num = v.Float()
		} else {
			return 0, fmt.Errorf("%s.%s is not a number", tp.Name, f.Name)
		}
		return int64(math.Round(num * float64(f.Extra))), nil
	}
	if v.IsInt() {
		return v.Int(), nil
	}
	if v.IsFloat() {
		f64 := v.Float()
		if f64 == math.Trunc(f64) && !math.IsInf(f64, 0) && !math.IsNaN(f64) {
			return int64(f64), nil
		}
	}
	return 0, fmt.Errorf("%s.%s is not an integer (Is a %s)", tp.Name, f.Name, v.TypeName())
}

func forEachMapEntry(s *moelua.State, raw *vm.State, arrVal moelua.Value, fn func(k, v moelua.Value) error) error {
	if pairsFn := s.GetMetafield(arrVal, "__pairs"); !pairsFn.IsNil() {
		iterRes, err := s.Call(pairsFn, arrVal)
		if err != nil {
			return err
		}
		var iterFn, stVal, ctrl moelua.Value
		if len(iterRes) > 0 {
			iterFn = iterRes[0]
		}
		if len(iterRes) > 1 {
			stVal = iterRes[1]
		}
		if len(iterRes) > 2 {
			ctrl = iterRes[2]
		}
		iterRef := raw.Ref(iterFn)
		defer raw.Unref(iterRef)
		stRef := raw.Ref(stVal)
		defer raw.Unref(stRef)
		ctrlRef := raw.Ref(ctrl)
		defer func() { raw.Unref(ctrlRef) }()
		for {
			kv, err := s.Call(iterFn, stVal, ctrl)
			if err != nil {
				return err
			}
			if len(kv) == 0 || kv[0].IsNil() {
				break
			}
			k := kv[0]
			v := moelua.Nil()
			if len(kv) > 1 {
				v = kv[1]
			}
			kRef := raw.Ref(k)
			vRef := raw.Ref(v)
			err = fn(k, v)
			raw.Unref(vRef)
			if err != nil {
				raw.Unref(kRef)
				return err
			}
			raw.Unref(ctrlRef)
			ctrlRef = kRef
			ctrl = k
		}
		return nil
	}
	if !arrVal.IsTable() {
		return fmt.Errorf("expected table, got %s", arrVal.TypeName())
	}
	return s.TableOf(arrVal).ForEach(fn)
}

func encodeArrayField(s *moelua.State, raw *vm.State, tp *Type, f *Field, arrVal moelua.Value, depth int) ([]byte, error) {
	baseType := f.Type &^ TypeArray

	// Indexed map or *Struct(key)
	if f.Key >= 0 && baseType == TypeStruct {
		var out []byte
		if f.Map {
			if len(f.St.Fields) < 2 {
				return nil, fmt.Errorf("sproto: invalid map struct %s", f.St.Name)
			}
			kName := f.St.Fields[0].Name
			vName := f.St.Fields[1].Name
			tmp, err := raw.TryNewTable(0, 2)
			if err != nil {
				return nil, err
			}
			tmpVal := vm.TableValue(tmp)
			tref := raw.Ref(tmpVal)
			defer raw.Unref(tref)
			kls, err := raw.TryIntern(kName)
			if err != nil {
				return nil, err
			}
			vls, err := raw.TryIntern(vName)
			if err != nil {
				return nil, err
			}
			if err := forEachMapEntry(s, raw, arrVal, func(k, v moelua.Value) error {
				tmp.RawSetStr(raw, kls, k)
				tmp.RawSetStr(raw, vls, v)
				sub, err := encodeTable(s, f.St, tmpVal, depth+1)
				if err != nil {
					return err
				}
				out = appendLenBytes(out, sub)
				return nil
			}); err != nil {
				return nil, err
			}
		} else {
			if err := forEachMapEntry(s, raw, arrVal, func(_, v moelua.Value) error {
				sub, err := encodeTable(s, f.St, v, depth+1)
				if err != nil {
					return err
				}
				out = appendLenBytes(out, sub)
				return nil
			}); err != nil {
				return nil, err
			}
		}
		return out, nil
	}

	if !arrVal.IsTable() {
		return nil, fmt.Errorf("%s.%s should be a table (Is a %s)", tp.Name, f.Name, arrVal.TypeName())
	}
	tbl := raw.AsTable(arrVal)

	switch baseType {
	case TypeInteger:
		var valStack [16]int64
		vals := valStack[:0]
		need64 := false
		for i := int64(1); ; i++ {
			elem := tbl.RawGetInt(raw, i)
			if elem.IsNil() {
				break
			}
			iv, err := luaToSprotoInt(tp, f, elem)
			if err != nil {
				return nil, err
			}
			vh := iv >> 31
			if vh != 0 && vh != -1 {
				need64 = true
			}
			vals = append(vals, iv)
		}
		if len(vals) == 0 {
			return nil, nil
		}
		if need64 {
			out := make([]byte, 0, 1+len(vals)*8)
			out = append(out, 8)
			for _, iv := range vals {
				out = binary.LittleEndian.AppendUint64(out, uint64(iv))
			}
			return out, nil
		}
		out := make([]byte, 0, 1+len(vals)*4)
		out = append(out, 4)
		for _, iv := range vals {
			out = binary.LittleEndian.AppendUint32(out, uint32(iv))
		}
		return out, nil

	case TypeDouble:
		var valStack [16]float64
		vals := valStack[:0]
		for i := int64(1); ; i++ {
			elem := tbl.RawGetInt(raw, i)
			if elem.IsNil() {
				break
			}
			if elem.IsInt() {
				vals = append(vals, float64(elem.Int()))
			} else if elem.IsFloat() {
				vals = append(vals, elem.Float())
			} else {
				return nil, fmt.Errorf("%s.%s[%d] is not a number", tp.Name, f.Name, i)
			}
		}
		if len(vals) == 0 {
			return nil, nil
		}
		out := make([]byte, 0, 1+len(vals)*8)
		out = append(out, 8)
		for _, fv := range vals {
			out = binary.LittleEndian.AppendUint64(out, math.Float64bits(fv))
		}
		return out, nil

	case TypeBoolean:
		var out []byte
		for i := int64(1); ; i++ {
			elem := tbl.RawGetInt(raw, i)
			if elem.IsNil() {
				break
			}
			if !elem.IsBool() {
				return nil, fmt.Errorf("%s.%s[%d] is not a boolean", tp.Name, f.Name, i)
			}
			if elem.IsTrue() {
				out = append(out, 1)
			} else {
				out = append(out, 0)
			}
		}
		return out, nil

	case TypeString:
		var out []byte
		for i := int64(1); ; i++ {
			elem := tbl.RawGetInt(raw, i)
			if elem.IsNil() {
				break
			}
			if !elem.IsString() {
				return nil, fmt.Errorf("%s.%s[%d] is not a string", tp.Name, f.Name, i)
			}
			out = appendLenStr(out, raw.AsString(elem).Str())
		}
		return out, nil

	case TypeStruct:
		var out []byte
		for i := int64(1); ; i++ {
			elem := tbl.RawGetInt(raw, i)
			if elem.IsNil() {
				break
			}
			sub, err := encodeTable(s, f.St, elem, depth+1)
			if err != nil {
				return nil, err
			}
			out = appendLenBytes(out, sub)
		}
		return out, nil
	}
	return nil, fmt.Errorf("invalid sproto array type %d", baseType)
}

// ---------- Sproto -> Lua Decoder ----------

func rawSetField(raw *vm.State, target *vm.Table, name string, val moelua.Value) error {
	vref := 0
	if val.IsGCObject() {
		vref = raw.Ref(val)
	}
	ls, err := raw.TryIntern(name)
	if err != nil {
		if vref != 0 {
			raw.Unref(vref)
		}
		return err
	}
	target.RawSetStr(raw, ls, val)
	if vref != 0 {
		raw.Unref(vref)
	}
	return raw.ConsumeMemErr()
}

func decodeTable(raw *vm.State, tp *Type, data []byte, target *vm.Table, depth int) (int, error) {
	if depth >= maxDeepLevel {
		return 0, errors.New("The table is too deep")
	}
	if len(data) < 2 {
		return 0, errors.New("truncated sproto header")
	}
	fn := int(binary.LittleEndian.Uint16(data[0:2]))
	hdrEnd := 2 + fn*2
	if len(data) < hdrEnd {
		return 0, errors.New("truncated sproto field headers")
	}
	pos := hdrEnd
	tag := -1

	for i := 0; i < fn; i++ {
		val := int(binary.LittleEndian.Uint16(data[2+i*2 : 4+i*2]))
		tag++
		if val&1 != 0 {
			tag += val / 2
			continue
		}
		val = val/2 - 1
		var fieldData []byte
		if val < 0 {
			if pos+4 > len(data) {
				return 0, errors.New("truncated field length")
			}
			dsz := uint64(binary.LittleEndian.Uint32(data[pos : pos+4]))
			if dsz > uint64(len(data)-(pos+4)) {
				return 0, errors.New("truncated field body")
			}
			fieldData = data[pos+4 : pos+4+int(dsz)]
			pos += 4 + int(dsz)
		}
		f := tp.findTag(tag)
		if f == nil {
			continue
		}

		if val < 0 {
			if f.Type&TypeArray != 0 {
				arrVal, err := decodeArrayField(raw, f, fieldData, depth)
				if err != nil {
					return 0, err
				}
				if err := rawSetField(raw, target, f.Name, arrVal); err != nil {
					return 0, err
				}
			} else {
				switch f.Type {
				case TypeInteger:
					var iv int64
					if len(fieldData) == 4 {
						iv = expandSign64(binary.LittleEndian.Uint32(fieldData))
					} else if len(fieldData) == 8 {
						iv = int64(binary.LittleEndian.Uint64(fieldData))
					} else {
						return 0, errors.New("invalid integer length")
					}
					if f.Extra > 0 {
						if err := rawSetField(raw, target, f.Name, moelua.Float(float64(iv)/float64(f.Extra))); err != nil {
							return 0, err
						}
					} else {
						if err := rawSetField(raw, target, f.Name, moelua.Int(iv)); err != nil {
							return 0, err
						}
					}
				case TypeDouble:
					if len(fieldData) != 8 {
						return 0, errors.New("invalid double length")
					}
					f64 := math.Float64frombits(binary.LittleEndian.Uint64(fieldData))
					if err := rawSetField(raw, target, f.Name, moelua.Float(f64)); err != nil {
						return 0, err
					}
				case TypeString:
					ls, err := raw.TryIntern(string(fieldData))
					if err != nil {
						return 0, err
					}
					if err := rawSetField(raw, target, f.Name, vm.StringValue(ls)); err != nil {
						return 0, err
					}
				case TypeStruct:
					sub, err := raw.TryNewTable(0, len(f.St.Fields))
					if err != nil {
						return 0, err
					}
					subVal := vm.TableValue(sub)
					sref := raw.Ref(subVal)
					nread, err := decodeTable(raw, f.St, fieldData, sub, depth+1)
					if err != nil || nread != len(fieldData) {
						raw.Unref(sref)
						return 0, errors.New("invalid nested struct")
					}
					err = rawSetField(raw, target, f.Name, subVal)
					raw.Unref(sref)
					if err != nil {
						return 0, err
					}
				default:
					return 0, errors.New("invalid field type")
				}
			}
		} else {
			switch f.Type {
			case TypeInteger:
				if f.Extra > 0 {
					if err := rawSetField(raw, target, f.Name, moelua.Float(float64(val)/float64(f.Extra))); err != nil {
						return 0, err
					}
				} else {
					if err := rawSetField(raw, target, f.Name, moelua.Int(int64(val))); err != nil {
						return 0, err
					}
				}
			case TypeBoolean:
				if err := rawSetField(raw, target, f.Name, moelua.Bool(val != 0)); err != nil {
					return 0, err
				}
			default:
				return 0, errors.New("inline value for non-int/bool field")
			}
		}
	}
	return pos, nil
}

func decodeArrayField(raw *vm.State, f *Field, bodyRaw []byte, depth int) (moelua.Value, error) {
	arr, err := raw.TryNewTable(4, 0)
	if err != nil {
		return moelua.Nil(), err
	}
	arrVal := vm.TableValue(arr)
	aref := raw.Ref(arrVal)
	defer raw.Unref(aref)

	if len(bodyRaw) == 0 {
		return arrVal, nil
	}
	baseType := f.Type &^ TypeArray
	switch baseType {
	case TypeInteger, TypeDouble:
		if len(bodyRaw) == 1 {
			return arrVal, nil
		}
		elemLen := int(bodyRaw[0])
		body := bodyRaw[1:]
		if elemLen == 4 {
			if len(body)%4 != 0 {
				return moelua.Nil(), errors.New("invalid int32 array length")
			}
			for i := 0; i < len(body)/4; i++ {
				iv := expandSign64(binary.LittleEndian.Uint32(body[i*4 : i*4+4]))
				if baseType == TypeInteger && f.Extra > 0 {
					arr.RawSetInt(raw, int64(i+1), moelua.Float(float64(iv)/float64(f.Extra)))
				} else {
					arr.RawSetInt(raw, int64(i+1), moelua.Int(iv))
				}
			}
		} else if elemLen == 8 {
			if len(body)%8 != 0 {
				return moelua.Nil(), errors.New("invalid int64 array length")
			}
			for i := 0; i < len(body)/8; i++ {
				u64 := binary.LittleEndian.Uint64(body[i*8 : i*8+8])
				if baseType == TypeDouble {
					arr.RawSetInt(raw, int64(i+1), moelua.Float(math.Float64frombits(u64)))
				} else if f.Extra > 0 {
					arr.RawSetInt(raw, int64(i+1), moelua.Float(float64(int64(u64))/float64(f.Extra)))
				} else {
					arr.RawSetInt(raw, int64(i+1), moelua.Int(int64(u64)))
				}
			}
		} else {
			return moelua.Nil(), errors.New("invalid integer array element size")
		}
		if err := raw.ConsumeMemErr(); err != nil {
			return moelua.Nil(), err
		}
		return arrVal, nil

	case TypeBoolean:
		for i, b := range bodyRaw {
			arr.RawSetInt(raw, int64(i+1), moelua.Bool(b != 0))
		}
		if err := raw.ConsumeMemErr(); err != nil {
			return moelua.Nil(), err
		}
		return arrVal, nil

	case TypeString:
		pos := 0
		idx := int64(1)
		for pos < len(bodyRaw) {
			if pos+4 > len(bodyRaw) {
				return moelua.Nil(), errors.New("truncated string array")
			}
			slen := uint64(binary.LittleEndian.Uint32(bodyRaw[pos : pos+4]))
			pos += 4
			if slen > uint64(len(bodyRaw)-pos) {
				return moelua.Nil(), errors.New("truncated string element")
			}
			ls, err := raw.TryIntern(string(bodyRaw[pos : pos+int(slen)]))
			if err != nil {
				return moelua.Nil(), err
			}
			sv := vm.StringValue(ls)
			sref := raw.Ref(sv)
			arr.RawSetInt(raw, idx, sv)
			raw.Unref(sref)
			if err := raw.ConsumeMemErr(); err != nil {
				return moelua.Nil(), err
			}
			pos += int(slen)
			idx++
		}
		return arrVal, nil

	case TypeStruct:
		pos := 0
		idx := int64(1)
		var mainKeyName string
		if f.Key >= 0 {
			if kf := f.St.findTag(f.Key); kf != nil {
				mainKeyName = kf.Name
			}
		}
		for pos < len(bodyRaw) {
			if pos+4 > len(bodyRaw) {
				return moelua.Nil(), errors.New("truncated struct array")
			}
			slen := uint64(binary.LittleEndian.Uint32(bodyRaw[pos : pos+4]))
			pos += 4
			if slen > uint64(len(bodyRaw)-pos) {
				return moelua.Nil(), errors.New("truncated struct element")
			}
			elemBytes := bodyRaw[pos : pos+int(slen)]
			pos += int(slen)

			sub, err := raw.TryNewTable(0, len(f.St.Fields))
			if err != nil {
				return moelua.Nil(), err
			}
			subVal := vm.TableValue(sub)
			sref := raw.Ref(subVal)
			nread, err := decodeTable(raw, f.St, elemBytes, sub, depth+1)
			if err != nil || nread != len(elemBytes) {
				raw.Unref(sref)
				return moelua.Nil(), errors.New("invalid struct array element")
			}
			if f.Key >= 0 {
				if f.Map {
					if len(f.St.Fields) < 2 {
						raw.Unref(sref)
						return moelua.Nil(), fmt.Errorf("invalid map struct in [%s]", f.Name)
					}
					kls, ok1 := raw.LookupString(f.St.Fields[0].Name)
					vls, ok2 := raw.LookupString(f.St.Fields[1].Name)
					if !ok1 || !ok2 {
						raw.Unref(sref)
						return moelua.Nil(), fmt.Errorf("Can't find key/value field in [%s]", f.Name)
					}
					kv := sub.RawGetStr(raw, kls)
					vv := sub.RawGetStr(raw, vls)
					if kv.IsNil() {
						raw.Unref(sref)
						return moelua.Nil(), fmt.Errorf("Can't find key field in [%s]", f.Name)
					}
					if vv.IsNil() {
						raw.Unref(sref)
						return moelua.Nil(), fmt.Errorf("Can't find value field in [%s]", f.Name)
					}
					_ = arr.RawSet(raw, kv, vv)
				} else {
					kls, ok := raw.LookupString(mainKeyName)
					if !ok {
						raw.Unref(sref)
						return moelua.Nil(), fmt.Errorf("Can't find main index (tag=%d) in [%s]", f.Key, f.Name)
					}
					kv := sub.RawGetStr(raw, kls)
					if kv.IsNil() {
						raw.Unref(sref)
						return moelua.Nil(), fmt.Errorf("Can't find main index (tag=%d) in [%s]", f.Key, f.Name)
					}
					_ = arr.RawSet(raw, kv, subVal)
				}
			} else {
				arr.RawSetInt(raw, idx, subVal)
			}
			raw.Unref(sref)
			if err := raw.ConsumeMemErr(); err != nil {
				return moelua.Nil(), err
			}
			idx++
		}
		return arrVal, nil
	}
	return moelua.Nil(), errors.New("invalid array element type")
}
