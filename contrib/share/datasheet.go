package share

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/runtime"
)

const (
	dsNodeCache  = "_ctable"
	dsProxyCache = "_proxy"
	dsTables     = "_ctables"
	dsProxyMeta  = "_datasheet_proxy_mt"

	dsValNil     = 0
	dsValInteger = 1
	dsValReal    = 2
	dsValBoolean = 3
	dsValTable   = 4
	dsValString  = 5

	dsInvalidOffset = 0xffffffff
)

type dsDoc struct {
	data []byte
}

type dsProxy struct {
	docTok uint64
	index  int
}

func dsCacheKey(docTok uint64, index int) string {
	return fmt.Sprintf("%x:%d", docTok, index)
}

func dsTableOffset(doc []byte, index int) (int, bool) {
	if len(doc) < 8 || index < 0 {
		return 0, false
	}
	n := uint64(binary.LittleEndian.Uint32(doc[4:8]))
	if uint64(index) >= n {
		return 0, false
	}
	idxPos := 8 + uint64(index)*4
	if idxPos+4 > uint64(len(doc)) {
		return 0, false
	}
	off := binary.LittleEndian.Uint32(doc[idxPos : idxPos+4])
	if off == dsInvalidOffset {
		return 0, false
	}
	base := 8 + n*4 + uint64(off)
	if base+8 > uint64(len(doc)) {
		return 0, false
	}
	return int(base), true
}

func dsReadCString(doc []byte, strtbl uint32, off uint32) string {
	start := uint64(strtbl) + uint64(off)
	if start >= uint64(len(doc)) {
		return ""
	}
	rest := doc[int(start):]
	if z := bytes.IndexByte(rest, 0); z >= 0 {
		return string(rest[:z])
	}
	return string(rest)
}

func preloadDatasheetCore(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, err
	}
	node := actor.Node()

	newWeak := func(st *moelua.State, mode string) *moelua.Table {
		t := st.NewTable(0, 8)
		mt := st.NewTable(0, 1)
		_ = mt.SetString("__mode", st.String(mode))
		_ = t.SetMetatable(mt)
		return t
	}

	reg := s.Registry()
	if !reg.GetString(dsNodeCache).IsTable() {
		_ = reg.SetString(dsNodeCache, newWeak(s, "kv").Value())
	}
	if !reg.GetString(dsProxyCache).IsTable() {
		_ = reg.SetString(dsProxyCache, newWeak(s, "k").Value())
	}
	if !reg.GetString(dsTables).IsTable() {
		_ = reg.SetString(dsTables, s.NewTable(0, 8).Value())
	}

	var createProxy func(st *moelua.State, docTok uint64, index int) (moelua.Value, error)
	var copyTable func(st *moelua.State, tbl *moelua.Table, p *dsProxy) error

	getDoc := func(docTok uint64) ([]byte, error) {
		v, ok := node.Objs().Get(runtime.KindDatasheetDoc, docTok)
		if !ok {
			return nil, errors.New("invalid datasheet document pointer")
		}
		return v.(*dsDoc).data, nil
	}

	createProxy = func(st *moelua.State, docTok uint64, index int) (moelua.Value, error) {
		doc, err := getDoc(docTok)
		if err != nil {
			return moelua.Nil(), err
		}
		if _, ok := dsTableOffset(doc, index); !ok {
			return moelua.Nil(), fmt.Errorf("Invalid index %d", index)
		}
		nc := st.TableOf(st.Registry().GetString(dsNodeCache))
		key := dsCacheKey(docTok, index)
		if existing := nc.GetString(key); existing.IsTable() {
			return existing, nil
		}
		t := st.NewTable(0, 4)
		mt := st.GetMetatableByName(dsProxyMeta)
		_ = t.SetMetatable(mt)
		_ = nc.SetString(key, t.Value())

		pc := st.TableOf(st.Registry().GetString(dsProxyCache))
		_ = pc.Set(t.Value(), st.NewUserdata(&dsProxy{docTok: docTok, index: index}))
		return t.Value(), nil
	}

	copyTable = func(st *moelua.State, tbl *moelua.Table, p *dsProxy) error {
		doc, err := getDoc(p.docTok)
		if err != nil {
			return err
		}
		base, ok := dsTableOffset(doc, p.index)
		if !ok {
			return fmt.Errorf("Invalid proxy (index = %d)", p.index)
		}
		strtbl := binary.LittleEndian.Uint32(doc[0:4])
		arrU := uint64(binary.LittleEndian.Uint32(doc[base : base+4]))
		dictU := uint64(binary.LittleEndian.Uint32(doc[base+4 : base+8]))
		typesStartU := uint64(base) + 8
		typesPadded := (arrU + dictU + 3) &^ 3
		valsPosU := typesStartU + typesPadded
		valsEndU := valsPosU + arrU*4 + dictU*8
		if valsPosU < typesStartU || valsEndU < valsPosU || valsEndU > uint64(len(doc)) {
			return errors.New("corrupt or truncated datasheet table")
		}
		arr := int(arrU)
		dict := int(dictU)
		typesStart := int(typesStartU)
		valsPos := int(valsPosU)

		decodeVal := func(typ byte, raw uint32) (moelua.Value, error) {
			switch typ {
			case dsValNil:
				return moelua.Nil(), nil
			case dsValInteger:
				return moelua.Int(int64(int32(raw))), nil
			case dsValReal:
				return moelua.Float(float64(math.Float32frombits(raw))), nil
			case dsValBoolean:
				return moelua.Bool(raw != 0), nil
			case dsValTable:
				return createProxy(st, p.docTok, int(raw))
			case dsValString:
				return st.TryString(dsReadCString(doc, strtbl, raw))
			default:
				return moelua.Nil(), fmt.Errorf("Invalid datasheet value type %d", typ)
			}
		}

		for i := 0; i < arr; i++ {
			typ := doc[typesStart+i]
			raw := binary.LittleEndian.Uint32(doc[valsPos : valsPos+4])
			valsPos += 4
			val, err := decodeVal(typ, raw)
			if err != nil {
				return err
			}
			if err := tbl.SetInt(int64(i+1), val); err != nil {
				return err
			}
		}
		for i := 0; i < dict; i++ {
			kOff := binary.LittleEndian.Uint32(doc[valsPos : valsPos+4])
			vRaw := binary.LittleEndian.Uint32(doc[valsPos+4 : valsPos+8])
			valsPos += 8
			kStr := dsReadCString(doc, strtbl, kOff)
			val, err := decodeVal(doc[typesStart+arr+i], vRaw)
			if err != nil {
				return err
			}
			if err := tbl.SetString(kStr, val); err != nil {
				return err
			}
		}
		return nil
	}

	copyFromData := func(st *moelua.State, tblVal moelua.Value) error {
		pc := st.TableOf(st.Registry().GetString(dsProxyCache))
		pv := pc.Get(tblVal)
		if !pv.IsUserdata() {
			return errors.New("Invalid proxy table")
		}
		p := st.Raw().UserdataData(st.Raw().AsUserdata(pv)).(*dsProxy)
		tbl := st.TableOf(tblVal)
		if err := copyTable(st, tbl, p); err != nil {
			return err
		}
		return tbl.SetMetatable(nil)
	}

	mt, created := s.NewMetatable(dsProxyMeta)
	if created {
		_ = mt.SetString("__index", s.NewFunc("datasheet.__index", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
			if len(args) < 2 {
				return nil, errors.New("invalid proxy index arguments")
			}
			if err := copyFromData(st, args[0]); err != nil {
				return nil, err
			}
			return []moelua.Value{st.TableOf(args[0]).Get(args[1])}, nil
		}))
		lnext := s.NewFunc("datasheet.next", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
			if len(args) < 1 || !args[0].IsTable() {
				return nil, errors.New("expected table for datasheet next")
			}
			tbl := st.TableOf(args[0])
			var k moelua.Value
			if len(args) >= 2 {
				k = args[1]
			} else {
				k = moelua.Nil()
			}
			nk, nv, ok, err := tbl.Next(k)
			if err != nil {
				return nil, err
			}
			if !ok {
				return []moelua.Value{moelua.Nil()}, nil
			}
			return []moelua.Value{nk, nv}, nil
		})
		_ = mt.SetString("__pairs", s.NewFunc("datasheet.__pairs", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
			if len(args) < 1 {
				return nil, errors.New("invalid proxy pairs arguments")
			}
			if err := copyFromData(st, args[0]); err != nil {
				return nil, err
			}
			return []moelua.Value{lnext, args[0], moelua.Nil()}, nil
		}))
		_ = mt.SetString("__len", s.NewFunc("datasheet.__len", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
			if len(args) < 1 {
				return nil, errors.New("invalid proxy len arguments")
			}
			if err := copyFromData(st, args[0]); err != nil {
				return nil, err
			}
			return []moelua.Value{moelua.Int(st.TableOf(args[0]).Len())}, nil
		}))
	}

	clearLuaTable := func(t *moelua.Table) {
		var keys []moelua.Value
		_ = t.ForEach(func(k, _ moelua.Value) error {
			keys = append(keys, k)
			return nil
		})
		for _, k := range keys {
			_ = t.Set(k, moelua.Nil())
		}
	}

	mod := s.NewTable(0, 3)
	_ = mod.SetString("stringpointer", s.NewFunc("datasheet.stringpointer", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("stringpointer requires string")
		}
		str, ok := st.ToString(args[0])
		if !ok {
			return nil, errors.New("bad argument #1 to 'stringpointer' (string expected)")
		}
		tok := node.Objs().Put(runtime.KindDatasheetDoc, &dsDoc{data: []byte(str)})
		return []moelua.Value{runtime.LightUDToken(tok)}, nil
	}))

	_ = mod.SetString("new", s.NewFunc("datasheet.new", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("datasheet.new requires document pointer")
		}
		docTok, ok := runtime.ExtractToken(args[0])
		if !ok || docTok == 0 {
			return nil, errors.New("bad argument #1 to 'new' (lightuserdata expected)")
		}
		tables := st.TableOf(st.Registry().GetString(dsTables))
		_ = tables.Set(args[0], args[0])
		proxy, err := createProxy(st, docTok, 0)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{proxy}, nil
	}))

	_ = mod.SetString("update", s.NewFunc("datasheet.update", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 2 {
			return nil, errors.New("datasheet.update requires (proxy, newdata)")
		}
		pc := st.TableOf(st.Registry().GetString(dsProxyCache))
		pv := pc.Get(args[0])
		if !pv.IsUserdata() {
			return nil, errors.New("Invalid proxy table")
		}
		rootProxy := st.Raw().UserdataData(st.Raw().AsUserdata(pv)).(*dsProxy)
		oldDocTok := rootProxy.docTok

		newDocTok, ok := runtime.ExtractToken(args[1])
		if !ok || newDocTok == 0 {
			return nil, errors.New("bad argument #2 to 'update' (lightuserdata expected)")
		}
		newDoc, err := getDoc(newDocTok)
		if err != nil {
			return nil, err
		}

		nc := st.TableOf(st.Registry().GetString(dsNodeCache))
		proxyMT := st.GetMetatableByName(dsProxyMeta)

		type pendingMove struct {
			oldKey string
			newKey string
			tblVal moelua.Value
			valid  bool
		}
		var moves []pendingMove
		_ = nc.ForEach(func(k, v moelua.Value) error {
			ks, ok := st.ToString(k)
			if !ok || !v.IsTable() {
				return nil
			}
			pv2 := pc.Get(v)
			if !pv2.IsUserdata() {
				return nil
			}
			p := st.Raw().UserdataData(st.Raw().AsUserdata(pv2)).(*dsProxy)
			if p.docTok != oldDocTok {
				return nil
			}
			p.docTok = newDocTok
			tbl := st.TableOf(v)
			clearLuaTable(tbl)
			_ = tbl.SetMetatable(proxyMT)
			_, valid := dsTableOffset(newDoc, p.index)
			moves = append(moves, pendingMove{
				oldKey: ks,
				newKey: dsCacheKey(newDocTok, p.index),
				tblVal: v,
				valid:  valid,
			})
			return nil
		})
		for _, mv := range moves {
			_ = nc.SetString(mv.oldKey, moelua.Nil())
		}
		for _, mv := range moves {
			if mv.valid {
				_ = nc.SetString(mv.newKey, mv.tblVal)
			}
		}
		return []moelua.Value{args[0]}, nil
	}))

	return []moelua.Value{mod.Value()}, nil
}
