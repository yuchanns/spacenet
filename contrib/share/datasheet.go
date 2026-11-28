package share

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/runtime"
)

const (
	dsNodeCache     = "_ctable"
	dsProxyCache    = "_proxy"
	dsProxyMeta     = "_datasheet_proxy_mt"
	dsProxyUDMeta   = "_datasheet_proxy_ud_mt"
	dsDocHandleMeta = "_datasheet_doc_mt"

	dsValNil     = 0
	dsValInteger = 1
	dsValReal    = 2
	dsValBoolean = 3
	dsValTable   = 4
	dsValString  = 5

	dsInvalidOffset = 0xffffffff
)

type dsDoc struct {
	mu   sync.Mutex
	refs int64
	tok  uint64
	node *runtime.Node
	data []byte
}

func newDSDoc(node *runtime.Node, data []byte) *dsDoc {
	d := &dsDoc{
		refs: 1,
		node: node,
		data: data,
	}
	d.mu.Lock()
	d.tok = node.Objs().Put(runtime.KindDatasheetDoc, d)
	d.mu.Unlock()
	return d
}

func (d *dsDoc) token() uint64 {
	d.mu.Lock()
	tok := d.tok
	d.mu.Unlock()
	return tok
}

func (d *dsDoc) bytes() []byte {
	d.mu.Lock()
	data := d.data
	d.mu.Unlock()
	return data
}

func (d *dsDoc) retain() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.refs <= 0 {
		return false
	}
	d.refs++
	return true
}

func (d *dsDoc) release() {
	d.mu.Lock()
	if d.refs <= 0 {
		d.mu.Unlock()
		return
	}
	d.refs--
	dead := d.refs == 0
	tok := d.tok
	if dead {
		d.data = nil
	}
	d.mu.Unlock()
	if dead && tok != 0 {
		d.node.Objs().Delete(runtime.KindDatasheetDoc, tok)
	}
}

// dsDocHandle wraps a dsDoc owned by the builder actor. It implements
// runtime.TokenCarrier so skynet.pack serializes it as a cross-actor
// lightuserdata token while Lua GC releases the document once builder.lua's
// collection monitor drops it from its cache.
type dsDocHandle struct {
	doc *dsDoc
}

func (h *dsDocHandle) LightUDToken() uint64 {
	if h.doc == nil {
		return 0
	}
	return h.doc.token()
}

type dsProxy struct {
	doc   *dsDoc
	index int
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

func extractDocToken(st *moelua.State, v moelua.Value) (uint64, bool) {
	if tok, ok := runtime.ExtractToken(v); ok {
		return tok, tok != 0
	}
	if h, ok := st.CheckUserdata[*dsDocHandle](v, dsDocHandleMeta); ok {
		tok := h.LightUDToken()
		return tok, tok != 0
	}
	return 0, false
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

	docHandleMT, created := s.NewMetatable(dsDocHandleMeta)
	if created {
		_ = docHandleMT.SetString("__gc", s.NewFunc("datasheet.doc.__gc", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
			if len(args) == 0 {
				return nil, nil
			}
			if h, ok := st.CheckUserdata[*dsDocHandle](args[0], dsDocHandleMeta); ok {
				if h.doc != nil {
					h.doc.release()
					h.doc = nil
				}
			}
			return nil, nil
		}))
	}

	proxyUDMT, created := s.NewMetatable(dsProxyUDMeta)
	if created {
		_ = proxyUDMT.SetString("__gc", s.NewFunc("datasheet.proxy.__gc", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
			if len(args) == 0 {
				return nil, nil
			}
			if p, ok := st.CheckUserdata[*dsProxy](args[0], dsProxyUDMeta); ok {
				if p.doc != nil {
					p.doc.release()
					p.doc = nil
				}
			}
			return nil, nil
		}))
	}

	var createProxy func(st *moelua.State, doc *dsDoc, index int) (moelua.Value, error)
	var copyTable func(st *moelua.State, tbl *moelua.Table, p *dsProxy) error

	getAndRetainDoc := func(docTok uint64) (*dsDoc, error) {
		doc, ok := node.Objs().Get[*dsDoc](runtime.KindDatasheetDoc, docTok)
		if !ok {
			return nil, errors.New("invalid datasheet document pointer")
		}
		if !doc.retain() {
			return nil, errors.New("invalid datasheet document pointer")
		}
		return doc, nil
	}

	createProxy = func(st *moelua.State, doc *dsDoc, index int) (moelua.Value, error) {
		if doc == nil {
			return moelua.Nil(), errors.New("invalid datasheet document pointer")
		}
		docBytes := doc.bytes()
		if docBytes == nil {
			return moelua.Nil(), errors.New("invalid datasheet document pointer")
		}
		if _, ok := dsTableOffset(docBytes, index); !ok {
			return moelua.Nil(), fmt.Errorf("Invalid index %d", index)
		}
		nc := st.TableOf(st.Registry().GetString(dsNodeCache))
		key := dsCacheKey(doc.token(), index)
		if existing := nc.GetString(key); existing.IsTable() {
			return existing, nil
		}
		if !doc.retain() {
			return moelua.Nil(), errors.New("invalid datasheet document pointer")
		}

		t := st.NewTable(0, 4)
		hT := st.Pin(t.Value())
		defer hT.Release()

		pUDMT := st.GetMetatableByName(dsProxyUDMeta)
		proxyUD, err := st.NewUserdataWithMeta(&dsProxy{doc: doc, index: index}, pUDMT)
		if err != nil {
			doc.release()
			return moelua.Nil(), err
		}
		hP := st.Pin(proxyUD)
		defer hP.Release()

		mt := st.GetMetatableByName(dsProxyMeta)
		_ = t.SetMetatable(mt)
		_ = nc.SetString(key, t.Value())

		pc := st.TableOf(st.Registry().GetString(dsProxyCache))
		_ = pc.Set(t.Value(), proxyUD)
		return t.Value(), nil
	}

	copyTable = func(st *moelua.State, tbl *moelua.Table, p *dsProxy) error {
		if p.doc == nil {
			return fmt.Errorf("Invalid proxy (index = %d)", p.index)
		}
		doc := p.doc.bytes()
		if doc == nil {
			return errors.New("invalid datasheet document pointer")
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
				return createProxy(st, p.doc, int(raw))
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
		p, ok := st.CheckUserdata[*dsProxy](pv, dsProxyUDMeta)
		if !ok {
			return errors.New("Invalid proxy table")
		}
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
		doc := newDSDoc(node, []byte(str))
		hMT := st.GetMetatableByName(dsDocHandleMeta)
		ud, err := st.NewUserdataWithMeta(&dsDocHandle{doc: doc}, hMT)
		if err != nil {
			doc.release()
			return nil, err
		}
		return []moelua.Value{ud}, nil
	}))

	_ = mod.SetString("new", s.NewFunc("datasheet.new", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("datasheet.new requires document pointer")
		}
		docTok, ok := extractDocToken(st, args[0])
		if !ok {
			return nil, errors.New("bad argument #1 to 'new' (lightuserdata expected)")
		}
		doc, err := getAndRetainDoc(docTok)
		if err != nil {
			return nil, err
		}
		defer doc.release()
		proxy, err := createProxy(st, doc, 0)
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
		rootProxy, ok := st.CheckUserdata[*dsProxy](pv, dsProxyUDMeta)
		if !ok {
			return nil, errors.New("Invalid proxy table")
		}
		oldDoc := rootProxy.doc
		if oldDoc == nil {
			return nil, errors.New("Invalid proxy table")
		}

		newDocTok, ok := extractDocToken(st, args[1])
		if !ok {
			return nil, errors.New("bad argument #2 to 'update' (lightuserdata expected)")
		}
		newDoc, err := getAndRetainDoc(newDocTok)
		if err != nil {
			return nil, err
		}
		defer newDoc.release()

		if oldDoc == newDoc {
			return []moelua.Value{args[0]}, nil
		}
		if !oldDoc.retain() {
			return nil, errors.New("invalid datasheet document pointer")
		}
		defer oldDoc.release()

		newDocBytes := newDoc.bytes()
		if newDocBytes == nil {
			return nil, errors.New("invalid datasheet document pointer")
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
			p, ok := st.CheckUserdata[*dsProxy](pv2, dsProxyUDMeta)
			if !ok {
				return nil
			}
			if p.doc != oldDoc {
				return nil
			}
			tbl := st.TableOf(v)
			clearLuaTable(tbl)
			_ = tbl.SetMetatable(proxyMT)
			_, valid := dsTableOffset(newDocBytes, p.index)
			if valid && newDoc.retain() {
				p.doc = newDoc
			} else {
				p.doc = nil
				valid = false
			}
			oldDoc.release()
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
		if rootProxy.doc == oldDoc {
			tbl := st.TableOf(args[0])
			clearLuaTable(tbl)
			_ = tbl.SetMetatable(proxyMT)
			if _, valid := dsTableOffset(newDocBytes, rootProxy.index); valid && newDoc.retain() {
				rootProxy.doc = newDoc
				_ = nc.SetString(dsCacheKey(newDocTok, rootProxy.index), args[0])
			} else {
				rootProxy.doc = nil
			}
			oldDoc.release()
		}
		return []moelua.Value{args[0]}, nil
	}))

	return []moelua.Value{mod.Value()}, nil
}
