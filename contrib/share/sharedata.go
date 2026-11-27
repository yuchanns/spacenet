package share

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/runtime"
)

func toIntegralKey(v moelua.Value) (int64, bool) {
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

const metaShareDataCtrl = "confctrl"

type sdValKind uint8

const (
	sdKindNil sdValKind = iota
	sdKindReal
	sdKindString
	sdKindBool
	sdKindTable
	sdKindInt
)

type sdValue struct {
	kind sdValKind
	f    float64
	i    int64
	s    string
	b    bool
	tbl  *sdTable
}

type sdKey struct {
	isInt bool
	i     int64
	s     string
}

type sdEntry struct {
	key sdKey
	val sdValue
}

type sdRootState struct {
	mu    sync.Mutex
	dirty atomic.Bool
	ref   atomic.Int32
	toks  []uint64
}

type sdTable struct {
	state   *sdRootState
	tok     uint64
	arr     []sdValue
	hash    []sdEntry
	hashIdx map[sdKey]int
}

type sdCtrl struct {
	root      *sdTable
	update    *sdTable
	updateTbl *moelua.Handle
}

func buildSDTable(node *runtime.Node, state *sdRootState, s *moelua.State, tbl *moelua.Table, visiting map[uintptr]bool) (*sdTable, error) {
	id := uintptr(tbl.Value().Ref())
	if visiting[id] {
		return nil, errors.New("cyclic table reference is not supported in sharedata")
	}
	visiting[id] = true
	defer delete(visiting, id)

	arrLen := int(tbl.Len())
	if arrLen < 0 {
		arrLen = 0
	}
	out := &sdTable{
		state:   state,
		hashIdx: make(map[sdKey]int),
	}
	if arrLen > 0 {
		out.arr = make([]sdValue, arrLen)
	}

	var convVal func(v moelua.Value) (sdValue, error)
	convVal = func(v moelua.Value) (sdValue, error) {
		switch {
		case v.IsNil():
			return sdValue{kind: sdKindNil}, nil
		case v.IsInt():
			return sdValue{kind: sdKindInt, i: v.Int()}, nil
		case v.IsFloat():
			return sdValue{kind: sdKindReal, f: v.Float()}, nil
		case v.IsString():
			str, _ := s.ToString(v)
			return sdValue{kind: sdKindString, s: str}, nil
		case v.IsBool():
			return sdValue{kind: sdKindBool, b: v.IsTrue()}, nil
		case v.IsTable():
			sub, err := buildSDTable(node, state, s, s.TableOf(v), visiting)
			if err != nil {
				return sdValue{}, err
			}
			return sdValue{kind: sdKindTable, tbl: sub}, nil
		default:
			return sdValue{}, fmt.Errorf("Unsupport value type %s", v.TypeName())
		}
	}

	err := tbl.ForEach(func(k, v moelua.Value) error {
		sv, err := convVal(v)
		if err != nil {
			return err
		}
		if ik, ok := toIntegralKey(k); ok {
			if ik > 0 && ik <= int64(arrLen) {
				out.arr[ik-1] = sv
				return nil
			}
			key := sdKey{isInt: true, i: ik}
			out.hashIdx[key] = len(out.hash)
			out.hash = append(out.hash, sdEntry{key: key, val: sv})
		} else if k.IsFloat() {
			return fmt.Errorf("Invalid key %f", k.Float())
		} else if k.IsString() {
			ks, _ := s.ToString(k)
			key := sdKey{isInt: false, s: ks}
			out.hashIdx[key] = len(out.hash)
			out.hash = append(out.hash, sdEntry{key: key, val: sv})
		} else {
			return fmt.Errorf("Invalid key type %s", k.TypeName())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out.tok = node.Objs().Put(runtime.KindShareDataNode, out)
	state.toks = append(state.toks, out.tok)
	return out, nil
}

func pushSDValue(s *moelua.State, sv sdValue) moelua.Value {
	switch sv.kind {
	case sdKindReal:
		return moelua.Float(sv.f)
	case sdKindInt:
		return moelua.Int(sv.i)
	case sdKindString:
		return s.String(sv.s)
	case sdKindBool:
		return moelua.Bool(sv.b)
	case sdKindTable:
		return runtime.LightUDToken(sv.tbl.tok)
	default:
		return moelua.Nil()
	}
}

func pushSDKey(s *moelua.State, k sdKey) moelua.Value {
	if k.isInt {
		return moelua.Int(k.i)
	}
	return s.String(k.s)
}

func preloadShareDataCore(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, err
	}
	node := actor.Node()

	getSDTable := func(args []moelua.Value, idx int) (*sdTable, error) {
		if idx >= len(args) {
			return nil, errors.New("Need a conf object")
		}
		tok, ok := runtime.ExtractToken(args[idx])
		if !ok || tok == 0 {
			return nil, errors.New("Need a conf object")
		}
		v, ok := node.Objs().Get(runtime.KindShareDataNode, tok)
		if !ok {
			return nil, errors.New("Need a conf object")
		}
		return v.(*sdTable), nil
	}

	tbl := s.NewTable(0, 14)

	_ = tbl.SetString("new", s.NewFunc("sharedata.new", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 || !args[0].IsTable() {
			return nil, errors.New("bad argument #1 to 'new' (table expected)")
		}
		state := &sdRootState{}
		root, err := buildSDTable(node, state, st, st.TableOf(args[0]), make(map[uintptr]bool))
		if err != nil {
			for _, tok := range state.toks {
				node.Objs().Delete(runtime.KindShareDataNode, tok)
			}
			return nil, err
		}
		return []moelua.Value{runtime.LightUDToken(root.tok)}, nil
	}))

	_ = tbl.SetString("delete", s.NewFunc("sharedata.delete", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		t, err := getSDTable(args, 0)
		if err != nil {
			return nil, err
		}
		t.state.mu.Lock()
		toks := t.state.toks
		t.state.toks = nil
		t.state.mu.Unlock()
		for _, tok := range toks {
			node.Objs().Delete(runtime.KindShareDataNode, tok)
		}
		return nil, nil
	}))

	_ = tbl.SetString("markdirty", s.NewFunc("sharedata.markdirty", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		t, err := getSDTable(args, 0)
		if err != nil {
			return nil, err
		}
		t.state.dirty.Store(true)
		return nil, nil
	}))

	_ = tbl.SetString("isdirty", s.NewFunc("sharedata.isdirty", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		t, err := getSDTable(args, 0)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{moelua.Bool(t.state.dirty.Load())}, nil
	}))

	_ = tbl.SetString("getref", s.NewFunc("sharedata.getref", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		t, err := getSDTable(args, 0)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{moelua.Int(int64(t.state.ref.Load()))}, nil
	}))

	_ = tbl.SetString("incref", s.NewFunc("sharedata.incref", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		t, err := getSDTable(args, 0)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{moelua.Int(int64(t.state.ref.Add(1)))}, nil
	}))

	_ = tbl.SetString("decref", s.NewFunc("sharedata.decref", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		t, err := getSDTable(args, 0)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{moelua.Int(int64(t.state.ref.Add(-1)))}, nil
	}))

	_ = tbl.SetString("box", s.NewFunc("sharedata.box", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		t, err := getSDTable(args, 0)
		if err != nil {
			return nil, err
		}
		t.state.ref.Add(1)
		mt, created := st.NewMetatable(metaShareDataCtrl)
		if created {
			_ = mt.SetString("__gc", st.NewFunc("sharedata.ctrl.__gc", func(gst *moelua.State, gargs []moelua.Value) ([]moelua.Value, error) {
				if len(gargs) == 0 {
					return nil, nil
				}
				if v, ok := gst.CheckUserdata(gargs[0], metaShareDataCtrl); ok {
					c := v.(*sdCtrl)
					if c.root != nil {
						c.root.state.ref.Add(-1)
						c.root = nil
					}
					if c.updateTbl != nil {
						c.updateTbl.Release()
						c.updateTbl = nil
					}
					c.update = nil
				}
				return nil, nil
			}))
		}
		ud, err := st.NewUserdataWithMeta(&sdCtrl{root: t}, mt)
		if err != nil {
			t.state.ref.Add(-1)
			return nil, err
		}
		return []moelua.Value{ud}, nil
	}))

	_ = tbl.SetString("index", s.NewFunc("sharedata.index", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		t, err := getSDTable(args, 0)
		if err != nil {
			return nil, err
		}
		if len(args) < 2 || args[1].IsNil() {
			return nil, nil
		}
		k := args[1]
		if ik, ok := toIntegralKey(k); ok {
			if ik > 0 && ik <= int64(len(t.arr)) {
				return []moelua.Value{pushSDValue(st, t.arr[ik-1])}, nil
			}
			if idx, ok := t.hashIdx[sdKey{isInt: true, i: ik}]; ok {
				return []moelua.Value{pushSDValue(st, t.hash[idx].val)}, nil
			}
			return nil, nil
		}
		if k.IsFloat() {
			return nil, fmt.Errorf("Invalid key %f", k.Float())
		}
		ks, ok := st.ToString(k)
		if !ok {
			return nil, errors.New("Invalid key")
		}
		if idx, ok := t.hashIdx[sdKey{isInt: false, s: ks}]; ok {
			return []moelua.Value{pushSDValue(st, t.hash[idx].val)}, nil
		}
		return nil, nil
	}))

	_ = tbl.SetString("nextkey", s.NewFunc("sharedata.nextkey", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		t, err := getSDTable(args, 0)
		if err != nil {
			return nil, err
		}
		firstHash := func() ([]moelua.Value, error) {
			if len(t.hash) > 0 {
				return []moelua.Value{pushSDKey(st, t.hash[0].key)}, nil
			}
			return nil, nil
		}
		if len(args) < 2 || args[1].IsNil() {
			for i, sv := range t.arr {
				if sv.kind != sdKindNil {
					return []moelua.Value{moelua.Int(int64(i + 1))}, nil
				}
			}
			return firstHash()
		}
		k := args[1]
		var lookupKey sdKey
		if ik, ok := toIntegralKey(k); ok {
			if ik > 0 && ik <= int64(len(t.arr)) {
				for i := int(ik); i < len(t.arr); i++ {
					if t.arr[i].kind != sdKindNil {
						return []moelua.Value{moelua.Int(int64(i + 1))}, nil
					}
				}
				return firstHash()
			}
			lookupKey = sdKey{isInt: true, i: ik}
		} else if k.IsFloat() {
			return nil, nil
		} else {
			ks, ok := st.ToString(k)
			if !ok {
				return nil, nil
			}
			lookupKey = sdKey{isInt: false, s: ks}
		}
		idx, ok := t.hashIdx[lookupKey]
		if !ok || idx+1 >= len(t.hash) {
			return nil, nil
		}
		return []moelua.Value{pushSDKey(st, t.hash[idx+1].key)}, nil
	}))

	_ = tbl.SetString("len", s.NewFunc("sharedata.len", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		t, err := getSDTable(args, 0)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{moelua.Int(int64(len(t.arr)))}, nil
	}))

	_ = tbl.SetString("hashlen", s.NewFunc("sharedata.hashlen", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		t, err := getSDTable(args, 0)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{moelua.Int(int64(len(t.hash)))}, nil
	}))

	_ = tbl.SetString("needupdate", s.NewFunc("sharedata.needupdate", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) == 0 {
			return nil, nil
		}
		v, ok := st.CheckUserdata(args[0], metaShareDataCtrl)
		if !ok {
			return nil, nil
		}
		c := v.(*sdCtrl)
		if c.update != nil {
			var uv moelua.Value = moelua.Nil()
			if c.updateTbl != nil {
				uv = c.updateTbl.Value()
			}
			return []moelua.Value{runtime.LightUDToken(c.update.tok), uv}, nil
		}
		return nil, nil
	}))

	_ = tbl.SetString("update", s.NewFunc("sharedata.update", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 3 || !args[2].IsTable() {
			return nil, errors.New("sharedata.update requires (ctrl, newobj, table)")
		}
		v, ok := st.CheckUserdata(args[0], metaShareDataCtrl)
		if !ok {
			return nil, errors.New("expected confctrl userdata")
		}
		c := v.(*sdCtrl)
		nt, err := getSDTable(args, 1)
		if err != nil {
			return nil, err
		}
		if c.root == nt {
			return nil, errors.New("You should update a new object")
		}
		if c.updateTbl != nil {
			c.updateTbl.Release()
		}
		c.updateTbl = st.Pin(args[2])
		c.update = nt
		return nil, nil
	}))

	return []moelua.Value{tbl.Value()}, nil
}
