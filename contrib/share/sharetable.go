package share

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/runtime"
)

const (
	metaMatrixState      = "BOXMATRIXSTATE"
	regSharedTablesKey   = "__spacenet_sharedtables"
)

type stValKind uint8

const (
	stNil stValKind = iota
	stBool
	stInt
	stFloat
	stString
	stLightUD
	stTable
	stFunc
)

type stFuncData struct {
	chunk *moelua.Chunk
}

type stValue struct {
	kind stValKind
	b    bool
	i    int64
	f    float64
	s    string
	u    uint64
	tbl  *stTableData
	fn   *stFuncData
}

type stKV struct {
	k stValue
	v stValue
}

type stTableData struct {
	entries []stKV
	approxB int64
}

type captureState struct {
	tables map[uintptr]*stTableData
	funcs  map[uintptr]*stFuncData
}

type matState struct {
	s         *moelua.State
	sharedReg *moelua.Table
	tables    map[*stTableData]*moelua.Table
	funcs     map[*stFuncData]moelua.Value
	pins      []*moelua.Handle
}

func (m *matState) pin(v moelua.Value) {
	if h := m.s.Pin(v); h != nil {
		m.pins = append(m.pins, h)
	}
}

func (m *matState) releasePins() {
	for i := len(m.pins) - 1; i >= 0; i-- {
		m.pins[i].Release()
	}
	m.pins = nil
}

type matrixState struct {
	mu     sync.Mutex
	node   *runtime.Node
	tok    uint64
	root   *stTableData
	closed bool
}

func (ms *matrixState) close() {
	ms.mu.Lock()
	already := ms.closed
	ms.closed = true
	tok := ms.tok
	ms.tok = 0
	ms.root = nil
	ms.mu.Unlock()
	if !already && tok != 0 {
		ms.node.Objs().Delete(runtime.KindShareMatrix, tok)
	}
}

func captureTable(s *moelua.State, tbl *moelua.Table, cs *captureState) (*stTableData, error) {
	if tbl.Metatable() != nil {
		return nil, errors.New("Can't share metatable")
	}
	id := uintptr(tbl.Value().Ref())
	if existing, ok := cs.tables[id]; ok {
		return existing, nil
	}
	out := &stTableData{approxB: 64}
	cs.tables[id] = out

	err := tbl.ForEach(func(k, v moelua.Value) error {
		sk, err := captureVal(s, k, cs)
		if err != nil {
			return err
		}
		sv, err := captureVal(s, v, cs)
		if err != nil {
			return err
		}
		out.entries = append(out.entries, stKV{k: sk, v: sv})
		out.approxB += 32 + int64(len(sk.s)) + int64(len(sv.s))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func captureVal(s *moelua.State, v moelua.Value, cs *captureState) (stValue, error) {
	switch {
	case v.IsNil():
		return stValue{kind: stNil}, nil
	case v.IsBool():
		return stValue{kind: stBool, b: v.IsTrue()}, nil
	case v.IsInt():
		return stValue{kind: stInt, i: v.Int()}, nil
	case v.IsFloat():
		return stValue{kind: stFloat, f: v.Float()}, nil
	case v.IsString():
		str, _ := s.ToString(v)
		return stValue{kind: stString, s: str}, nil
	case v.IsLightUD():
		tok, _ := runtime.ExtractToken(v)
		return stValue{kind: stLightUD, u: tok}, nil
	case v.IsTable():
		td, err := captureTable(s, s.TableOf(v), cs)
		if err != nil {
			return stValue{}, err
		}
		return stValue{kind: stTable, tbl: td}, nil
	case v.IsClosure():
		id := uintptr(v.Ref())
		if existing, ok := cs.funcs[id]; ok {
			return stValue{kind: stFunc, fn: existing}, nil
		}
		// Verify zero upvalues (matching lua-sharetable.c:39) and dump bytecode chunk.
		dbg := s.TableOf(s.GetGlobal("debug"))
		if dbg != nil {
			if getup := dbg.GetString("getupvalue"); !getup.IsNil() {
				res, err := s.Call(getup, v, moelua.Int(1))
				if err == nil && len(res) > 0 && !res[0].IsNil() {
					return stValue{}, errors.New("Invalid function with upvalue")
				}
			}
		}
		strLib := s.TableOf(s.GetGlobal("string"))
		if strLib != nil {
			if dumpFn := strLib.GetString("dump"); !dumpFn.IsNil() {
				res, err := s.Call(dumpFn, v)
				if err != nil || len(res) == 0 {
					return stValue{}, errors.New("failed to dump shared function")
				}
				bin, _ := s.ToString(res[0])
				ch, err := moelua.LoadChunk([]byte(bin))
				if err != nil {
					return stValue{}, err
				}
				fd := &stFuncData{chunk: ch}
				cs.funcs[id] = fd
				return stValue{kind: stFunc, fn: fd}, nil
			}
		}
		return stValue{}, errors.New("Invalid type [function]")
	default:
		return stValue{}, fmt.Errorf("Invalid type [%s]", v.TypeName())
	}
}

func getSharedRegistry(s *moelua.State) *moelua.Table {
	reg := s.Registry()
	if reg == nil {
		return nil
	}
	v := reg.GetString(regSharedTablesKey)
	if v.IsTable() {
		return s.TableOf(v)
	}
	t := s.NewTable(0, 16)
	mt := s.NewTable(0, 1)
	_ = mt.SetString("__mode", s.String("k"))
	_ = t.SetMetatable(mt)
	_ = reg.SetString(regSharedTablesKey, t.Value())
	return t
}

func materializeTable(ms *matState, td *stTableData) (*moelua.Table, error) {
	if existing, ok := ms.tables[td]; ok {
		return existing, nil
	}
	t, err := ms.s.TryNewTable(0, len(td.entries))
	if err != nil {
		return nil, err
	}
	ms.pin(t.Value())
	ms.tables[td] = t
	if ms.sharedReg != nil {
		_ = ms.sharedReg.Set(t.Value(), moelua.Bool(true))
	}
	for _, kv := range td.entries {
		k, err := materializeVal(ms, kv.k)
		if err != nil {
			return nil, err
		}
		kh := ms.s.Pin(k)
		v, err := materializeVal(ms, kv.v)
		if err != nil {
			kh.Release()
			return nil, err
		}
		err = t.Set(k, v)
		kh.Release()
		if err != nil {
			return nil, err
		}
	}
	return t, nil
}

func materializeVal(ms *matState, sv stValue) (moelua.Value, error) {
	switch sv.kind {
	case stNil:
		return moelua.Nil(), nil
	case stBool:
		return moelua.Bool(sv.b), nil
	case stInt:
		return moelua.Int(sv.i), nil
	case stFloat:
		return moelua.Float(sv.f), nil
	case stString:
		return ms.s.TryString(sv.s)
	case stLightUD:
		return runtime.LightUDToken(sv.u), nil
	case stTable:
		t, err := materializeTable(ms, sv.tbl)
		if err != nil {
			return moelua.Nil(), err
		}
		return t.Value(), nil
	case stFunc:
		if existing, ok := ms.funcs[sv.fn]; ok {
			return existing, nil
		}
		fn, err := ms.s.Load(sv.fn.chunk)
		if err != nil {
			return moelua.Nil(), err
		}
		ms.pin(fn)
		ms.funcs[sv.fn] = fn
		return fn, nil
	default:
		return moelua.Nil(), nil
	}
}

func preloadShareTableCore(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, err
	}
	node := actor.Node()

	tbl := s.NewTable(0, 4)

	_ = tbl.SetString("matrix", s.NewFunc("sharetable.matrix", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("matrix requires source")
		}
		source, ok := st.ToString(args[0])
		if !ok {
			return nil, errors.New("bad argument #1 to 'matrix' (string expected)")
		}

		// Execute matrix builder in an isolated MoeLua State with LayeredFS loaders
		// and actor cancellation/interrupt propagation.
		mL := moelua.New(
			moelua.WithNoEnv(),
			moelua.WithLibs(moelua.LibAll),
			moelua.WithContext(actor.Context()),
		)
		defer func() {
			mL.SetContext(nil)
			mL.Close()
		}()
		mL.Raw().SetInterruptFlag(actor.InterruptFlag())
		node.InstallFreshVFSLoaders(mL)
		if callerPkg := st.TableOf(st.GetGlobal("package")); callerPkg != nil {
			if callerPath, ok := st.ToString(callerPkg.GetString("path")); ok && callerPath != "" {
				if mPkg := mL.TableOf(mL.GetGlobal("package")); mPkg != nil {
					if defPath, ok := mL.ToString(mPkg.GetString("path")); ok && defPath != "" {
						_ = mPkg.SetString("path", mL.String(callerPath+";"+defPath))
					} else {
						_ = mPkg.SetString("path", mL.String(callerPath))
					}
				}
			}
		}

		var ch *moelua.Chunk
		if strings.HasPrefix(source, "@") {
			ch, err = node.VFS().LoadChunkUncached(source[1:])
			if err != nil {
				return nil, err
			}
		} else {
			ch, err = moelua.Compile("=(sharetable)", source)
			if err != nil {
				return nil, err
			}
		}
		fn, err := mL.Load(ch)
		if err != nil {
			return nil, err
		}

		// Marshal extra arguments into mL (supporting bool, number, string, lightud,
		// and skynet.unpack for sharetable.loadtable!).
		callArgs := make([]moelua.Value, 0, len(args)-1)
		for i := 1; i < len(args); i++ {
			a := args[i]
			switch {
			case a.IsNil():
				callArgs = append(callArgs, moelua.Nil())
			case a.IsBool():
				callArgs = append(callArgs, moelua.Bool(a.IsTrue()))
			case a.IsInt():
				callArgs = append(callArgs, moelua.Int(a.Int()))
			case a.IsFloat():
				callArgs = append(callArgs, moelua.Float(a.Float()))
			case a.IsString():
				str, _ := st.ToString(a)
				callArgs = append(callArgs, mL.String(str))
			case a.IsLightUD():
				tok, _ := runtime.ExtractToken(a)
				callArgs = append(callArgs, runtime.LightUDToken(tok))
			case a.IsNative():
				// Bridge spacenet.unpack into the isolated matrix VM for sharetable.loadtable.
				callArgs = append(callArgs, mL.NewFunc("spacenet.unpack", func(ms *moelua.State, uargs []moelua.Value) ([]moelua.Value, error) {
					if len(uargs) == 0 || uargs[0].IsNil() {
						return nil, nil
					}
					if uargs[0].IsString() {
						str, _ := ms.ToString(uargs[0])
						return runtime.UnpackLuaValues(ms, []byte(str))
					}
					tok, ok := runtime.ExtractToken(uargs[0])
					if !ok || tok == 0 {
						return nil, nil
					}
					sz := -1
					if len(uargs) >= 2 && uargs[1].IsNumber() {
						sz = int(toInt(uargs[1]))
					}
					data, ok := node.Msgs().Borrow(tok)
					if !ok {
						return nil, errors.New("invalid unpack buffer in sharetable matrix")
					}
					if sz >= 0 && sz < len(data) {
						data = data[:sz]
					}
					return runtime.UnpackLuaValues(ms, data)
				}))
			default:
				return nil, fmt.Errorf("bad argument #%d to 'matrix' (Type invalid: %s)", i+1, a.TypeName())
			}
		}

		res, err := mL.Call(fn, callArgs...)
		if err != nil {
			return nil, err
		}
		if len(res) == 0 || !res[0].IsTable() {
			got := "no value"
			if len(res) > 0 {
				got = res[0].TypeName()
			}
			return nil, fmt.Errorf("Not a table, it's a %s.", got)
		}

		// Pin the returned root table across captureTable, which may invoke
		// debug.getupvalue / string.dump (triggering post-Call GC steps).
		rootPin := mL.Pin(res[0])
		cs := &captureState{
			tables: make(map[uintptr]*stTableData),
			funcs:  make(map[uintptr]*stFuncData),
		}
		root, err := captureTable(mL, mL.TableOf(res[0]), cs)
		rootPin.Release()
		if err != nil {
			return nil, err
		}

		ms := &matrixState{
			node: node,
			root: root,
		}
		ms.tok = node.Objs().Put(runtime.KindShareMatrix, root)

		mt, created := st.NewMetatable(metaMatrixState)
		if created {
			_ = mt.SetString("__index", mt.Value())
			_ = mt.SetString("close", st.NewFunc("matrix.close", func(cst *moelua.State, cargs []moelua.Value) ([]moelua.Value, error) {
				if len(cargs) == 0 {
					return nil, nil
				}
				if m, ok := cst.CheckUserdata[*matrixState](cargs[0], metaMatrixState); ok {
					m.close()
				}
				return nil, nil
			}))
			_ = mt.SetString("getptr", st.NewFunc("matrix.getptr", func(cst *moelua.State, cargs []moelua.Value) ([]moelua.Value, error) {
				if len(cargs) == 0 {
					return nil, errors.New("expected BOXMATRIXSTATE")
				}
				m, ok := cst.CheckUserdata[*matrixState](cargs[0], metaMatrixState)
				if !ok {
					return nil, errors.New("expected BOXMATRIXSTATE")
				}
				m.mu.Lock()
				tok := m.tok
				closed := m.closed
				m.mu.Unlock()
				if closed || tok == 0 {
					return nil, nil
				}
				return []moelua.Value{runtime.LightUDToken(tok)}, nil
			}))
			_ = mt.SetString("size", st.NewFunc("matrix.size", func(cst *moelua.State, cargs []moelua.Value) ([]moelua.Value, error) {
				if len(cargs) == 0 {
					return nil, errors.New("expected BOXMATRIXSTATE")
				}
				m, ok := cst.CheckUserdata[*matrixState](cargs[0], metaMatrixState)
				if !ok {
					return nil, errors.New("expected BOXMATRIXSTATE")
				}
				m.mu.Lock()
				var sz int64
				if !m.closed && m.root != nil {
					sz = m.root.approxB
				}
				m.mu.Unlock()
				return []moelua.Value{moelua.Int(sz)}, nil
			}))
			_ = mt.SetString("__gc", st.NewFunc("matrix.__gc", func(cst *moelua.State, cargs []moelua.Value) ([]moelua.Value, error) {
				if len(cargs) == 0 {
					return nil, nil
				}
				if m, ok := cst.CheckUserdata[*matrixState](cargs[0], metaMatrixState); ok {
					m.close()
				}
				return nil, nil
			}))
		}
		ud, err := st.NewUserdataWithMeta(ms, mt)
		if err != nil {
			ms.close()
			return nil, err
		}
		return []moelua.Value{ud}, nil
	}))

	_ = tbl.SetString("clone", s.NewFunc("sharetable.clone", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("clone requires matrix pointer")
		}
		tok, ok := runtime.ExtractToken(args[0])
		if !ok || tok == 0 {
			return nil, errors.New("invalid matrix pointer")
		}
		root, ok := node.Objs().Get[*stTableData](runtime.KindShareMatrix, tok)
		if !ok {
			return nil, errors.New("matrix pointer not found or closed")
		}
		ms := &matState{
			s:         st,
			sharedReg: getSharedRegistry(st),
			tables:    make(map[*stTableData]*moelua.Table),
			funcs:     make(map[*stFuncData]moelua.Value),
		}
		defer ms.releasePins()
		t, err := materializeTable(ms, root)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{t.Value()}, nil
	}))

	_ = tbl.SetString("is_sharedtable", s.NewFunc("sharetable.is_sharedtable", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 || !args[0].IsTable() {
			return []moelua.Value{moelua.Bool(false)}, nil
		}
		sharedReg := getSharedRegistry(st)
		if sharedReg == nil {
			return []moelua.Value{moelua.Bool(false)}, nil
		}
		return []moelua.Value{moelua.Bool(!sharedReg.Get(args[0]).IsFalsy())}, nil
	}))

	_ = tbl.SetString("stackvalues", s.NewFunc("sharetable.stackvalues", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 || !args[0].IsThread() {
			return nil, errors.New("bad argument #1 to 'stackvalues' (thread expected)")
		}
		co := args[0]

		// Matching lualib-src/lua-sharetable.c:lco_stackvalues: return 0 for the
		// currently running coroutine; otherwise populate args[1] with stack values.
		if coroTbl := st.TableOf(st.GetGlobal("coroutine")); coroTbl != nil {
			if runningFn := coroTbl.GetString("running"); !runningFn.IsNil() {
				if res, err := st.Call(runningFn); err == nil && len(res) > 0 && st.RawEqual(co, res[0]) {
					return []moelua.Value{moelua.Int(0)}, nil
				}
			}
		}
		if len(args) < 2 || !args[1].IsTable() {
			return nil, errors.New("bad argument #2 to 'stackvalues' (table expected)")
		}
		out := st.TableOf(args[1])

		dbg := st.TableOf(st.GetGlobal("debug"))
		if dbg == nil {
			return []moelua.Value{moelua.Int(0)}, nil
		}
		getInfo := dbg.GetString("getinfo")
		getLocal := dbg.GetString("getlocal")
		if getInfo.IsNil() || getLocal.IsNil() {
			return []moelua.Value{moelua.Int(0)}, nil
		}

		var count int64
		appendVal := func(v moelua.Value) {
			if v.IsNil() {
				return
			}
			count++
			_ = out.SetInt(count, v)
		}

		whatF := st.String("f")
		whatFPin := st.Pin(whatF)
		defer whatFPin.Release()

		for level := int64(0); ; level++ {
			infoRes, err := st.Call(getInfo, co, moelua.Int(level), whatF)
			if err != nil || len(infoRes) == 0 || !infoRes[0].IsTable() {
				break
			}
			infoTbl := st.TableOf(infoRes[0])
			appendVal(infoTbl.GetString("func"))

			// Enumerate all positive local/temporary slots on this frame.
			for idx := int64(1); ; idx++ {
				locRes, err := st.Call(getLocal, co, moelua.Int(level), moelua.Int(idx))
				if err != nil || len(locRes) == 0 || locRes[0].IsNil() {
					break
				}
				if len(locRes) >= 2 {
					appendVal(locRes[1])
				}
			}
			// Enumerate all negative vararg slots on this frame.
			for idx := int64(-1); ; idx-- {
				locRes, err := st.Call(getLocal, co, moelua.Int(level), moelua.Int(idx))
				if err != nil || len(locRes) == 0 || locRes[0].IsNil() {
					break
				}
				if len(locRes) >= 2 {
					appendVal(locRes[1])
				}
			}
		}

		return []moelua.Value{moelua.Int(count)}, nil
	}))

	return []moelua.Value{tbl.Value()}, nil
}
