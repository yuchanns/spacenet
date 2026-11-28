package share

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/runtime"
)

const (
	metaSTMWriter = "spacenet_stm_writer"
	metaSTMReader = "spacenet_stm_reader"
)

type stmCopy struct {
	data []byte
}

type stmObject struct {
	mu   sync.RWMutex
	ref  atomic.Int32
	tok  uint64
	node *runtime.Node
	copy *stmCopy
}

func (o *stmObject) releaseWriter() {
	o.mu.Lock()
	o.copy = nil
	dead := o.ref.Add(-1) <= 0
	o.mu.Unlock()
	if dead && o.tok != 0 {
		o.node.Objs().Delete(runtime.KindSTMObject, o.tok)
	}
}

func (o *stmObject) releaseReader() {
	o.mu.RLock()
	dead := o.ref.Add(-1) <= 0
	o.mu.RUnlock()
	if dead && o.tok != 0 {
		o.node.Objs().Delete(runtime.KindSTMObject, o.tok)
	}
}

type stmWriterBox struct {
	obj *stmObject
}

type stmReaderBox struct {
	obj      *stmObject
	lastCopy *stmCopy
}

func extractSTMBytes(node *runtime.Node, s *moelua.State, args []moelua.Value, idx int) ([]byte, error) {
	if idx >= len(args) {
		return nil, errors.New("missing stm payload")
	}
	v := args[idx]
	if v.IsLightUD() {
		if idx+1 >= len(args) {
			return nil, errors.New("stm lightuserdata requires size")
		}
		sz := int(toInt(args[idx+1]))
		if sz < 0 {
			return nil, errors.New("negative stm size")
		}
		tok, _ := runtime.ExtractToken(v)
		if tok == 0 {
			return nil, nil
		}
		if b, ok := node.Msgs().Take(tok); ok {
			if sz < len(b) {
				return b[:sz], nil
			}
			return b, nil
		}
		if a, err := runtime.ActorFromState(s); err == nil {
			if b, _, ok := a.Borrow().Read(tok); ok {
				if sz < len(b) {
					b = b[:sz]
				}
				cp := make([]byte, len(b))
				copy(cp, b)
				return cp, nil
			}
		}
		return nil, nil
	}
	str, ok := s.ToString(v)
	if !ok {
		return nil, errors.New("expected string or lightuserdata")
	}
	return []byte(str), nil
}

func preloadSTM(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, err
	}
	node := actor.Node()

	writerMT, created := s.NewMetatable(metaSTMWriter)
	if created {
		_ = writerMT.SetString("__gc", s.NewFunc("stm.writer.__gc", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
			if len(args) == 0 {
				return nil, nil
			}
			if v, ok := st.CheckUserdata(args[0], metaSTMWriter); ok {
				box := v.(*stmWriterBox)
				if box.obj != nil {
					box.obj.releaseWriter()
					box.obj = nil
				}
			}
			return nil, nil
		}))
		_ = writerMT.SetString("__call", s.NewFunc("stm.writer.__call", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
			v, ok := st.CheckUserdata(args[0], metaSTMWriter)
			if !ok {
				return nil, errors.New("invalid stm writer")
			}
			box := v.(*stmWriterBox)
			if box.obj == nil {
				return nil, errors.New("stm writer closed")
			}
			data, err := extractSTMBytes(node, st, args, 1)
			if err != nil {
				return nil, err
			}
			nc := &stmCopy{data: data}
			box.obj.mu.Lock()
			box.obj.copy = nc
			box.obj.mu.Unlock()
			return nil, nil
		}))
	}

	readerMT, rCreated := s.NewMetatable(metaSTMReader)
	if rCreated {
		_ = readerMT.SetString("__gc", s.NewFunc("stm.reader.__gc", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
			if len(args) == 0 {
				return nil, nil
			}
			if v, ok := st.CheckUserdata(args[0], metaSTMReader); ok {
				box := v.(*stmReaderBox)
				if box.obj != nil {
					box.obj.releaseReader()
					box.obj = nil
					box.lastCopy = nil
				}
			}
			return nil, nil
		}))
		_ = readerMT.SetString("__call", s.NewFunc("stm.reader.__call", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
			v, ok := st.CheckUserdata(args[0], metaSTMReader)
			if !ok {
				return nil, errors.New("invalid stm reader")
			}
			if len(args) < 2 || !args[1].IsFunction() {
				return nil, errors.New("bad argument #1 to stm reader (function expected)")
			}
			box := v.(*stmReaderBox)
			if box.obj == nil {
				return []moelua.Value{moelua.Bool(false)}, nil
			}
			box.obj.mu.RLock()
			cur := box.obj.copy
			box.obj.mu.RUnlock()

			if cur == box.lastCopy {
				return []moelua.Value{moelua.Bool(false)}, nil
			}
			box.lastCopy = cur
			if cur == nil {
				return []moelua.Value{moelua.Bool(false)}, nil
			}
			tmpTok := node.Msgs().Alloc(cur.data)
			defer node.Msgs().Free(tmpTok)

			extra := moelua.Nil()
			if len(args) >= 3 {
				extra = args[2]
			}
			res, err := st.Call(args[1], runtime.LightUDToken(tmpTok), moelua.Int(int64(len(cur.data))), extra)
			if err != nil {
				return nil, err
			}
			out := make([]moelua.Value, 1+len(res))
			out[0] = moelua.Bool(true)
			copy(out[1:], res)
			return out, nil
		}))
	}

	tbl := s.NewTable(0, 3)
	_ = tbl.SetString("new", s.NewFunc("stm.new", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		data, err := extractSTMBytes(node, st, args, 0)
		if err != nil {
			return nil, err
		}
		obj := &stmObject{
			node: node,
			copy: &stmCopy{data: data},
		}
		obj.ref.Store(1)
		obj.tok = node.Objs().Put(runtime.KindSTMObject, obj)
		mt := st.GetMetatableByName(metaSTMWriter)
		ud, err := st.NewUserdataWithMeta(&stmWriterBox{obj: obj}, mt)
		if err != nil {
			obj.releaseWriter()
			return nil, err
		}
		return []moelua.Value{ud}, nil
	}))

	_ = tbl.SetString("copy", s.NewFunc("stm.copy", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		v, ok := st.CheckUserdata(args[0], metaSTMWriter)
		if !ok {
			return nil, errors.New("expected stm writer")
		}
		box := v.(*stmWriterBox)
		if box.obj == nil {
			return nil, errors.New("stm writer closed")
		}
		box.obj.mu.RLock()
		box.obj.ref.Add(1)
		tok := box.obj.tok
		box.obj.mu.RUnlock()
		return []moelua.Value{runtime.LightUDToken(tok)}, nil
	}))

	_ = tbl.SetString("newcopy", s.NewFunc("stm.newcopy", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("stm.newcopy requires stm pointer")
		}
		tok, ok := runtime.ExtractToken(args[0])
		if !ok || tok == 0 {
			return nil, errors.New("invalid stm pointer")
		}
		obj, ok := node.Objs().Get[*stmObject](runtime.KindSTMObject, tok)
		if !ok {
			return nil, errors.New("invalid stm pointer")
		}
		mt := st.GetMetatableByName(metaSTMReader)
		ud, err := st.NewUserdataWithMeta(&stmReaderBox{obj: obj}, mt)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{ud}, nil
	}))

	return []moelua.Value{tbl.Value()}, nil
}
