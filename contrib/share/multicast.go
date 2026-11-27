package share

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/runtime"
)

type mcPackage struct {
	ref     atomic.Int32
	dataTok uint64
	size    uint32
	pkgTok  uint64
}

func releaseMulticastPkg(node *runtime.Node, pkgTok uint64) error {
	v, ok := node.Objs().Get(runtime.KindMulticastPkg, pkgTok)
	if !ok {
		return nil
	}
	pkg := v.(*mcPackage)
	rem := pkg.ref.Add(-1)
	if rem == 0 {
		node.Objs().Delete(runtime.KindMulticastPkg, pkgTok)
		node.Msgs().Free(pkg.dataTok)
	} else if rem < 0 {
		node.Objs().Delete(runtime.KindMulticastPkg, pkgTok)
		node.Msgs().Free(pkg.dataTok)
		return fmt.Errorf("Invalid multicast package reference %d", rem)
	}
	return nil
}

func initMulticastNode(node *runtime.Node) (io.Closer, error) {
	node.RegisterDropHandler(runtime.PTypeMulticast, func(msg *runtime.Message) {
		if len(msg.Data) != 8 {
			return
		}
		pkgTok := binary.LittleEndian.Uint64(msg.Data)
		kind, _, _ := runtime.SplitToken(pkgTok)
		if kind != runtime.KindMulticastPkg {
			return
		}
		_ = releaseMulticastPkg(node, pkgTok)
	})
	return nil, nil
}

func preloadMulticastCore(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, err
	}
	node := actor.Node()

	makeBox := func(dataTok uint64, sz uint32) ([]moelua.Value, error) {
		pkg := &mcPackage{
			dataTok: dataTok,
			size:    sz,
		}
		pkgTok := node.Objs().Put(runtime.KindMulticastPkg, pkg)
		pkg.pkgTok = pkgTok

		// 8-byte box storing LittleEndian(pkgTok) so multicastd.lua can call
		// skynet.tostring(pack, 8) and redirect the 8-byte string verbatim!
		box := make([]byte, 8)
		binary.LittleEndian.PutUint64(box, pkgTok)
		boxTok := node.Msgs().Alloc(box)
		return []moelua.Value{runtime.LightUDToken(boxTok), moelua.Int(8)}, nil
	}

	readBoxBytes := func(st *moelua.State, v moelua.Value, take bool) ([]byte, error) {
		tok, ok := runtime.ExtractToken(v)
		if !ok || tok == 0 {
			return nil, errors.New("invalid multicast package handle")
		}
		kind, _, _ := runtime.SplitToken(tok)
		switch kind {
		case runtime.KindMsgBuf:
			if take {
				b, ok := node.Msgs().Take(tok)
				if !ok {
					return nil, errors.New("multicast package already freed")
				}
				return b, nil
			}
			b, ok := node.Msgs().Borrow(tok)
			if !ok {
				return nil, errors.New("multicast package already freed")
			}
			return b, nil
		case runtime.KindDispatchSlot:
			if a, err := runtime.ActorFromState(st); err == nil {
				if b, _, ok := a.Borrow().Read(tok); ok {
					return b, nil
				}
			}
		}
		return nil, errors.New("invalid multicast package handle")
	}

	tbl := s.NewTable(0, 7)

	_ = tbl.SetString("pack", s.NewFunc("mc.pack", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 2 {
			return nil, errors.New("mc.pack requires (data, size)")
		}
		sz := toInt(args[1])
		if sz < 0 || uint64(sz) != uint64(uint32(sz)) {
			return nil, errors.New("Size should be 32bit integer")
		}
		tok, ok := runtime.ExtractToken(args[0])
		if !ok || tok == 0 {
			return nil, errors.New("mc.pack requires lightuserdata")
		}
		// Keep dataTok alive inside mcPackage until all subscribers call mc.close.
		return makeBox(tok, uint32(sz))
	}))

	_ = tbl.SetString("packremote", s.NewFunc("mc.packremote", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 2 {
			return nil, errors.New("mc.packremote requires (data, size)")
		}
		sz := toInt(args[1])
		if sz < 0 || uint64(sz) != uint64(uint32(sz)) {
			return nil, errors.New("Size should be 32bit integer")
		}
		raw, err := readBoxBytes(st, args[0], false)
		if err != nil {
			return nil, err
		}
		if int(sz) < len(raw) {
			raw = raw[:sz]
		}
		dataTok := node.Msgs().AllocCopy(raw)
		return makeBox(dataTok, uint32(sz))
	}))

	_ = tbl.SetString("unpack", s.NewFunc("mc.unpack", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 2 {
			return nil, errors.New("mc.unpack requires (pack, size)")
		}
		sz := int(toInt(args[1]))
		if sz != 8 {
			return nil, fmt.Errorf("Invalid multicast package size %d", sz)
		}
		box, err := readBoxBytes(st, args[0], false)
		if err != nil || len(box) < 8 {
			return nil, errors.New("Invalid multicast package")
		}
		pkgTok := binary.LittleEndian.Uint64(box[:8])
		v, ok := node.Objs().Get(runtime.KindMulticastPkg, pkgTok)
		if !ok {
			return nil, errors.New("multicast package not found")
		}
		pkg := v.(*mcPackage)
		return []moelua.Value{
			runtime.LightUDToken(pkgTok),
			runtime.LightUDToken(pkg.dataTok),
			moelua.Int(int64(pkg.size)),
		}, nil
	}))

	_ = tbl.SetString("bind", s.NewFunc("mc.bind", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 2 {
			return nil, errors.New("mc.bind requires (pack, ref)")
		}
		ref := int32(toInt(args[1]))
		box, err := readBoxBytes(st, args[0], true) // frees the 8-byte box!
		if err != nil || len(box) < 8 {
			return nil, errors.New("Invalid multicast package")
		}
		pkgTok := binary.LittleEndian.Uint64(box[:8])
		v, ok := node.Objs().Get(runtime.KindMulticastPkg, pkgTok)
		if !ok {
			return nil, errors.New("multicast package not found")
		}
		pkg := v.(*mcPackage)
		if ref < 0 {
			return nil, fmt.Errorf("Invalid multicast package reference %d", ref)
		}
		if ref == 0 {
			node.Objs().Delete(runtime.KindMulticastPkg, pkgTok)
			node.Msgs().Free(pkg.dataTok)
			return nil, nil
		}
		if !pkg.ref.CompareAndSwap(0, ref) {
			return nil, errors.New("Can't bind a multicast package more than once")
		}
		return []moelua.Value{runtime.LightUDToken(pkgTok)}, nil
	}))

	_ = tbl.SetString("close", s.NewFunc("mc.close", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, nil
		}
		pkgTok, ok := runtime.ExtractToken(args[0])
		if !ok || pkgTok == 0 {
			return nil, nil
		}
		return nil, releaseMulticastPkg(node, pkgTok)
	}))

	_ = tbl.SetString("remote", s.NewFunc("mc.remote", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("mc.remote requires pack")
		}
		box, err := readBoxBytes(st, args[0], true)
		if err != nil || len(box) < 8 {
			return nil, errors.New("Invalid multicast package")
		}
		pkgTok := binary.LittleEndian.Uint64(box[:8])
		v, ok := node.Objs().Delete(runtime.KindMulticastPkg, pkgTok)
		if !ok {
			return nil, errors.New("multicast package not found")
		}
		pkg := v.(*mcPackage)
		return []moelua.Value{runtime.LightUDToken(pkg.dataTok), moelua.Int(int64(pkg.size))}, nil
	}))

	_ = tbl.SetString("nextid", s.NewFunc("mc.nextid", func(_ *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, errors.New("mc.nextid requires channel id")
		}
		id := uint32(toInt(args[0]))
		id = (id + 256) & 0x7fffffff
		return []moelua.Value{moelua.Int(int64(id))}, nil
	}))

	return []moelua.Value{tbl.Value()}, nil
}
