// Package sproto provides an implementation of cloudwu's sproto.core
// binary serialization protocol plus embedded sproto, sprotoparser, and sprotoloader.
package sproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Wire type constants matching lualib-src/sproto/sproto.h.
const (
	TypeInteger = 0
	TypeBoolean = 1
	TypeString  = 2
	TypeDouble  = 3
	TypeStruct  = 4
	TypeArray   = 0x80

	StringUTF8   = 0
	StringBinary = 1

	ProtoRequest  = 0
	ProtoResponse = 1
)

type Field struct {
	Tag   int
	Type  int // base type | TypeArray
	Name  string
	St    *Type
	Key   int // mainindex tag (>= 0 if indexed)
	Map   bool
	Extra int // decimal precision multiplier (10^n) for integer, or binary flag for string
}

type Type struct {
	Name   string
	Fields []Field
	Base   int // >= 0 when tags are contiguous starting at Base
	MaxN   int
}

type Protocol struct {
	Name    string
	Tag     int
	Confirm bool
	Req     *Type
	Resp    *Type
}

type Schema struct {
	Types     []Type
	Protocols []Protocol
}

func pow10(n int) int {
	r := 1
	for i := 0; i < n; i++ {
		r *= 10
	}
	return r
}

func structFieldCount(stream []byte) (int, error) {
	if len(stream) < 4 {
		return -1, errors.New("sproto: truncated struct")
	}
	fn := int(binary.LittleEndian.Uint16(stream[0:2]))
	header := 2 + 2*fn
	if len(stream) < header {
		return -1, errors.New("sproto: truncated header")
	}
	pos := header
	for i := 0; i < fn; i++ {
		v := int(binary.LittleEndian.Uint16(stream[2+i*2 : 4+i*2]))
		if v != 0 {
			continue
		}
		if pos+4 > len(stream) {
			return -1, errors.New("sproto: truncated field data length")
		}
		dsz := uint64(binary.LittleEndian.Uint32(stream[pos : pos+4]))
		if dsz > uint64(len(stream)-(pos+4)) {
			return -1, errors.New("sproto: truncated field data")
		}
		pos += 4 + int(dsz)
	}
	return fn, nil
}

func countArray(stream []byte) (int, error) {
	if len(stream) < 4 {
		return -1, errors.New("sproto: truncated array length")
	}
	length := uint64(binary.LittleEndian.Uint32(stream[0:4]))
	if length > uint64(len(stream)-4) {
		return -1, errors.New("sproto: truncated array body")
	}
	pos := 4
	end := 4 + int(length)
	n := 0
	for pos < end {
		if end-pos < 4 {
			return -1, errors.New("sproto: truncated array element")
		}
		elemLen := uint64(binary.LittleEndian.Uint32(stream[pos : pos+4]))
		if elemLen > uint64(end-pos-4) {
			return -1, errors.New("sproto: array element overflow")
		}
		n++
		pos += 4 + int(elemLen)
	}
	return n, nil
}

func readLenString(stream []byte) (string, int, error) {
	if len(stream) < 4 {
		return "", 0, errors.New("sproto: truncated string length")
	}
	sz := uint64(binary.LittleEndian.Uint32(stream[0:4]))
	if sz > uint64(len(stream)-4) {
		return "", 0, errors.New("sproto: truncated string")
	}
	return string(stream[4 : 4+int(sz)]), 4 + int(sz), nil
}

func importField(s *Schema, stream []byte) (Field, []byte, error) {
	f := Field{Tag: -1, Type: -1, Key: -1}
	if len(stream) < 4 {
		return f, nil, errors.New("sproto: truncated field")
	}
	sz := uint64(binary.LittleEndian.Uint32(stream[0:4]))
	if sz > uint64(len(stream)-4) {
		return f, nil, errors.New("sproto: field overflow")
	}
	body := stream[4 : 4+int(sz)]
	rest := stream[4+int(sz):]

	fn, err := structFieldCount(body)
	if err != nil {
		return f, nil, err
	}
	hdr := body[2 : 2+fn*2]
	data := body[2+fn*2:]
	array := 0
	tag := -1
	for i := 0; i < fn; i++ {
		tag++
		val := int(binary.LittleEndian.Uint16(hdr[i*2 : i*2+2]))
		if val&1 != 0 {
			tag += val / 2
			continue
		}
		if tag == 0 {
			if val != 0 {
				return f, nil, errors.New("sproto: field name must be data")
			}
			name, _, err := readLenString(data)
			if err != nil {
				return f, nil, err
			}
			f.Name = name
			continue
		}
		if val == 0 {
			return f, nil, errors.New("sproto: unexpected data field")
		}
		val = val/2 - 1
		switch tag {
		case 1:
			if val >= TypeStruct {
				return f, nil, errors.New("sproto: invalid builtin type")
			}
			f.Type = val
		case 2:
			if f.Type == TypeInteger {
				f.Extra = pow10(val)
			} else if f.Type == TypeString {
				f.Extra = val
			} else {
				if val < 0 || val >= len(s.Types) || f.Type >= 0 {
					return f, nil, errors.New("sproto: invalid struct type index")
				}
				f.Type = TypeStruct
				f.St = &s.Types[val]
			}
		case 3:
			f.Tag = val
		case 4:
			if val != 0 {
				array = TypeArray
			}
		case 5:
			f.Key = val
		case 6:
			if val != 0 {
				f.Map = true
			}
		default:
			return f, nil, errors.New("sproto: unknown field schema tag")
		}
	}
	if f.Tag < 0 || f.Type < 0 || f.Name == "" {
		return f, nil, errors.New("sproto: incomplete field schema")
	}
	f.Type |= array
	return f, rest, nil
}

func importType(s *Schema, t *Type, stream []byte) ([]byte, error) {
	if len(stream) < 4 {
		return nil, errors.New("sproto: truncated type")
	}
	sz := uint64(binary.LittleEndian.Uint32(stream[0:4]))
	if sz > uint64(len(stream)-4) {
		return nil, errors.New("sproto: type overflow")
	}
	body := stream[4 : 4+int(sz)]
	rest := stream[4+int(sz):]

	fn, err := structFieldCount(body)
	if err != nil || fn <= 0 || fn > 2 {
		return nil, errors.New("sproto: invalid type field count")
	}
	for i := 0; i < fn; i++ {
		if binary.LittleEndian.Uint16(body[2+i*2:4+i*2]) != 0 {
			return nil, errors.New("sproto: type name/fields must be data")
		}
	}
	pos := 2 + fn*2
	name, nread, err := readLenString(body[pos:])
	if err != nil {
		return nil, err
	}
	t.Name = name
	if fn == 1 {
		t.Base = -1
		return rest, nil
	}
	pos += nread
	n, err := countArray(body[pos:])
	if err != nil {
		return nil, err
	}
	fStream := body[pos+4:]
	t.Fields = make([]Field, n)
	maxn := n
	last := -1
	for i := 0; i < n; i++ {
		f, next, err := importField(s, fStream)
		if err != nil {
			return nil, err
		}
		if f.Tag <= last {
			return nil, errors.New("sproto: field tags must be ascending")
		}
		if f.Tag > last+1 {
			maxn++
		}
		last = f.Tag
		t.Fields[i] = f
		fStream = next
	}
	t.MaxN = maxn
	if n > 0 {
		t.Base = t.Fields[0].Tag
		if t.Fields[n-1].Tag-t.Base+1 != n {
			t.Base = -1
		}
	} else {
		t.Base = -1
	}
	return rest, nil
}

func importProtocol(s *Schema, p *Protocol, stream []byte) ([]byte, error) {
	if len(stream) < 4 {
		return nil, errors.New("sproto: truncated protocol")
	}
	sz := int(binary.LittleEndian.Uint32(stream[0:4]))
	if 4+sz > len(stream) {
		return nil, errors.New("sproto: protocol overflow")
	}
	body := stream[4 : 4+sz]
	rest := stream[4+sz:]

	fn, err := structFieldCount(body)
	if err != nil {
		return nil, err
	}
	hdr := body[2 : 2+fn*2]
	data := body[2+fn*2:]
	p.Tag = -1
	tag := 0
	for i := 0; i < fn; i, tag = i+1, tag+1 {
		val := int(binary.LittleEndian.Uint16(hdr[i*2 : i*2+2]))
		if val&1 != 0 {
			tag += (val - 1) / 2
			continue
		}
		val = val/2 - 1
		switch tag {
		case 0:
			if val != -1 {
				return nil, errors.New("sproto: protocol name must be data")
			}
			name, _, err := readLenString(data)
			if err != nil {
				return nil, err
			}
			p.Name = name
		case 1:
			if val < 0 {
				return nil, errors.New("sproto: negative protocol tag")
			}
			p.Tag = val
		case 2:
			if val < 0 || val >= len(s.Types) {
				return nil, errors.New("sproto: invalid protocol request type")
			}
			p.Req = &s.Types[val]
		case 3:
			if val < 0 || val >= len(s.Types) {
				return nil, errors.New("sproto: invalid protocol response type")
			}
			p.Resp = &s.Types[val]
		case 4:
			p.Confirm = val != 0
		default:
			return nil, errors.New("sproto: unknown protocol schema tag")
		}
	}
	if p.Name == "" || p.Tag < 0 {
		return nil, errors.New("sproto: incomplete protocol schema")
	}
	return rest, nil
}

// ParseSchema parses a binary sproto schema produced by sprotoparser.parse.
func ParseSchema(bundle []byte) (*Schema, error) {
	fn, err := structFieldCount(bundle)
	if err != nil || fn < 0 || fn > 2 {
		return nil, errors.New("sproto: invalid bundle")
	}
	s := &Schema{}
	pos := 2 + fn*2
	var typeData, protoData []byte
	for i := 0; i < fn; i++ {
		if binary.LittleEndian.Uint16(bundle[2+i*2:4+i*2]) != 0 {
			return nil, errors.New("sproto: bundle field must be data")
		}
		n, err := countArray(bundle[pos:])
		if err != nil {
			return nil, err
		}
		chunkLen := int(binary.LittleEndian.Uint32(bundle[pos : pos+4]))
		if i == 0 {
			typeData = bundle[pos+4 : pos+4+chunkLen]
			s.Types = make([]Type, n)
		} else {
			protoData = bundle[pos+4 : pos+4+chunkLen]
			s.Protocols = make([]Protocol, n)
		}
		pos += 4 + chunkLen
	}
	for i := range s.Types {
		typeData, err = importType(s, &s.Types[i], typeData)
		if err != nil {
			return nil, err
		}
	}
	for i := range s.Protocols {
		protoData, err = importProtocol(s, &s.Protocols[i], protoData)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Schema) QueryType(name string) *Type {
	for i := range s.Types {
		if s.Types[i].Name == name {
			return &s.Types[i]
		}
	}
	return nil
}

func (s *Schema) QueryProtocolByTag(tag int) *Protocol {
	lo, hi := 0, len(s.Protocols)
	for lo < hi {
		mid := (lo + hi) / 2
		t := s.Protocols[mid].Tag
		if t == tag {
			return &s.Protocols[mid]
		}
		if tag > t {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return nil
}

func (s *Schema) QueryProtocolByName(name string) *Protocol {
	for i := range s.Protocols {
		if s.Protocols[i].Name == name {
			return &s.Protocols[i]
		}
	}
	return nil
}

func (t *Type) findTag(tag int) *Field {
	if t.Base >= 0 {
		idx := tag - t.Base
		if idx < 0 || idx >= len(t.Fields) {
			return nil
		}
		return &t.Fields[idx]
	}
	lo, hi := 0, len(t.Fields)
	for lo < hi {
		mid := (lo + hi) / 2
		ft := t.Fields[mid].Tag
		if ft == tag {
			return &t.Fields[mid]
		}
		if tag > ft {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return nil
}

// Pack implements sproto's zero-compression wire packing (sproto_pack).
func Pack(src []byte) []byte {
	srcLen := len(src)
	out := make([]byte, 0, srcLen+(srcLen+2047)/2048*2+2)
	ffStart := -1
	ffOutPos := -1
	ffCount := 0

	flushFF := func() {
		if ffCount <= 0 {
			return
		}
		out[ffOutPos] = 0xff
		out[ffOutPos+1] = byte(ffCount - 1)
		nbytes := ffCount * 8
		avail := srcLen - ffStart
		if avail >= nbytes {
			copy(out[ffOutPos+2:ffOutPos+2+nbytes], src[ffStart:ffStart+nbytes])
		} else {
			copy(out[ffOutPos+2:ffOutPos+2+avail], src[ffStart:srcLen])
			clear(out[ffOutPos+2+avail : ffOutPos+2+nbytes])
		}
		ffCount = 0
	}

	var tmp [8]byte
	for i := 0; i < srcLen; i += 8 {
		var seg []byte
		if i+8 <= srcLen {
			seg = src[i : i+8]
		} else {
			clear(tmp[:])
			copy(tmp[:], src[i:srcLen])
			seg = tmp[:]
		}
		var header byte
		nonZero := 0
		var nzBytes [8]byte
		for j := 0; j < 8; j++ {
			if seg[j] != 0 {
				header |= 1 << uint(j)
				nzBytes[nonZero] = seg[j]
				nonZero++
			}
		}
		if (nonZero == 7 || nonZero == 6) && ffCount > 0 {
			nonZero = 8
		}
		if nonZero == 8 {
			if ffCount == 0 {
				ffStart = i
				ffOutPos = len(out)
				ffCount = 1
				out = append(out, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
			} else {
				ffCount++
				out = append(out, 0, 0, 0, 0, 0, 0, 0, 0)
				if ffCount == 256 {
					flushFF()
				}
			}
		} else {
			if ffCount > 0 {
				flushFF()
			}
			out = append(out, header)
			out = append(out, nzBytes[:nonZero]...)
		}
	}
	flushFF()
	return out
}

// Unpack implements sproto's zero-decompression wire unpacking (sproto_unpack).
func Unpack(src []byte) ([]byte, error) {
	out := make([]byte, 0, len(src)*2)
	pos := 0
	for pos < len(src) {
		header := src[pos]
		pos++
		if header == 0xff {
			if pos >= len(src) {
				return nil, errors.New("Invalid unpack stream")
			}
			n := (int(src[pos]) + 1) * 8
			pos++
			if pos+n > len(src) {
				return nil, errors.New("Invalid unpack stream")
			}
			out = append(out, src[pos:pos+n]...)
			pos += n
		} else {
			for i := 0; i < 8; i++ {
				if (header>>uint(i))&1 != 0 {
					if pos >= len(src) {
						return nil, errors.New("Invalid unpack stream")
					}
					out = append(out, src[pos])
					pos++
				} else {
					out = append(out, 0)
				}
			}
		}
	}
	return out, nil
}

func appendLenBytes(dst, payload []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(payload)))
	return append(dst, payload...)
}

func expandSign64(v uint32) int64 {
	return int64(int32(v))
}

// unused import guards
var (
	_ = fmt.Sprintf
	_ = math.Float64bits
)
