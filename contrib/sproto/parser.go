package sproto

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/yuchanns/moelua"
)

// Parse compiles sproto IDL text into the binary sproto schema format produced
// by cloudwu's sprotoparser.parse.
func Parse(text, filename string) ([]byte, error) {
	if filename == "" {
		filename = "=text"
	}
	p := &idlParser{
		src:  text,
		file: filename,
		line: 1,
	}
	ast, err := p.parseAll()
	if err != nil {
		return nil, err
	}
	if err := ast.resolveAndCheck(); err != nil {
		return nil, err
	}
	return ast.encodeBinary()
}

var builtinTypeID = map[string]int{
	"integer": TypeInteger,
	"boolean": TypeBoolean,
	"string":  TypeString,
	"binary":  TypeString,
	"double":  TypeDouble,
}

type astField struct {
	name       string
	tag        int
	array      bool
	typename   string
	hasDecimal bool
	decimal    int
	hasKey     bool
	key        string
}

type astProto struct {
	name     string
	tag      int
	request  string
	response string
	confirm  bool
}

type astSchema struct {
	types     map[string][]astField
	protocols map[string]*astProto
}

type idlParser struct {
	src  string
	file string
	pos  int
	line int
}

func (p *idlParser) syntaxErr(detail string) error {
	if detail != "" {
		return fmt.Errorf("syntax error at [%s] line (%d): %s", p.file, p.line, detail)
	}
	return fmt.Errorf("syntax error at [%s] line (%d)", p.file, p.line)
}

func (p *idlParser) eof() bool {
	return p.pos >= len(p.src)
}

func (p *idlParser) peek() byte {
	if p.pos >= len(p.src) {
		return 0
	}
	return p.src[p.pos]
}

func (p *idlParser) skipBlanks() bool {
	start := p.pos
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		switch c {
		case ' ', '\t':
			p.pos++
		case '\r':
			if p.pos+1 < len(p.src) && p.src[p.pos+1] == '\n' {
				p.pos += 2
				p.line++
			} else {
				p.pos++
			}
		case '\n':
			p.pos++
			p.line++
		case '#':
			for p.pos < len(p.src) && p.src[p.pos] != '\n' && p.src[p.pos] != '\r' {
				p.pos++
			}
		default:
			return p.pos > start
		}
	}
	return p.pos > start
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isAlnum(c byte) bool {
	return isAlpha(c) || (c >= '0' && c <= '9')
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func (p *idlParser) parseWord() (string, error) {
	if p.eof() || !isAlpha(p.peek()) {
		return "", p.syntaxErr("expected identifier")
	}
	start := p.pos
	p.pos++
	for !p.eof() && isAlnum(p.peek()) {
		p.pos++
	}
	return p.src[start:p.pos], nil
}

func (p *idlParser) parseTypeName() (string, error) {
	w, err := p.parseWord()
	if err != nil {
		return "", err
	}
	var sb strings.Builder
	sb.WriteString(w)
	for !p.eof() && p.peek() == '.' {
		p.pos++
		next, err := p.parseWord()
		if err != nil {
			return "", err
		}
		sb.WriteByte('.')
		sb.WriteString(next)
	}
	return sb.String(), nil
}

func (p *idlParser) parseTag() (int, error) {
	if p.eof() || !isDigit(p.peek()) {
		return 0, p.syntaxErr("expected tag number")
	}
	start := p.pos
	for !p.eof() && isDigit(p.peek()) {
		p.pos++
	}
	v, err := strconv.Atoi(p.src[start:p.pos])
	if err != nil {
		return 0, p.syntaxErr("invalid tag number")
	}
	return v, nil
}

func (p *idlParser) parseAll() (*astSchema, error) {
	s := &astSchema{
		types:     make(map[string][]astField),
		protocols: make(map[string]*astProto),
	}
	for {
		p.skipBlanks()
		if p.eof() {
			break
		}
		if p.peek() == '.' {
			p.pos++
			name, err := p.parseWord()
			if err != nil {
				return nil, err
			}
			if _, exists := s.types[name]; exists {
				return nil, fmt.Errorf("redefined %s", name)
			}
			p.skipBlanks()
			fields, err := p.parseStruct(s, name)
			if err != nil {
				return nil, err
			}
			s.types[name] = fields
		} else {
			name, err := p.parseWord()
			if err != nil {
				return nil, err
			}
			if _, exists := s.protocols[name]; exists {
				return nil, fmt.Errorf("redefined %s", name)
			}
			if !p.skipBlanks() {
				return nil, p.syntaxErr("expected whitespace after protocol name")
			}
			tag, err := p.parseTag()
			if err != nil {
				return nil, err
			}
			p.skipBlanks()
			proto, err := p.parseProtocolBody(s, name, tag)
			if err != nil {
				return nil, err
			}
			s.protocols[name] = proto
		}
	}
	return s, nil
}

func (p *idlParser) parseStruct(s *astSchema, typeName string) ([]astField, error) {
	if p.eof() || p.peek() != '{' {
		return nil, p.syntaxErr("expected '{'")
	}
	p.pos++
	var fields []astField
	names := make(map[string]bool)
	tags := make(map[int]bool)

	for {
		p.skipBlanks()
		if p.eof() {
			return nil, p.syntaxErr("unterminated struct '{'")
		}
		if p.peek() == '}' {
			p.pos++
			break
		}
		if p.peek() == '.' {
			p.pos++
			subName, err := p.parseWord()
			if err != nil {
				return nil, err
			}
			fullName := typeName + "." + subName
			if _, exists := s.types[fullName]; exists {
				return nil, fmt.Errorf("redefined %s", fullName)
			}
			p.skipBlanks()
			subFields, err := p.parseStruct(s, fullName)
			if err != nil {
				return nil, err
			}
			s.types[fullName] = subFields
			continue
		}
		fname, err := p.parseWord()
		if err != nil {
			return nil, err
		}
		if names[fname] {
			return nil, fmt.Errorf("redefine %s in type %s", fname, typeName)
		}
		names[fname] = true
		if !p.skipBlanks() {
			return nil, p.syntaxErr("expected whitespace after field name")
		}
		ftag, err := p.parseTag()
		if err != nil {
			return nil, err
		}
		if tags[ftag] {
			return nil, fmt.Errorf("redefine tag %d in type %s", ftag, typeName)
		}
		tags[ftag] = true
		p.skipBlanks()
		if p.eof() || p.peek() != ':' {
			return nil, p.syntaxErr("expected ':'")
		}
		p.pos++
		p.skipBlanks()

		f := astField{name: fname, tag: ftag}
		if !p.eof() && p.peek() == '*' {
			f.array = true
			p.pos++
		}
		ftyp, err := p.parseTypeName()
		if err != nil {
			return nil, err
		}
		f.typename = ftyp

		// Optional (mainkey) / () / (decimal)
		for !p.eof() && p.peek() == '(' {
			p.pos++
			p.skipBlanks()
			if p.eof() {
				return nil, p.syntaxErr("unterminated '('")
			}
			if p.peek() == ')' {
				p.pos++
				if !f.array {
					return nil, p.syntaxErr("empty map key '()' requires array field")
				}
				f.hasKey = true
				f.key = ""
			} else if isDigit(p.peek()) {
				dec, err := p.parseTag()
				if err != nil {
					return nil, err
				}
				p.skipBlanks()
				if p.eof() || p.peek() != ')' {
					return nil, p.syntaxErr("expected ')'")
				}
				p.pos++
				if f.typename == "integer" {
					f.hasDecimal = true
					f.decimal = dec
				} else {
					if !f.array {
						return nil, p.syntaxErr("mainkey requires array field")
					}
					f.hasKey = true
					f.key = strconv.Itoa(dec)
				}
			} else {
				mk, err := p.parseWord()
				if err != nil {
					return nil, err
				}
				p.skipBlanks()
				if p.eof() || p.peek() != ')' {
					return nil, p.syntaxErr("expected ')'")
				}
				p.pos++
				if !f.array {
					return nil, p.syntaxErr("mainkey requires array field")
				}
				f.hasKey = true
				f.key = mk
			}
		}

		fields = append(fields, f)
	}
	sort.Slice(fields, func(i, j int) bool { return fields[i].tag < fields[j].tag })
	return fields, nil
}

func (p *idlParser) parseProtocolBody(s *astSchema, protoName string, tag int) (*astProto, error) {
	if p.eof() || p.peek() != '{' {
		return nil, p.syntaxErr("expected '{'")
	}
	p.pos++
	res := &astProto{name: protoName, tag: tag}
	seenReq := false
	seenResp := false

	for {
		p.skipBlanks()
		if p.eof() {
			return nil, p.syntaxErr("unterminated protocol '{'")
		}
		if p.peek() == '}' {
			p.pos++
			break
		}
		kw, err := p.parseWord()
		if err != nil {
			return nil, err
		}
		if kw != "request" && kw != "response" {
			return nil, p.syntaxErr("expected 'request' or 'response'")
		}
		if (kw == "request" && seenReq) || (kw == "response" && seenResp) {
			return nil, fmt.Errorf("redefine %s in protocol %s", kw, protoName)
		}
		if !p.skipBlanks() {
			return nil, p.syntaxErr("expected whitespace after " + kw)
		}

		var typeRef string
		if !p.eof() && p.peek() == '{' {
			typeRef = protoName + "." + kw
			if _, exists := s.types[typeRef]; exists {
				return nil, fmt.Errorf("redefined %s", typeRef)
			}
			fields, err := p.parseStruct(s, typeRef)
			if err != nil {
				return nil, err
			}
			s.types[typeRef] = fields
		} else {
			tn, err := p.parseTypeName()
			if err != nil {
				return nil, err
			}
			typeRef = tn
		}

		if kw == "request" {
			seenReq = true
			if typeRef != "nil" {
				res.request = typeRef
			}
		} else {
			seenResp = true
			if typeRef == "nil" {
				res.confirm = true
			} else {
				res.response = typeRef
			}
		}
	}
	return res, nil
}

func resolveTypeName(types map[string][]astField, parent, name string) (string, bool) {
	if _, ok := builtinTypeID[name]; ok {
		return name, true
	}
	cur := parent
	for {
		full := cur + "." + name
		if _, ok := types[full]; ok {
			return full, true
		}
		dot := strings.LastIndexByte(cur, '.')
		if dot < 0 {
			break
		}
		cur = cur[:dot]
	}
	if _, ok := types[name]; ok {
		return name, true
	}
	return "", false
}

const maxInlineSchemaVal = 0x7ffe // largest v such that (v+1)*2 fits in uint16

func (s *astSchema) resolveAndCheck() error {
	if len(s.types) > maxInlineSchemaVal+1 {
		return fmt.Errorf("too many sproto types (%d > %d)", len(s.types), maxInlineSchemaVal+1)
	}
	protoTags := make(map[int]string, len(s.protocols))
	for name, pr := range s.protocols {
		if pr.tag < 0 || pr.tag > maxInlineSchemaVal {
			return fmt.Errorf("protocol %s tag %d out of range [0, %d]", name, pr.tag, maxInlineSchemaVal)
		}
		if _, dup := protoTags[pr.tag]; dup {
			return fmt.Errorf("redefined protocol tag %d at %s", pr.tag, name)
		}
		protoTags[pr.tag] = name
		if pr.request != "" {
			if _, ok := s.types[pr.request]; !ok {
				return fmt.Errorf("Undefined request type %s in protocol %s", pr.request, name)
			}
		}
		if pr.response != "" {
			if _, ok := s.types[pr.response]; !ok {
				return fmt.Errorf("Undefined response type %s in protocol %s", pr.response, name)
			}
		}
	}
	for tname, fields := range s.types {
		for i := range fields {
			if fields[i].tag < 0 || fields[i].tag > maxInlineSchemaVal {
				return fmt.Errorf("field %s.%s tag %d out of range [0, %d]", tname, fields[i].name, fields[i].tag, maxInlineSchemaVal)
			}
			if fields[i].hasDecimal && (fields[i].decimal < 0 || fields[i].decimal > 18) {
				return fmt.Errorf("field %s.%s decimal precision %d out of range [0, 18]", tname, fields[i].name, fields[i].decimal)
			}
			full, ok := resolveTypeName(s.types, tname, fields[i].typename)
			if !ok {
				return fmt.Errorf("Undefined type %s in type %s", fields[i].typename, tname)
			}
			fields[i].typename = full
		}
		s.types[tname] = fields
	}
	return nil
}

func packU16Val(dst []byte, v int) []byte {
	return binary.LittleEndian.AppendUint16(dst, uint16((v+1)*2))
}

func packSkip(dst []byte) []byte {
	return append(dst, 1, 0)
}

func packRef(dst []byte) []byte {
	return append(dst, 0, 0)
}

func packBinBytes(dst, data []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(data)))
	return append(dst, data...)
}

type indexedType struct {
	id     int
	fields map[string]struct {
		tag       int
		isBuiltin bool
	}
}

func (s *astSchema) encodeBinary() ([]byte, error) {
	if len(s.types) == 0 && len(s.protocols) == 0 {
		return []byte{0, 0}, nil
	}
	typeNames := make([]string, 0, len(s.types))
	for name := range s.types {
		typeNames = append(typeNames, name)
	}
	sort.Strings(typeNames)

	allTypes := make(map[string]indexedType, len(typeNames))
	for idx, name := range typeNames {
		fmap := make(map[string]struct {
			tag       int
			isBuiltin bool
		}, len(s.types[name]))
		for _, f := range s.types[name] {
			_, isB := builtinTypeID[f.typename]
			fmap[f.name] = struct {
				tag       int
				isBuiltin bool
			}{tag: f.tag, isBuiltin: isB}
		}
		allTypes[name] = indexedType{id: idx, fields: fmap}
	}

	var ttBuf []byte
	for _, name := range typeNames {
		encoded, err := encodeTypeDef(name, s.types[name], allTypes)
		if err != nil {
			return nil, err
		}
		ttBuf = append(ttBuf, encoded...)
	}

	var tpBuf []byte
	if len(s.protocols) > 0 {
		protos := make([]*astProto, 0, len(s.protocols))
		for _, pr := range s.protocols {
			protos = append(protos, pr)
		}
		sort.Slice(protos, func(i, j int) bool { return protos[i].tag < protos[j].tag })
		for _, pr := range protos {
			encoded, err := encodeProtoDef(pr, allTypes)
			if err != nil {
				return nil, err
			}
			tpBuf = append(tpBuf, encoded...)
		}
	}

	var out []byte
	if len(s.protocols) == 0 {
		out = append(out, 1, 0)
		out = packRef(out)
		out = packBinBytes(out, ttBuf)
	} else {
		out = append(out, 2, 0)
		out = packRef(out)
		out = packRef(out)
		out = packBinBytes(out, ttBuf)
		out = packBinBytes(out, tpBuf)
	}
	return out, nil
}

func encodeTypeDef(name string, fields []astField, allTypes map[string]indexedType) ([]byte, error) {
	var fieldsBuf []byte
	for _, f := range fields {
		bID, isBuiltin := builtinTypeID[f.typename]
		hasExtra := f.hasDecimal
		extra := f.decimal
		if f.typename == "binary" {
			hasExtra = true
			extra = 1
		}
		var subType indexedType
		if !isBuiltin {
			st, ok := allTypes[f.typename]
			if !ok {
				return nil, fmt.Errorf("Undefined type %s", f.typename)
			}
			subType = st
		}

		isMap := false
		keyTag := -1
		if f.hasKey {
			if !f.array || isBuiltin {
				return nil, fmt.Errorf("Invalid map index :%s", f.key)
			}
			keyName := f.key
			if keyName == "" {
				isMap = true
				if len(subType.fields) != 2 {
					return nil, fmt.Errorf("Invalid map definition: %s, must only have two fields", f.name)
				}
				minTag := math.MaxInt
				for fn, fi := range subType.fields {
					if fi.tag < minTag {
						minTag = fi.tag
						keyName = fn
					}
				}
			}
			fi, ok := subType.fields[keyName]
			if !ok || !fi.isBuiltin {
				return nil, fmt.Errorf("Invalid map index :%s", keyName)
			}
			keyTag = fi.tag
		}

		var fb []byte
		switch {
		case !f.array:
			fb = append(fb, 4, 0)
		case !f.hasKey:
			fb = append(fb, 5, 0)
		case !isMap:
			fb = append(fb, 6, 0)
		default:
			fb = append(fb, 7, 0)
		}
		fb = packRef(fb) // tag 0: name
		if isBuiltin {
			fb = packU16Val(fb, bID) // tag 1: buildin
			if hasExtra {
				fb = packU16Val(fb, extra) // tag 2: extra
			} else {
				fb = packSkip(fb)
			}
			fb = packU16Val(fb, f.tag) // tag 3: tag
		} else {
			fb = packSkip(fb)               // tag 1: skip
			fb = packU16Val(fb, subType.id) // tag 2: type
			fb = packU16Val(fb, f.tag)      // tag 3: tag
		}
		if f.array {
			fb = packU16Val(fb, 1) // tag 4: array
			if f.hasKey {
				fb = packU16Val(fb, keyTag) // tag 5: key
				if isMap {
					fb = packU16Val(fb, 1) // tag 6: map
				}
			}
		}
		fb = packBinBytes(fb, []byte(f.name))
		fieldsBuf = packBinBytes(fieldsBuf, fb)
	}

	var tb []byte
	if len(fields) == 0 {
		tb = append(tb, 1, 0)
		tb = packRef(tb)
		tb = packBinBytes(tb, []byte(name))
	} else {
		tb = append(tb, 2, 0)
		tb = packRef(tb)
		tb = packRef(tb)
		tb = packBinBytes(tb, []byte(name))
		tb = packBinBytes(tb, fieldsBuf)
	}
	return packBinBytes(nil, tb), nil
}

func encodeProtoDef(pr *astProto, allTypes map[string]indexedType) ([]byte, error) {
	var pb []byte
	switch {
	case pr.request == "" && pr.response == "" && !pr.confirm:
		pb = append(pb, 2, 0)
		pb = packRef(pb)
		pb = packU16Val(pb, pr.tag)
	case pr.response != "":
		pb = append(pb, 4, 0)
		pb = packRef(pb)
		pb = packU16Val(pb, pr.tag)
		if pr.request != "" {
			rt, ok := allTypes[pr.request]
			if !ok {
				return nil, fmt.Errorf("Protocol %s request type %s not found", pr.name, pr.request)
			}
			pb = packU16Val(pb, rt.id)
		} else {
			pb = packSkip(pb)
		}
		resp, ok := allTypes[pr.response]
		if !ok {
			return nil, fmt.Errorf("Protocol %s response type %s not found", pr.name, pr.response)
		}
		pb = packU16Val(pb, resp.id)
	case pr.confirm:
		pb = append(pb, 5, 0)
		pb = packRef(pb)
		pb = packU16Val(pb, pr.tag)
		if pr.request != "" {
			rt, ok := allTypes[pr.request]
			if !ok {
				return nil, fmt.Errorf("Protocol %s request type %s not found", pr.name, pr.request)
			}
			pb = packU16Val(pb, rt.id)
		} else {
			pb = packSkip(pb)
		}
		pb = packSkip(pb)
		pb = packU16Val(pb, 1)
	default:
		pb = append(pb, 3, 0)
		pb = packRef(pb)
		pb = packU16Val(pb, pr.tag)
		rt, ok := allTypes[pr.request]
		if !ok {
			return nil, fmt.Errorf("Protocol %s request type %s not found", pr.name, pr.request)
		}
		pb = packU16Val(pb, rt.id)
	}
	pb = packBinBytes(pb, []byte(pr.name))
	return packBinBytes(nil, pb), nil
}

func preloadSprotoParser(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	tbl := s.NewTable(0, 2)
	_ = tbl.SetString("parse", s.NewFunc("sprotoparser.parse", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, fmt.Errorf("bad argument #1 to 'parse' (string expected)")
		}
		text, ok := st.ToString(args[0])
		if !ok {
			return nil, fmt.Errorf("bad argument #1 to 'parse' (string expected)")
		}
		name := "=text"
		if len(args) >= 2 && !args[1].IsNil() {
			if n, ok := st.ToString(args[1]); ok {
				name = n
			}
		}
		bin, err := Parse(text, name)
		if err != nil {
			return nil, err
		}
		return []moelua.Value{st.String(string(bin))}, nil
	}))
	_ = tbl.SetString("dump", s.NewFunc("sprotoparser.dump", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		if len(args) < 1 {
			return nil, nil
		}
		str, ok := st.ToString(args[0])
		if !ok {
			return nil, nil
		}
		var line strings.Builder
		for i := 0; i < len(str); i++ {
			fmt.Fprintf(&line, "%02X ", str[i])
			if (i+1)%8 == 0 {
				if (i+1)%16 == 0 {
					fmt.Println(line.String())
					line.Reset()
				} else {
					line.WriteString("- ")
				}
			}
		}
		if line.Len() > 0 {
			fmt.Println(line.String())
		}
		return nil, nil
	}))
	return []moelua.Value{tbl.Value()}, nil
}
