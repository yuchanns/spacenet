// Package crypt provides Skynet-compatible skynet.crypt, client.crypt, md5.core,
// and md5 Lua modules in pure Go.
package crypt

import (
	"crypto/des"
	"crypto/hmac"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"time"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/runtime"
)

const (
	paddingISO7816_4 = 0
	paddingPKCS7     = 1
	paddingCount     = 2

	dhPrime uint64 = 0xffffffffffffffc5
	dhGen   uint64 = 5
)

// New returns the crypt contrib (providing skynet.crypt, client.crypt, md5.core, md5).
func New() *runtime.Contrib {
	return runtime.NewContrib("crypt",
		runtime.WithContribFS(subFS),
		runtime.WithContribPreload("skynet.crypt", preloadCrypt),
		runtime.WithContribPreload("client.crypt", preloadCrypt),
		runtime.WithContribPreload("md5.core", preloadMD5Core),
	)
}

func preloadCrypt(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	tbl := s.NewTable(0, 18)
	funcs := map[string]moelua.GoFunc{
		"hashkey":      lHashKey,
		"randomkey":    lRandomKey,
		"desencode":    lDESEncode,
		"desdecode":    lDESDecode,
		"hexencode":    lHexEncode,
		"hexdecode":    lHexDecode,
		"hmac64":       lHMAC64,
		"hmac64_md5":   lHMAC64MD5,
		"dhexchange":   lDHExchange,
		"dhsecret":     lDHSecret,
		"base64encode": lBase64Encode,
		"base64decode": lBase64Decode,
		"sha1":         lSHA1,
		"hmac_sha1":    lHMACSHA1,
		"hmac_hash":    lHMACHash,
		"xor_str":      lXorStr,
	}
	for name, fn := range funcs {
		_ = tbl.SetString(name, s.NewFunc("crypt."+name, fn))
	}
	pad := s.NewTable(0, 2)
	_ = pad.SetString("iso7816_4", moelua.Int(paddingISO7816_4))
	_ = pad.SetString("pkcs7", moelua.Int(paddingPKCS7))
	_ = tbl.SetString("padding", pad.Value())
	return []moelua.Value{tbl.Value()}, nil
}

func checkString(s *moelua.State, args []moelua.Value, idx int) (string, error) {
	if idx >= len(args) {
		return "", fmt.Errorf("bad argument #%d (string expected)", idx+1)
	}
	if str, ok := s.ToString(args[idx]); ok {
		return str, nil
	}
	if args[idx].IsNumber() {
		return s.FormatValue(args[idx]), nil
	}
	return "", fmt.Errorf("bad argument #%d (string expected, got %s)", idx+1, args[idx].TypeName())
}

func optInt(args []moelua.Value, idx int, def int64) (int64, error) {
	if idx >= len(args) || args[idx].IsNil() {
		return def, nil
	}
	if args[idx].IsInt() {
		return args[idx].Int(), nil
	}
	if args[idx].IsFloat() {
		return int64(args[idx].Float()), nil
	}
	return 0, fmt.Errorf("bad argument #%d (number expected)", idx+1)
}

func skynetHash(str string) [8]byte {
	var djb uint32 = 5381
	var js uint32 = 1315423911
	for i := 0; i < len(str); i++ {
		c := uint32(str[i])
		djb += (djb << 5) + c
		js ^= (js << 5) + c + (js >> 2)
	}
	var out [8]byte
	binary.LittleEndian.PutUint32(out[0:4], djb)
	binary.LittleEndian.PutUint32(out[4:8], js)
	return out
}

func lHashKey(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	str, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	h := skynetHash(str)
	return []moelua.Value{s.String(string(h[:]))}, nil
}

func lRandomKey(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	var tmp [8]byte
	_, _ = rand.Read(tmp[:])
	var x byte
	for _, b := range tmp {
		x ^= b
	}
	if x == 0 {
		tmp[0] |= 1
	}
	return []moelua.Value{s.String(string(tmp[:]))}, nil
}

func lDESEncode(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	key, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	if len(key) != 8 {
		return nil, fmt.Errorf("Invalid key size %d, need 8 bytes", len(key))
	}
	text, err := checkString(s, args, 1)
	if err != nil {
		return nil, err
	}
	mode, err := optInt(args, 2, paddingISO7816_4)
	if err != nil {
		return nil, err
	}
	if mode < 0 || mode >= paddingCount {
		return nil, fmt.Errorf("Invalid padding mode %d", mode)
	}

	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return nil, err
	}

	textBytes := []byte(text)
	chunkSz := (len(textBytes) + 8) &^ 7
	out := make([]byte, chunkSz)
	i := 0
	for ; i <= len(textBytes)-8; i += 8 {
		block.Encrypt(out[i:i+8], textBytes[i:i+8])
	}
	var tail [8]byte
	rem := len(textBytes) - i
	copy(tail[:], textBytes[i:])
	switch mode {
	case paddingISO7816_4:
		tail[rem] = 0x80
	case paddingPKCS7:
		pad := byte(8 - rem)
		for j := rem; j < 8; j++ {
			tail[j] = pad
		}
	}
	block.Encrypt(out[i:i+8], tail[:])
	return []moelua.Value{s.String(string(out))}, nil
}

func lDESDecode(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	key, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	if len(key) != 8 {
		return nil, fmt.Errorf("Invalid key size %d, need 8 bytes", len(key))
	}
	text, err := checkString(s, args, 1)
	if err != nil {
		return nil, err
	}
	if (len(text)&7) != 0 || len(text) == 0 {
		return nil, fmt.Errorf("Invalid des crypt text length %d", len(text))
	}
	mode, err := optInt(args, 2, paddingISO7816_4)
	if err != nil {
		return nil, err
	}
	if mode < 0 || mode >= paddingCount {
		return nil, fmt.Errorf("Invalid padding mode %d", mode)
	}

	block, err := des.NewCipher([]byte(key))
	if err != nil {
		return nil, err
	}

	in := []byte(text)
	out := make([]byte, len(in))
	for i := 0; i < len(in); i += 8 {
		block.Decrypt(out[i:i+8], in[i:i+8])
	}

	padding := 0
	switch mode {
	case paddingISO7816_4:
		p := 1
		for i := len(out) - 1; i >= len(out)-8; i-- {
			if out[i] == 0 {
				p++
			} else if out[i] == 0x80 {
				padding = p
				break
			} else {
				break
			}
		}
	case paddingPKCS7:
		p := int(out[len(out)-1])
		if p >= 1 && p <= 8 {
			ok := true
			for i := 1; i < p; i++ {
				if int(out[len(out)-1-i]) != p {
					ok = false
					break
				}
			}
			if ok {
				padding = p
			}
		}
	}
	if padding <= 0 || padding > 8 {
		return nil, errors.New("Invalid des crypt text")
	}
	return []moelua.Value{s.String(string(out[:len(out)-padding]))}, nil
}

func lHexEncode(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	const hexChars = "0123456789abcdef"
	text, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(text)*2)
	for i := 0; i < len(text); i++ {
		out[i*2] = hexChars[text[i]>>4]
		out[i*2+1] = hexChars[text[i]&0xf]
	}
	return []moelua.Value{s.String(string(out))}, nil
}

func hexNibble(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

func lHexDecode(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	text, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	if len(text)&1 != 0 {
		return nil, fmt.Errorf("Invalid hex text size %d", len(text))
	}
	out := make([]byte, len(text)/2)
	for i := 0; i < len(text); i += 2 {
		hi, ok1 := hexNibble(text[i])
		lo, ok2 := hexNibble(text[i+1])
		if !ok1 || !ok2 {
			return nil, errors.New("Invalid hex text")
		}
		out[i/2] = (hi << 4) | lo
	}
	return []moelua.Value{s.String(string(out))}, nil
}

// Skynet's raw MD5 compression round (without final addition) for hmac64 & hmac64_md5.
var md5K = [64]uint32{
	0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee,
	0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501,
	0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be,
	0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821,
	0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa,
	0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8,
	0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed,
	0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a,
	0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c,
	0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70,
	0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05,
	0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665,
	0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039,
	0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1,
	0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1,
	0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391,
}

var md5R = [64]int{
	7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22, 7, 12, 17, 22,
	5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20, 5, 9, 14, 20,
	4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23, 4, 11, 16, 23,
	6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21, 6, 10, 15, 21,
}

func digestMD5Block(w [16]uint32) [4]uint32 {
	a := uint32(0x67452301)
	b := uint32(0xefcdab89)
	c := uint32(0x98badcfe)
	d := uint32(0x10325476)
	for i := 0; i < 64; i++ {
		var f, g uint32
		switch {
		case i < 16:
			f = (b & c) | ((^b) & d)
			g = uint32(i)
		case i < 32:
			f = (d & b) | ((^d) & c)
			g = uint32((5*i + 1) % 16)
		case i < 48:
			f = b ^ c ^ d
			g = uint32((3*i + 5) % 16)
		default:
			f = c ^ (b | (^d))
			g = uint32((7 * i) % 16)
		}
		tmp := d
		d = c
		c = b
		b = b + bits.RotateLeft32(a+f+md5K[i]+w[g], md5R[i])
		a = tmp
	}
	return [4]uint32{a, b, c, d}
}

func skynetHMAC64(x, y [2]uint32) [2]uint32 {
	var w [16]uint32
	for i := 0; i < 16; i += 4 {
		w[i] = x[1]
		w[i+1] = x[0]
		w[i+2] = y[1]
		w[i+3] = y[0]
	}
	r := digestMD5Block(w)
	return [2]uint32{r[2] ^ r[3], r[0] ^ r[1]}
}

func skynetHMAC64MD5(x, y [2]uint32) [2]uint32 {
	var w [16]uint32
	for i := 0; i < 12; i += 4 {
		w[i] = x[0]
		w[i+1] = x[1]
		w[i+2] = y[0]
		w[i+3] = y[1]
	}
	w[12] = 0x80
	w[13] = 0
	w[14] = 384
	w[15] = 0
	r := digestMD5Block(w)
	return [2]uint32{
		(r[0] + 0x67452301) ^ (r[2] + 0x98badcfe),
		(r[1] + 0xefcdab89) ^ (r[3] + 0x10325476),
	}
}

func readPair64(s *moelua.State, args []moelua.Value) ([2]uint32, [2]uint32, error) {
	sx, err := checkString(s, args, 0)
	if err != nil {
		return [2]uint32{}, [2]uint32{}, err
	}
	if len(sx) != 8 {
		return [2]uint32{}, [2]uint32{}, errors.New("Invalid uint64 x")
	}
	sy, err := checkString(s, args, 1)
	if err != nil {
		return [2]uint32{}, [2]uint32{}, err
	}
	if len(sy) != 8 {
		return [2]uint32{}, [2]uint32{}, errors.New("Invalid uint64 y")
	}
	x := [2]uint32{binary.LittleEndian.Uint32([]byte(sx[0:4])), binary.LittleEndian.Uint32([]byte(sx[4:8]))}
	y := [2]uint32{binary.LittleEndian.Uint32([]byte(sy[0:4])), binary.LittleEndian.Uint32([]byte(sy[4:8]))}
	return x, y, nil
}

func encodeQword(r [2]uint32) string {
	var out [8]byte
	binary.LittleEndian.PutUint32(out[0:4], r[0])
	binary.LittleEndian.PutUint32(out[4:8], r[1])
	return string(out[:])
}

func lHMAC64(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	x, y, err := readPair64(s, args)
	if err != nil {
		return nil, err
	}
	return []moelua.Value{s.String(encodeQword(skynetHMAC64(x, y)))}, nil
}

func lHMAC64MD5(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	x, y, err := readPair64(s, args)
	if err != nil {
		return nil, err
	}
	return []moelua.Value{s.String(encodeQword(skynetHMAC64MD5(x, y)))}, nil
}

func lHMACHash(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	ks, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	if len(ks) != 8 {
		return nil, errors.New("Invalid uint64 key")
	}
	text, err := checkString(s, args, 1)
	if err != nil {
		return nil, err
	}
	key := [2]uint32{binary.LittleEndian.Uint32([]byte(ks[0:4])), binary.LittleEndian.Uint32([]byte(ks[4:8]))}
	h := skynetHash(text)
	htext := [2]uint32{binary.LittleEndian.Uint32(h[0:4]), binary.LittleEndian.Uint32(h[4:8])}
	return []moelua.Value{s.String(encodeQword(skynetHMAC64(htext, key)))}, nil
}

func mulModP(a, b uint64) uint64 {
	var m uint64
	for b != 0 {
		if b&1 != 0 {
			t := dhPrime - a
			if m >= t {
				m -= t
			} else {
				m += a
			}
		}
		if a >= dhPrime-a {
			a = a*2 - dhPrime
		} else {
			a = a * 2
		}
		b >>= 1
	}
	return m
}

func powModP(a, b uint64) uint64 {
	if a > dhPrime {
		a %= dhPrime
	}
	res := uint64(1)
	base := a
	exp := b
	for exp > 0 {
		if exp&1 != 0 {
			res = mulModP(res, base)
		}
		exp >>= 1
		if exp > 0 {
			base = mulModP(base, base)
		}
	}
	return res
}

func encodeUint64(v uint64) string {
	var out [8]byte
	binary.LittleEndian.PutUint64(out[:], v)
	return string(out[:])
}

func lDHExchange(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	xs, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	if len(xs) != 8 {
		return nil, errors.New("Invalid dh uint64 key")
	}
	x := binary.LittleEndian.Uint64([]byte(xs))
	if x == 0 {
		return nil, errors.New("Can't be 0")
	}
	return []moelua.Value{s.String(encodeUint64(powModP(dhGen, x)))}, nil
}

func lDHSecret(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	xs, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	if len(xs) != 8 {
		return nil, errors.New("Invalid uint64 x")
	}
	ys, err := checkString(s, args, 1)
	if err != nil {
		return nil, err
	}
	if len(ys) != 8 {
		return nil, errors.New("Invalid uint64 y")
	}
	x := binary.LittleEndian.Uint64([]byte(xs))
	y := binary.LittleEndian.Uint64([]byte(ys))
	if x == 0 || y == 0 {
		return nil, errors.New("Can't be 0")
	}
	return []moelua.Value{s.String(encodeUint64(powModP(x, y)))}, nil
}

func lBase64Encode(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	const enc = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	text, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	sz := len(text)
	out := make([]byte, (sz+2)/3*4)
	i, j := 0, 0
	for ; i < sz-2; i += 3 {
		v := uint32(text[i])<<16 | uint32(text[i+1])<<8 | uint32(text[i+2])
		out[j] = enc[v>>18]
		out[j+1] = enc[(v>>12)&0x3f]
		out[j+2] = enc[(v>>6)&0x3f]
		out[j+3] = enc[v&0x3f]
		j += 4
	}
	switch sz - i {
	case 1:
		v := uint32(text[i])
		out[j] = enc[v>>2]
		out[j+1] = enc[(v&3)<<4]
		out[j+2] = '='
		out[j+3] = '='
	case 2:
		v := uint32(text[i])<<8 | uint32(text[i+1])
		out[j] = enc[v>>10]
		out[j+1] = enc[(v>>4)&0x3f]
		out[j+2] = enc[(v&0xf)<<2]
		out[j+3] = '='
	}
	return []moelua.Value{s.String(string(out))}, nil
}

var b64Table = [...]int{
	62, -1, -1, -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1,
	-1, -1, -2, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
	-1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35,
	36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
}

func b64Index(c byte) int {
	if c < 43 {
		return -1
	}
	idx := int(c - 43)
	if idx >= len(b64Table) {
		return -1
	}
	return b64Table[idx]
}

// lBase64Decode matches lua-crypt.c's exact lenient whitespace/missing-padding rules.
func lBase64Decode(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	text, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	sz := len(text)
	out := make([]byte, 0, (sz+3)/4*3)
	for i := 0; i < sz; {
		padding := 0
		var c [4]int
		for j := 0; j < 4; {
			if i >= sz {
				c[j] = -2
			} else {
				c[j] = b64Index(text[i])
			}
			if c[j] == -1 {
				i++
				continue
			}
			if c[j] == -2 {
				padding++
			}
			i++
			j++
		}
		switch padding {
		case 0:
			v := uint32(c[0])<<18 | uint32(c[1])<<12 | uint32(c[2])<<6 | uint32(c[3])
			out = append(out, byte(v>>16), byte(v>>8), byte(v))
		case 1:
			if c[3] != -2 || (c[2]&3) != 0 {
				return nil, errors.New("Invalid base64 text")
			}
			v := uint32(c[0])<<10 | uint32(c[1])<<4 | uint32(c[2])>>2
			out = append(out, byte(v>>8), byte(v))
		case 2:
			if c[3] != -2 || c[2] != -2 || (c[1]&0xf) != 0 {
				return nil, errors.New("Invalid base64 text")
			}
			v := uint32(c[0])<<2 | uint32(c[1])>>4
			out = append(out, byte(v))
		default:
			return nil, errors.New("Invalid base64 text")
		}
	}
	return []moelua.Value{s.String(string(out))}, nil
}

func lSHA1(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	text, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	sum := sha1.Sum([]byte(text))
	return []moelua.Value{s.String(string(sum[:]))}, nil
}

func lHMACSHA1(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	key, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	text, err := checkString(s, args, 1)
	if err != nil {
		return nil, err
	}
	mac := hmac.New(sha1.New, []byte(key))
	_, _ = mac.Write([]byte(text))
	return []moelua.Value{s.String(string(mac.Sum(nil)))}, nil
}

func lXorStr(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	s1, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	s2, err := checkString(s, args, 1)
	if err != nil {
		return nil, err
	}
	if len(s2) == 0 {
		return nil, errors.New("Can't xor empty string")
	}
	out := make([]byte, len(s1))
	for i := 0; i < len(s1); i++ {
		out[i] = s1[i] ^ s2[i%len(s2)]
	}
	return []moelua.Value{s.String(string(out))}, nil
}

// ---------- md5.core ----------

func preloadMD5Core(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	tbl := s.NewTable(0, 8)
	_ = tbl.SetString("sum", s.NewFunc("md5.sum", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		msg, err := checkString(st, args, 0)
		if err != nil {
			return nil, err
		}
		sum := md5.Sum([]byte(msg))
		return []moelua.Value{st.String(string(sum[:]))}, nil
	}))
	_ = tbl.SetString("exor", s.NewFunc("md5.exor", func(st *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
		s1, err := checkString(st, args, 0)
		if err != nil {
			return nil, err
		}
		s2, err := checkString(st, args, 1)
		if err != nil {
			return nil, err
		}
		if len(s1) != len(s2) {
			return nil, errors.New("bad argument #2 to 'exor' (lengths must be equal)")
		}
		out := make([]byte, len(s1))
		for i := range out {
			out[i] = s1[i] ^ s2[i]
		}
		return []moelua.Value{st.String(string(out))}, nil
	}))
	_ = tbl.SetString("crypt", s.NewFunc("md5.crypt", lMD5Crypt))
	_ = tbl.SetString("decrypt", s.NewFunc("md5.decrypt", lMD5Decrypt))
	_ = tbl.SetString("_COPYRIGHT", s.String("Copyright (C) 2003-2013 PUC-Rio"))
	_ = tbl.SetString("_DESCRIPTION", s.String("Basic cryptographic facilities"))
	_ = tbl.SetString("_VERSION", s.String("MD5 1.2"))
	return []moelua.Value{tbl.Value()}, nil
}

func lMD5Crypt(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	msg, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	key, err := checkString(s, args, 1)
	if err != nil {
		return nil, err
	}
	if len(key) > 256 {
		return nil, errors.New("key too long (> 256)")
	}
	var seed []byte
	if len(args) >= 3 && !args[2].IsNil() {
		ss, err := checkString(s, args, 2)
		if err != nil {
			return nil, err
		}
		seed = []byte(ss)
	} else {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], uint64(time.Now().Unix()))
		seed = tmp[:]
	}
	if len(seed) > 16 {
		return nil, errors.New("seed too long (> 16)")
	}
	block := make([]byte, 16+len(key))
	copy(block[:len(seed)], seed)
	copy(block[16:], key)

	out := make([]byte, 0, 1+len(seed)+len(msg))
	out = append(out, byte(len(seed)))
	out = append(out, seed...)
	for pos := 0; pos < len(msg); {
		code := md5.Sum(block)
		n := min(16, len(msg)-pos)
		for i := 0; i < n; i++ {
			code[i] ^= msg[pos+i]
		}
		out = append(out, code[:n]...)
		copy(block[:n], code[:n])
		pos += n
	}
	return []moelua.Value{s.String(string(out))}, nil
}

func lMD5Decrypt(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	ct, err := checkString(s, args, 0)
	if err != nil {
		return nil, err
	}
	key, err := checkString(s, args, 1)
	if err != nil {
		return nil, err
	}
	if len(key) > 256 {
		return nil, errors.New("key too long (> 256)")
	}
	if len(ct) < 1 {
		return nil, errors.New("invalid cyphered string")
	}
	lseed := int(ct[0])
	if lseed > 16 || len(ct) < lseed+1 {
		return nil, errors.New("invalid cyphered string")
	}
	seed := ct[1 : 1+lseed]
	body := ct[1+lseed:]

	block := make([]byte, 16+len(key))
	copy(block[:lseed], seed)
	copy(block[16:], key)

	out := make([]byte, 0, len(body))
	for pos := 0; pos < len(body); {
		code := md5.Sum(block)
		n := min(16, len(body)-pos)
		for i := 0; i < n; i++ {
			code[i] ^= body[pos+i]
		}
		out = append(out, code[:n]...)
		copy(block[:n], body[pos:pos+n])
		pos += n
	}
	return []moelua.Value{s.String(string(out))}, nil
}
