// Package tls provides crypto/tls bindings compatible with Skynet's ltls.c
// and ltls.init.c modules.
package tls

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuchanns/moelua"
	"github.com/yuchanns/spacenet/runtime"
	"github.com/yuchanns/spacenet/runtime/contrib"
)

const (
	extKey     = "contrib.tls.manager"
	metaSSLCtx = "_TLS_SSLCTX_METATABLE_"
	metaTLSCtx = "_TLS_CONTEXT_METATABLE_"
)

// Option configures the TLS contrib.
type Option func(*options)

type options struct {
	baseCfg *tls.Config
}

// WithBaseConfig sets a base *tls.Config template cloned for every new TLS context.
// By default InsecureSkipVerify is true to match Skynet's upstream ltls.c (SSL_VERIFY_NONE).
func WithBaseConfig(cfg *tls.Config) Option {
	return func(o *options) { o.baseCfg = cfg }
}

type tlsManager struct {
	baseCfg *tls.Config
	inited  atomic.Bool

	mu       sync.Mutex
	closed   bool
	sessions map[*tlsSession]struct{}
}

func newTLSManager(baseCfg *tls.Config) *tlsManager {
	var cfg *tls.Config
	if baseCfg != nil {
		cfg = baseCfg.Clone()
	}
	m := &tlsManager{
		baseCfg:  cfg,
		sessions: make(map[*tlsSession]struct{}),
	}
	// Mark ready automatically when the tls contrib is installed (and also allow
	// bootstrap's ltls.init.c constructor/destructor to toggle cleanly per Node).
	m.inited.Store(true)
	return m
}

func (m *tlsManager) track(sess *tlsSession) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	m.sessions[sess] = struct{}{}
	return true
}

func (m *tlsManager) untrack(sess *tlsSession) {
	m.mu.Lock()
	delete(m.sessions, sess)
	m.mu.Unlock()
}

func (m *tlsManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	active := make([]*tlsSession, 0, len(m.sessions))
	for sess := range m.sessions {
		active = append(active, sess)
	}
	clear(m.sessions)
	m.mu.Unlock()

	for _, sess := range active {
		sess.close()
	}
	return nil
}

// New returns the TLS contrib providing ltls.c and ltls.init.c.
func New(opts ...Option) *contrib.Contrib {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	var baseCfg *tls.Config
	if o.baseCfg != nil {
		baseCfg = o.baseCfg.Clone()
	}
	return contrib.New("tls",
		contrib.WithInit(func(n *runtime.Node) (io.Closer, error) {
			mgr := newTLSManager(baseCfg)
			n.SetExt(extKey, mgr)
			return mgr, nil
		}),
		contrib.WithPreload("ltls.c", preloadLTLS),
		contrib.WithPreload("ltls.init.c", preloadLTLSInit),
	)
}

func managerFromState(s *moelua.State) (*tlsManager, *runtime.Actor, error) {
	actor, err := runtime.ActorFromState(s)
	if err != nil {
		return nil, nil, err
	}
	mgr, ok := actor.Node().Ext(extKey).(*tlsManager)
	if !ok || mgr == nil {
		return nil, nil, errors.New("tls contrib not initialized on node")
	}
	return mgr, actor, nil
}

func preloadLTLSInit(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	mgr, _, err := managerFromState(s)
	if err != nil {
		return nil, err
	}
	tbl := s.NewTable(0, 2)
	_ = tbl.SetString("constructor", s.NewFunc("ltls.init.constructor", func(_ *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		mgr.inited.Store(true)
		return nil, nil
	}))
	_ = tbl.SetString("destructor", s.NewFunc("ltls.init.destructor", func(_ *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
		mgr.inited.Store(false)
		return nil, nil
	}))
	return []moelua.Value{tbl.Value()}, nil
}

func preloadLTLS(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	mgr, _, err := managerFromState(s)
	if err != nil {
		return nil, err
	}
	if !mgr.inited.Load() {
		return nil, errors.New("ltls need init, Put enablessl = true in you config file.")
	}
	tbl := s.NewTable(0, 2)
	_ = tbl.SetString("newctx", s.NewFunc("ltls.newctx", mgr.lNewCtx))
	_ = tbl.SetString("newtls", s.NewFunc("ltls.newtls", mgr.lNewTLS))
	return []moelua.Value{tbl.Value()}, nil
}

type sslCtx struct {
	mu   sync.Mutex
	cfg  *tls.Config
	cert []tls.Certificate
}

func (m *tlsManager) lNewCtx(s *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
	var cfg *tls.Config
	if m.baseCfg != nil {
		cfg = m.baseCfg.Clone()
	} else {
		cfg = &tls.Config{
			InsecureSkipVerify: true, // matches upstream Skynet ltls.c default (SSL_VERIFY_NONE)
			MinVersion:         tls.VersionTLS12,
		}
	}
	ctx := &sslCtx{cfg: cfg}
	mt, created := s.NewMetatable(metaSSLCtx)
	if created {
		idx := s.NewTable(0, 2)
		_ = idx.SetString("set_cert", s.NewFunc("sslctx.set_cert", lCtxSetCert))
		_ = idx.SetString("set_ciphers", s.NewFunc("sslctx.set_ciphers", lCtxSetCiphers))
		_ = mt.SetString("__index", idx.Value())
		_ = mt.SetString("__gc", s.NewFunc("sslctx.__gc", func(_ *moelua.State, _ []moelua.Value) ([]moelua.Value, error) {
			return nil, nil
		}))
	}
	ud, err := s.NewUserdataWithMeta(ctx, mt)
	if err != nil {
		return nil, err
	}
	return []moelua.Value{ud}, nil
}

func checkSSLCtx(s *moelua.State, args []moelua.Value, idx int) (*sslCtx, error) {
	if idx >= len(args) {
		return nil, errors.New("need sslctx")
	}
	v, ok := s.CheckUserdata(args[idx], metaSSLCtx)
	if !ok {
		return nil, errors.New("need sslctx")
	}
	ctx, ok := v.(*sslCtx)
	if !ok || ctx == nil {
		return nil, errors.New("need sslctx")
	}
	return ctx, nil
}

func lCtxSetCert(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	ctx, err := checkSSLCtx(s, args, 0)
	if err != nil {
		return nil, err
	}
	if len(args) < 3 {
		return nil, errors.New("need certfile and private key")
	}
	certFile, ok := s.ToString(args[1])
	if !ok || certFile == "" {
		return nil, errors.New("need certfile")
	}
	keyFile, ok := s.ToString(args[2])
	if !ok || keyFile == "" {
		return nil, errors.New("need private key")
	}
	var cert tls.Certificate
	if actor, aerr := runtime.ActorFromState(s); aerr == nil && actor.Node().VFS() != nil {
		certPEM, _, cerr := actor.Node().VFS().ReadFile(certFile)
		if cerr != nil {
			return nil, fmt.Errorf("load cert failed: %w", cerr)
		}
		keyPEM, _, kerr := actor.Node().VFS().ReadFile(keyFile)
		if kerr != nil {
			return nil, fmt.Errorf("load key failed: %w", kerr)
		}
		cert, err = tls.X509KeyPair(certPEM, keyPEM)
	} else {
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
	}
	if err != nil {
		return nil, fmt.Errorf("load cert/key failed: %w", err)
	}
	ctx.mu.Lock()
	ctx.cert = []tls.Certificate{cert}
	ctx.cfg.Certificates = ctx.cert
	ctx.mu.Unlock()
	return nil, nil
}

func lCtxSetCiphers(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	if _, err := checkSSLCtx(s, args, 0); err != nil {
		return nil, err
	}
	if len(args) < 2 || !args[1].IsString() {
		return nil, errors.New("need cipher list")
	}
	// Go's crypto/tls negotiates safe cipher suites automatically.
	return nil, nil
}

// bioConn is an in-memory net.Conn that never returns temporary errors on Read
// (preventing crypto/tls.Conn permanent error poisoning) and coordinates exact
// handshake/record step boundaries with Lua's non-blocking BIO API.
type bioConn struct {
	mu          sync.Mutex
	cond        *sync.Cond
	inBuf       []byte
	outBuf      []byte
	waitingRead bool
	closed      bool
}

func newBioConn() *bioConn {
	b := &bioConn{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *bioConn) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.inBuf) == 0 && !b.closed {
		b.waitingRead = true
		b.cond.Broadcast()
		b.cond.Wait()
	}
	b.waitingRead = false
	if len(b.inBuf) == 0 && b.closed {
		return 0, io.EOF
	}
	n := copy(p, b.inBuf)
	if n == len(b.inBuf) {
		b.inBuf = b.inBuf[:0]
	} else {
		b.inBuf = append(b.inBuf[:0], b.inBuf[n:]...)
	}
	return n, nil
}

func (b *bioConn) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, net.ErrClosed
	}
	b.outBuf = append(b.outBuf, p...)
	b.cond.Broadcast()
	return len(p), nil
}

func (b *bioConn) Close() error {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.cond.Broadcast()
	}
	b.mu.Unlock()
	return nil
}

func (b *bioConn) feed(data []byte) {
	b.mu.Lock()
	if len(data) > 0 {
		b.inBuf = append(b.inBuf, data...)
		b.waitingRead = false
	}
	b.cond.Broadcast()
	b.mu.Unlock()
}

func (b *bioConn) drainOut() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.outBuf) == 0 {
		return nil
	}
	out := make([]byte, len(b.outBuf))
	copy(out, b.outBuf)
	b.outBuf = b.outBuf[:0]
	return out
}

type dummyAddr struct{}

func (dummyAddr) Network() string { return "bio" }
func (dummyAddr) String() string  { return "bio" }

func (b *bioConn) LocalAddr() net.Addr                { return dummyAddr{} }
func (b *bioConn) RemoteAddr() net.Addr               { return dummyAddr{} }
func (b *bioConn) SetDeadline(_ time.Time) error      { return nil }
func (b *bioConn) SetReadDeadline(_ time.Time) error  { return nil }
func (b *bioConn) SetWriteDeadline(_ time.Time) error { return nil }

type tlsSession struct {
	mgr      *tlsManager
	isServer bool
	cfg      *tls.Config
	bio      *bioConn
	conn     *tls.Conn

	mu            sync.Mutex
	closed        bool
	hsStarted     bool
	hsDone        bool
	hsErr         error
	readerStarted bool
	plainBuf      []byte
	readErr       error
}

func (sess *tlsSession) close() {
	sess.mu.Lock()
	already := sess.closed
	sess.closed = true
	sess.mu.Unlock()
	if !already {
		_ = sess.bio.Close()
		if sess.mgr != nil {
			sess.mgr.untrack(sess)
		}
	}
}

func (m *tlsManager) lNewTLS(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	method := "nil"
	if len(args) >= 1 && !args[0].IsNil() {
		if str, ok := s.ToString(args[0]); ok {
			method = str
		}
	}
	ctx, err := checkSSLCtx(s, args, 1)
	if err != nil {
		return nil, err
	}
	ctx.mu.Lock()
	cfg := ctx.cfg.Clone()
	ctx.mu.Unlock()

	var isServer bool
	switch method {
	case "client":
		isServer = false
		if len(args) >= 3 && !args[2].IsNil() {
			if host, ok := s.ToString(args[2]); ok {
				cfg.ServerName = host
			}
		}
	case "server":
		isServer = true
	default:
		return nil, fmt.Errorf("invalid method:%s e.g[server, client]", method)
	}

	sess := &tlsSession{
		mgr:      m,
		isServer: isServer,
		cfg:      cfg,
		bio:      newBioConn(),
	}
	if !m.track(sess) {
		return nil, errors.New("tls manager is closed")
	}

	mt, created := s.NewMetatable(metaTLSCtx)
	if created {
		idx := s.NewTable(0, 6)
		_ = idx.SetString("close", s.NewFunc("tls.close", lTLSClose))
		_ = idx.SetString("finished", s.NewFunc("tls.finished", lTLSFinished))
		_ = idx.SetString("handshake", s.NewFunc("tls.handshake", lTLSHandshake))
		_ = idx.SetString("read", s.NewFunc("tls.read", lTLSRead))
		_ = idx.SetString("write", s.NewFunc("tls.write", lTLSWrite))
		_ = idx.SetString("set_ext_host_name", s.NewFunc("tls.set_ext_host_name", lTLSSetHost))
		_ = mt.SetString("__index", idx.Value())
		_ = mt.SetString("__gc", s.NewFunc("tls.__gc", lTLSClose))
	}
	ud, err := s.NewUserdataWithMeta(sess, mt)
	if err != nil {
		sess.close()
		return nil, err
	}
	return []moelua.Value{ud}, nil
}

func checkTLSSession(s *moelua.State, args []moelua.Value, allowClosed bool) (*tlsSession, error) {
	if len(args) == 0 {
		return nil, errors.New("need tls context")
	}
	v, ok := s.CheckUserdata(args[0], metaTLSCtx)
	if !ok {
		return nil, errors.New("need tls context")
	}
	sess, ok := v.(*tlsSession)
	if !ok || sess == nil {
		return nil, errors.New("need tls context")
	}
	sess.mu.Lock()
	closed := sess.closed
	sess.mu.Unlock()
	if closed && !allowClosed {
		return nil, errors.New("context is closed")
	}
	return sess, nil
}

func (sess *tlsSession) ensureConnLocked() {
	if sess.conn != nil {
		return
	}
	if sess.isServer {
		sess.conn = tls.Server(sess.bio, sess.cfg)
	} else {
		sess.conn = tls.Client(sess.bio, sess.cfg)
	}
}

func (sess *tlsSession) startReaderLocked() {
	if sess.readerStarted {
		return
	}
	sess.readerStarted = true
	conn := sess.conn
	go func() {
		buf := make([]byte, 16*1024)
		for {
			n, err := conn.Read(buf)
			sess.bio.mu.Lock()
			if n > 0 {
				sess.plainBuf = append(sess.plainBuf, buf[:n]...)
			}
			if err != nil {
				sess.readErr = err
				sess.bio.cond.Broadcast()
				sess.bio.mu.Unlock()
				return
			}
			sess.bio.cond.Broadcast()
			sess.bio.mu.Unlock()
		}
	}()
}

func lTLSSetHost(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	sess, err := checkTLSSession(s, args, false)
	if err != nil {
		return nil, err
	}
	host := ""
	if len(args) >= 2 && !args[1].IsNil() {
		host, _ = s.ToString(args[1])
	}
	sess.mu.Lock()
	if !sess.hsStarted {
		sess.cfg.ServerName = host
	}
	sess.mu.Unlock()
	return []moelua.Value{moelua.Int(1)}, nil
}

func lTLSFinished(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	sess, err := checkTLSSession(s, args, false)
	if err != nil {
		return nil, err
	}
	sess.bio.mu.Lock()
	done := sess.hsDone && sess.hsErr == nil
	sess.bio.mu.Unlock()
	return []moelua.Value{moelua.Bool(done)}, nil
}

func lTLSHandshake(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	sess, err := checkTLSSession(s, args, false)
	if err != nil {
		return nil, err
	}

	sess.bio.mu.Lock()
	if sess.hsDone && sess.hsErr == nil {
		sess.bio.mu.Unlock()
		return nil, errors.New("handshake is finished")
	}
	sess.bio.mu.Unlock()

	if len(args) >= 2 && !args[1].IsNil() {
		if str, ok := s.ToString(args[1]); ok && len(str) > 0 {
			sess.bio.feed([]byte(str))
		}
	}

	sess.mu.Lock()
	if !sess.hsStarted {
		sess.hsStarted = true
		sess.ensureConnLocked()
		conn := sess.conn
		go func() {
			herr := conn.Handshake()
			sess.bio.mu.Lock()
			sess.hsDone = true
			sess.hsErr = herr
			sess.bio.cond.Broadcast()
			sess.bio.mu.Unlock()
		}()
	}
	sess.mu.Unlock()

	sess.bio.mu.Lock()
	for !sess.hsDone && !(sess.bio.waitingRead && len(sess.bio.inBuf) == 0) && !sess.bio.closed {
		sess.bio.cond.Wait()
	}
	herr := sess.hsErr
	done := sess.hsDone
	sess.bio.mu.Unlock()

	if done && herr != nil {
		return nil, fmt.Errorf("SSL_do_handshake error: %w", herr)
	}
	out := sess.bio.drainOut()
	if len(out) > 0 {
		return []moelua.Value{s.String(string(out))}, nil
	}
	return nil, nil
}

func lTLSRead(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	sess, err := checkTLSSession(s, args, false)
	if err != nil {
		return nil, err
	}
	sess.bio.mu.Lock()
	hsDone := sess.hsDone
	hsErr := sess.hsErr
	sess.bio.mu.Unlock()
	if !hsDone {
		return nil, errors.New("tls handshake not completed")
	}
	if hsErr != nil {
		return nil, fmt.Errorf("SSL_read error: %w", hsErr)
	}

	if len(args) >= 2 && !args[1].IsNil() {
		if str, ok := s.ToString(args[1]); ok && len(str) > 0 {
			sess.bio.feed([]byte(str))
		}
	}

	sess.mu.Lock()
	sess.ensureConnLocked()
	sess.startReaderLocked()
	sess.mu.Unlock()

	sess.bio.mu.Lock()
	for !(len(sess.bio.inBuf) == 0 && (sess.bio.waitingRead || sess.readErr != nil || sess.bio.closed)) {
		sess.bio.cond.Wait()
	}
	var out []byte
	if len(sess.plainBuf) > 0 {
		out = make([]byte, len(sess.plainBuf))
		copy(out, sess.plainBuf)
		sess.plainBuf = sess.plainBuf[:0]
	}
	rerr := sess.readErr
	sess.bio.mu.Unlock()

	if len(out) == 0 && rerr != nil && !errors.Is(rerr, io.EOF) {
		return nil, fmt.Errorf("SSL_read error: %w", rerr)
	}
	return []moelua.Value{s.String(string(out))}, nil
}

func lTLSWrite(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	sess, err := checkTLSSession(s, args, false)
	if err != nil {
		return nil, err
	}
	var plain string
	if len(args) >= 2 && !args[1].IsNil() {
		plain, _ = s.ToString(args[1])
	}
	if len(plain) > 0 {
		sess.bio.mu.Lock()
		hsDone := sess.hsDone
		hsErr := sess.hsErr
		sess.bio.mu.Unlock()
		if !hsDone {
			return nil, errors.New("tls handshake not completed")
		}
		if hsErr != nil {
			return nil, fmt.Errorf("SSL_write error: %w", hsErr)
		}
		sess.mu.Lock()
		conn := sess.conn
		sess.mu.Unlock()
		if conn == nil {
			return nil, errors.New("tls handshake not completed")
		}
		if _, err := conn.Write([]byte(plain)); err != nil {
			return nil, fmt.Errorf("SSL_write error: %w", err)
		}
	}
	out := sess.bio.drainOut()
	return []moelua.Value{s.String(string(out))}, nil
}

func lTLSClose(s *moelua.State, args []moelua.Value) ([]moelua.Value, error) {
	sess, err := checkTLSSession(s, args, true)
	if err != nil {
		return nil, nil
	}
	sess.close()
	return nil, nil
}
