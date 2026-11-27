package socket

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yuchanns/spacenet/runtime"
)

type transientErrListener struct {
	mu       sync.Mutex
	failures int
	accepted atomic.Int32
	closed   bool
	doneCh   chan struct{}
	connCh   chan net.Conn
}

func (l *transientErrListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil, net.ErrClosed
	}
	if l.failures > 0 {
		l.failures--
		l.mu.Unlock()
		return nil, errors.New("too many open files")
	}
	l.mu.Unlock()

	select {
	case c := <-l.connCh:
		l.accepted.Add(1)
		return c, nil
	case <-l.doneCh:
		return nil, net.ErrClosed
	}
}

func (l *transientErrListener) Close() error {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		close(l.doneCh)
	}
	l.mu.Unlock()
	return nil
}

func (l *transientErrListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0}
}

func TestAcceptLoopRetriesRecoverableError(t *testing.T) {
	node, err := runtime.NewNode(runtime.Config{
		Threads:   1,
		LogOutput: io.Discard,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	mgr := newManager(node)
	defer mgr.Close()

	// Logger handle is always alive in the node and accepts pushed messages.
	owner := node.Handles().FindName("logger")
	if owner == 0 {
		t.Fatal("logger handle not found")
	}

	ln := &transientErrListener{
		failures: 2,
		doneCh:   make(chan struct{}),
		connCh:   make(chan net.Conn, 1),
	}
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	ln.connCh <- c2

	e := mgr.allocEntry(kindListen, owner)
	e.listener = ln
	e.startAcceptLoop(mgr)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ln.accepted.Load() >= 1 {
			e.forceClose(mgr, false)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("accept loop exited permanently instead of retrying recoverable error")
}
