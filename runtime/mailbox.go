package runtime

import (
	stdruntime "runtime"
	"sync"
)

const (
	defaultMailboxCap = 8
	mqOverloadBase    = 1024
)

// Mailbox is a per-Actor ring queue matching skynet_mq.c semantics:
// single-consumer dispatch, inGlobal scheduling flag, and 1024-doubling overload tracking.
type Mailbox struct {
	mu                sync.Mutex
	handle            uint32
	queue             []Message
	head              int
	tail              int
	inGlobal          bool
	released          bool
	overload          int
	overloadThreshold int
	next              *Mailbox // intrusive pointer used exclusively by GlobalQueue under gq.mu
}

func NewMailbox(handle uint32) *Mailbox {
	return &Mailbox{
		handle:            handle,
		queue:             make([]Message, defaultMailboxCap),
		inGlobal:          true, // held until actor Init succeeds, then pushed to GlobalQueue
		overloadThreshold: mqOverloadBase,
	}
}

func (m *Mailbox) Handle() uint32 {
	return m.handle
}

func (m *Mailbox) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.head <= m.tail {
		return m.tail - m.head
	}
	return m.tail + len(m.queue) - m.head
}

func (m *Mailbox) ConsumeOverload() int {
	m.mu.Lock()
	ov := m.overload
	m.overload = 0
	m.mu.Unlock()
	return ov
}

func (m *Mailbox) Push(msg Message, gq *GlobalQueue) {
	m.mu.Lock()
	m.queue[m.tail] = msg
	m.tail++
	if m.tail >= len(m.queue) {
		m.tail = 0
	}
	if m.head == m.tail {
		m.expandLocked()
	}
	needSchedule := false
	if !m.inGlobal {
		m.inGlobal = true
		needSchedule = true
	}
	m.mu.Unlock()

	if needSchedule && gq != nil {
		gq.Push(m)
	}
}

func (m *Mailbox) expandLocked() {
	oldCap := len(m.queue)
	newQ := make([]Message, oldCap*2)
	for i := 0; i < oldCap; i++ {
		newQ[i] = m.queue[(m.head+i)%oldCap]
	}
	m.head = 0
	m.tail = oldCap
	m.queue = newQ
}

func (m *Mailbox) Pop() (Message, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.head == m.tail {
		m.overloadThreshold = mqOverloadBase
		m.inGlobal = false
		return Message{}, false
	}

	msg := m.queue[m.head]
	m.queue[m.head] = Message{} // clear references for GC
	m.head++
	if m.head >= len(m.queue) {
		m.head = 0
	}

	length := m.tail - m.head
	if length < 0 {
		length += len(m.queue)
	}
	for length > m.overloadThreshold {
		m.overload = length
		m.overloadThreshold *= 2
	}
	return msg, true
}

func (m *Mailbox) MarkRelease(gq *GlobalQueue) {
	m.mu.Lock()
	if m.released {
		m.mu.Unlock()
		return
	}
	m.released = true
	needSchedule := false
	if !m.inGlobal {
		m.inGlobal = true
		needSchedule = true
	}
	m.mu.Unlock()

	if needSchedule && gq != nil {
		gq.Push(m)
	}
}

// DrainIfReleased drains remaining messages with dropFn if the mailbox has been
// marked released (or immediately when gq is nil during Node shutdown). If not yet
// released and gq is active, yields briefly and re-enqueues so the retiring goroutine
// completes Release() without spinning.
func (m *Mailbox) DrainIfReleased(gq *GlobalQueue, dropFn func(Message)) {
	m.mu.Lock()
	rel := m.released
	if !rel && gq != nil {
		m.mu.Unlock()
		stdruntime.Gosched()
		gq.Push(m)
		return
	}
	m.mu.Unlock()

	for {
		msg, ok := m.Pop()
		if !ok {
			return
		}
		if dropFn != nil {
			dropFn(msg)
		}
	}
}

// GlobalQueue is the shared FIFO ready-queue of active actor mailboxes.
type GlobalQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	head   *Mailbox
	tail   *Mailbox
	closed bool
}

func NewGlobalQueue() *GlobalQueue {
	gq := &GlobalQueue{}
	gq.cond = sync.NewCond(&gq.mu)
	return gq
}

func (gq *GlobalQueue) Push(m *Mailbox) {
	gq.mu.Lock()
	if gq.closed {
		gq.mu.Unlock()
		return
	}
	m.next = nil
	if gq.tail != nil {
		gq.tail.next = m
		gq.tail = m
	} else {
		gq.head = m
		gq.tail = m
	}
	gq.cond.Signal()
	gq.mu.Unlock()
}

func (gq *GlobalQueue) Pop() *Mailbox {
	gq.mu.Lock()
	m := gq.head
	if m != nil {
		gq.head = m.next
		if gq.head == nil {
			gq.tail = nil
		}
		m.next = nil
	}
	gq.mu.Unlock()
	return m
}

// PopWait blocks until a mailbox is available or the queue is closed.
func (gq *GlobalQueue) PopWait() *Mailbox {
	gq.mu.Lock()
	for gq.head == nil && !gq.closed {
		gq.cond.Wait()
	}
	m := gq.head
	if m != nil {
		gq.head = m.next
		if gq.head == nil {
			gq.tail = nil
		}
		m.next = nil
	}
	gq.mu.Unlock()
	return m
}

func (gq *GlobalQueue) Close() {
	gq.mu.Lock()
	gq.closed = true
	gq.cond.Broadcast()
	gq.mu.Unlock()
}
