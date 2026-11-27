package runtime

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

// TimerManager provides Skynet's 10ms centisecond clock (skynet.now, skynet.starttime,
// skynet.hpc) and scheduled TIMEOUT dispatch via a min-heap + single timer goroutine
// (zero per-timeout goroutine leaks at 10,000+ agents).
type TimerManager struct {
	startUnix uint64
	epoch     time.Time
	nowTicks  atomic.Uint64 // centiseconds (10ms) since epoch

	mu     sync.Mutex
	events timerHeap
	seq    uint64
	wake   chan struct{}
	stop   chan struct{}
	done   chan struct{}
	fire   func(handle uint32, session int32)
}

type timerEvent struct {
	expire  uint64
	seq     uint64
	handle  uint32
	session int32
}

type timerHeap []timerEvent

func (h timerHeap) Len() int { return len(h) }
func (h timerHeap) Less(i, j int) bool {
	if h[i].expire != h[j].expire {
		return h[i].expire < h[j].expire
	}
	return h[i].seq < h[j].seq
}
func (h timerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)   { *h = append(*h, x.(timerEvent)) }
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	ev := old[n-1]
	*h = old[:n-1]
	return ev
}

func NewTimerManager(fire func(handle uint32, session int32)) *TimerManager {
	now := time.Now()
	tm := &TimerManager{
		startUnix: uint64(now.Unix()),
		epoch:     now,
		wake:      make(chan struct{}, 1),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
		fire:      fire,
	}
	go tm.loop()
	return tm
}

func (tm *TimerManager) StartTime() uint64 {
	return tm.startUnix
}

// Now returns centiseconds (10ms ticks) elapsed since node startup.
func (tm *TimerManager) Now() uint64 {
	// Keep wall-monotonic progression accurate even between 10ms ticks.
	cur := uint64(time.Since(tm.epoch) / (10 * time.Millisecond))
	for {
		prev := tm.nowTicks.Load()
		if cur <= prev {
			return prev
		}
		if tm.nowTicks.CompareAndSwap(prev, cur) {
			return cur
		}
	}
}

// HPC returns monotonic nanoseconds since node startup.
func (tm *TimerManager) HPC() int64 {
	return time.Since(tm.epoch).Nanoseconds()
}

// Timeout schedules a PTYPE_RESPONSE wakeup after ti centiseconds (10ms each).
// Matches skynet_timer.c: ti <= 0 fires immediately into the actor mailbox.
func (tm *TimerManager) Timeout(handle uint32, session int32, ti int) {
	if ti <= 0 {
		tm.fire(handle, session)
		return
	}
	expire := tm.Now() + uint64(ti)
	tm.mu.Lock()
	tm.seq++
	heap.Push(&tm.events, timerEvent{
		expire:  expire,
		seq:     tm.seq,
		handle:  handle,
		session: session,
	})
	tm.mu.Unlock()
	select {
	case tm.wake <- struct{}{}:
	default:
	}
}

func (tm *TimerManager) Close() {
	select {
	case <-tm.stop:
	default:
		close(tm.stop)
	}
	<-tm.done
}

func (tm *TimerManager) loop() {
	defer close(tm.done)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	defer timer.Stop()

	for {
		now := tm.Now()
		var ready []timerEvent
		var waitDur time.Duration

		tm.mu.Lock()
		for len(tm.events) > 0 && tm.events[0].expire <= now {
			ready = append(ready, heap.Pop(&tm.events).(timerEvent))
		}
		if len(tm.events) > 0 {
			nextExpire := tm.events[0].expire
			target := tm.epoch.Add(time.Duration(nextExpire) * 10 * time.Millisecond)
			waitDur = time.Until(target)
			if waitDur < time.Millisecond {
				waitDur = time.Millisecond
			}
		} else {
			waitDur = time.Hour
		}
		tm.mu.Unlock()

		for _, ev := range ready {
			tm.fire(ev.handle, ev.session)
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(waitDur)

		select {
		case <-tm.stop:
			return
		case <-tm.wake:
		case <-timer.C:
		}
	}
}
