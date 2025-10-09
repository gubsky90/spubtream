package spubtream

import (
	"runtime"
	"sync"
	"sync/atomic"
)

type QP[R comparable] struct {
	sync.Cond
	stop      atomic.Bool
	wg        sync.WaitGroup
	tail, cur *Subscription[R]
	fn        func(*Subscription[R])
}

func (qp *QP[R]) send() {
	qp.L.Lock()
	for qp.cur == nil {
		qp.Wait()
		if qp.stop.Load() {
			qp.L.Unlock()
			return
		}
	}
	sub := qp.cur
	qp.cur = sub.next
	qp.L.Unlock()

	if sub.next != nil {
		qp.Signal()
		sub.next = nil
	}

	qp.fn(sub)
}

func (qp *QP[R]) Start(size int) {
	qp.wg.Add(size)
	for i := 0; i < size; i++ {
		go func() {
			defer qp.wg.Done()
			for !qp.stop.Load() {
				qp.send()
			}
		}()
	}
}

func (qp *QP[R]) Stop() {
	qp.stop.Store(true)
	qp.Broadcast()
	qp.wg.Wait()
}

func (qp *QP[R]) Put(first, last *Subscription[R]) {
	var sig bool
	qp.L.Lock()
	if qp.cur == nil {
		qp.cur = first
		sig = true
	} else {
		qp.tail.next = first
	}
	qp.tail = last
	qp.L.Unlock()
	if sig {
		qp.Signal()
	}
}

type Spinlock struct {
	state int32
}

func (s *Spinlock) Lock() {
	for !atomic.CompareAndSwapInt32(&s.state, 0, 1) {
		runtime.Gosched()
	}
}

func (s *Spinlock) Unlock() {
	atomic.StoreInt32(&s.state, 0)
}
