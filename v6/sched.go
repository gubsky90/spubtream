package spubtream

import (
	"runtime"
	"sync"
	"sync/atomic"
)

func NewSched[R comparable](qout func(*Subscription[R])) *Sched[R] {
	var size int32 = 32
	sched := &Sched[R]{
		size: size,
		ins:  make([]*QP[R], size),
	}
	for i := 0; i < int(size); i++ {
		sched.ins[i] = &QP[R]{
			Cond: sync.Cond{L: &Spinlock{}},
			// Cond: sync.Cond{L: &sync.Mutex{}},
			fn: qout,
		}
		sched.ins[i].Start(1024 / int(size))
	}
	return sched
}

type Sched[R comparable] struct {
	c    atomic.Int32
	size int32
	ins  []*QP[R]
}

func (s *Sched[R]) Put(first, last *Subscription[R]) {
	// s.ins[rand.Intn(2)].Put(first, last)
	s.ins[s.c.Add(1)%s.size].Put(first, last)
}

type QP[R comparable] struct {
	sync.Cond
	tail, cur *Subscription[R]
	fn        func(*Subscription[R])
}

func (qp *QP[R]) send() {
	var sub *Subscription[R]
	qp.L.Lock()
	for qp.cur == nil {
		qp.Wait()
	}
	sub, qp.cur = qp.cur, qp.cur.next
	qp.L.Unlock()

	if sub.next != nil {
		qp.Signal()
		sub.next = nil
	}

	qp.fn(sub)
}

func (qp *QP[R]) Start(size int) {
	for i := 0; i < size; i++ {
		go func() {
			for {
				qp.send()
			}
		}()
	}
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
