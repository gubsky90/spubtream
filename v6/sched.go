package spubtream

import (
	"sync"
	"sync/atomic"
)

//func loop2[R comparable](qout func(*Subscription[R])) func(first, last *Subscription[R]) {
//	var size int32 = 2
//	sched := &Sched[R]{
//		size: size,
//		ins:  make([]*QP[R], size),
//	}
//	for i := 0; i < int(size); i++ {
//		sched.ins[i] = &QP[R]{
//			cond: sync.NewCond(&sync.Mutex{}),
//			fn:   qout,
//		}
//		sched.ins[i].Start(64)
//	}
//
//	return sched.Put
//}

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
	tail, cur *Subscription[R]
	cond      *sync.Cond
	fn        func(*Subscription[R])
}

func (qp *QP[R]) send() {
	var sub *Subscription[R]
	qp.cond.L.Lock()
	for qp.cur == nil {
		qp.cond.Wait()
	}
	sub, qp.cur, qp.cur.next = qp.cur, qp.cur.next, nil
	if qp.cur != nil {
		qp.cond.Signal()
	}
	qp.cond.L.Unlock()
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
	qp.cond.L.Lock()
	if qp.cur == nil {
		qp.cur = first
		qp.cond.Signal()
	} else {
		qp.tail.next = first
	}
	qp.tail = last
	qp.cond.L.Unlock()
}
