package spubtream

import (
	"runtime"
	"sync"
	"sync/atomic"
)

func loop2[R comparable](qout func(*Subscription[R])) func(first, last *Subscription[R]) {
	var size int32 = 8
	ins := make([]func(first, last *Subscription[R]), size)
	for i := 0; i < int(size); i++ {
		ins[i] = _loop2(512, qout)
	}

	var c atomic.Int32

	return func(first, last *Subscription[R]) {
		ins[c.Add(1)%size](first, last)
	}
}

func _loop2[R comparable](size int, qout func(*Subscription[R])) func(first, last *Subscription[R]) {
	var tail, cur *Subscription[R]

	cond := sync.NewCond(&Spinlock{})

	for i := 0; i < size; i++ {
		go func() {
			for {
				var sub *Subscription[R]
				cond.L.Lock()
				for cur == nil {
					cond.Wait()
				}
				sub, cur, cur.next = cur, cur.next, nil
				if cur != nil {
					cond.Signal()
				}
				cond.L.Unlock()

				qout(sub)
			}
		}()
	}

	return func(first, last *Subscription[R]) {
		cond.L.Lock()
		if cur == nil {
			cur = first
			cond.Signal()

		} else {
			tail.next = first
		}
		tail = last
		cond.L.Unlock()
	}
}

type Spinlock struct {
	state atomic.Uint32
}

func (s *Spinlock) Lock() {
	for !s.state.CompareAndSwap(0, 1) {
		runtime.Gosched()
	}
}

func (s *Spinlock) Unlock() {
	s.state.Store(0)
}
