package spubtream

import (
	"runtime"
	"sync"
)

func loop[R comparable](qout chan *Subscription[R]) func([2]*Subscription[R]) {
	var tail, cur *Subscription[R]

	var mx, wait sync.Mutex
	wait.Lock()

	go func() {
		for {
			mx.Lock()
			if cur == nil {
				mx.Unlock()
				wait.Lock()
				mx.Lock()
			}

			sub := cur
			cur, cur.next = cur.next, nil
			mx.Unlock()
			qout <- sub
		}
	}()

	return func(sub [2]*Subscription[R]) {
		mx.Lock()
		if cur == nil {
			cur = sub[0]
			wait.TryLock()
			wait.Unlock()
		} else {
			tail.next = sub[0]
		}
		tail = sub[1]
		mx.Unlock()
	}
}

func loop2[R comparable](qout func(*Subscription[R])) func([2]*Subscription[R]) {
	var tail, cur *Subscription[R]

	cond := sync.NewCond(&sync.Mutex{})

	for i := 0; i < 64; i++ {
		go func() {
			var sub *Subscription[R]
			for {
				cond.L.Lock()
				for cur == nil {
					cond.Wait()
				}
				sub = cur
				cur, cur.next = cur.next, nil
				cond.L.Unlock()
				qout(sub)
			}
		}()
	}

	return func(sub [2]*Subscription[R]) {
		if sub[0] != sub[1] {
			runtime.Gosched()
		}

		cond.L.Lock()
		if cur == nil {
			cur = sub[0]
			cond.Signal()
		} else {
			tail.next = sub[0]
		}
		tail = sub[1]
		cond.L.Unlock()
	}
}
