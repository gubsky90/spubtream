package way

import (
	"sync"
	"sync/atomic"
	"time"
)

type ConsumerFunc[M any, R comparable] func(receiver R, msg M)

func (stream *Stream[M, R]) Start(consumerFunc ConsumerFunc[M, R]) {
	StartDynamicPool(stream.process, 10000, time.Second, func(task Task[M, R]) {
		consumerFunc(task.receiver, task.msg)
		stream.done <- task
	})

	//StartStaticPool(stream.process, 128, func(task Task[M, R]) {
	//	consumerFunc(task.receiver, task.msg)
	//	stream.done <- task
	//})
}

func (stream *Stream[M, R]) Tasks() <-chan Task[M, R] {
	return stream.process
}

func (stream *Stream[M, R]) Close() {

}

func StartStaticPool[T any](in <-chan T, size int, consume func(T)) {
	var wg sync.WaitGroup
	wg.Add(size)
	for i := 0; i < size; i++ {
		go func() {
			defer wg.Done()
			for task := range in {
				consume(task)
			}
		}()
	}
}

func StartDynamicPool[T any](in <-chan T, limit int32, ttl time.Duration, consume func(T)) {
	var wg sync.WaitGroup
	tasks := make(chan T)

	worker := func() {
		defer wg.Done()
		defer atomic.AddInt32(&limit, 1)
		t := time.NewTimer(ttl)
		defer t.Stop()
		for {
			select {
			case task := <-tasks:
				consume(task)
				t.Reset(ttl)
			case <-t.C:
				return
			}
		}
	}

	go func() {
		t := time.NewTimer(time.Millisecond)
		defer t.Stop()
		for task := range in {
			if atomic.AddInt32(&limit, -1) >= 0 {
				t.Reset(time.Millisecond)
				select {
				case tasks <- task:
					atomic.AddInt32(&limit, 1)
				// default:
				case <-t.C:
					wg.Add(1)
					go worker()
					tasks <- task
				}
			} else {
				atomic.AddInt32(&limit, 1)
				tasks <- task
			}
		}
	}()
}
