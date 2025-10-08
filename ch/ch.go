package ch

import "sync"

type Ch[T any] struct {
	mx sync.Mutex
	fn func(T)
}

func (ch *Ch[T]) Send(v T) {
	ch.mx.Lock()
	ch.fn(v)
	ch.mx.Unlock()
}

func (ch *Ch[T]) Receive() (T, bool) {

}

func New[T any](fn func(T)) *Ch[T] {
	return &Ch[T]{
		fn: fn,
	}
}
