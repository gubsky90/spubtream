package ch

import "testing"

type Subscription[R comparable] struct {
	receiver R
}

func TestCh(t *testing.T) {
	out := New(func(*Subscription[int]) {})

	in := New(func(sub *Subscription[int]) {
		var tail, cur *Subscription[int]
		var out *Ch[*Subscription[int]]

		out.Send(sub)
	})

	ch.Send(&Subscription[int]{})
}
