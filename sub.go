package spubtream

import (
	"context"
	"errors"
	"slices"
	"sort"
)

const (
	Unknown = 0
	Idle    = 1
	Ready   = 2
)

type Receiver[T Message] interface {
	Receive(ctx context.Context, message T) error
}

type Subscription[T Message] struct {
	tags     []int
	receiver Receiver[T]
	pos      int
	status   uint8
}

type Position struct {
	pos int
}

type ReceiverFunc[T Message] func(ctx context.Context, message T) error

func (fn ReceiverFunc[T]) Receive(ctx context.Context, message T) error {
	return fn(ctx, message)
}

func (s *Stream[T]) SubFunc(receiver ReceiverFunc[T], pos Position, tags ...string) *Subscription[T] {
	return s.Sub(receiver, pos, tags...)
}

func (s *Stream[T]) Sub(receiver Receiver[T], pos Position, tags ...string) *Subscription[T] {
	sub := &Subscription[T]{
		tags:     EncodeAll(tags...),
		receiver: receiver,
		pos:      pos.pos,
		status:   Unknown,
	}

	// TODO: check for laggard

	if len(sub.tags) > 0 {
		s.mx.Lock()
		s.enqSub(sub)
		s.mx.Unlock()
	}

	return sub
}

func (s *Stream[T]) ReSub(sub *Subscription[T], add, remove []string) {
	s.mx.Lock()
	defer s.mx.Unlock()

	var ok bool

	for _, tag := range add {
		tagID := Encode(tag)
		if sub.tags, ok = addItem(sub.tags, tagID); !ok {
			continue
		}

		switch sub.status {
		case Idle:
			s.idleSubs.Add(tagID, sub)
			// s.idleSubs[tagID] = append(s.idleSubs[tagID], sub)
		case Ready:

		default:
			panic("unexpected")
		}
	}

	for _, tag := range remove {
		tagID := Encode(tag)
		if sub.tags, ok = deleteItem(sub.tags, tagID); !ok {
			continue
		}

		switch sub.status {
		case Idle:
			s.idleSubs.Del(tagID, sub)
			// s.idleSubs[tagID], _ = deleteItem(s.idleSubs[tagID], sub)
		case Ready:

		default:
			panic("unexpected")
		}
	}
}

func (s *Stream[T]) UnSub(sub *Subscription[T]) {
	s.mx.Lock()
	defer s.mx.Unlock()
	for _, tagID := range sub.tags {
		s.idleSubs.Del(tagID, sub)
		//s.idleSubs[tagID] = slices.DeleteFunc(s.idleSubs[tagID], func(el *Subscription[T]) bool {
		//	return el == sub
		//})
	}
	sub.tags = nil
}

func (s *Stream[T]) Newest() Position {
	s.mx.Lock()
	defer s.mx.Unlock()
	return Position{
		pos: s.offset + (len(s.messages) - 1),
	}
}

func (s *Stream[T]) Oldest() Position {
	s.mx.Lock()
	defer s.mx.Unlock()
	return Position{
		pos: s.offset,
	}
}

func (s *Stream[T]) After(cmp func(T) int) (Position, error) {
	// maybe validate message tags?
	n := sort.Search(len(s.messages), func(i int) bool {
		return cmp(s.messages[i]) >= 0
	})
	if n == len(s.messages) || cmp(s.messages[n]) != 0 {
		return Position{}, errors.New("not found")
	}
	return Position{
		pos: n + s.offset,
	}, nil
}

func deleteItem[S ~[]E, E comparable](s S, e E) (S, bool) {
	i := slices.Index(s, e)
	if i < 0 {
		return s, false
	}
	s[i] = s[len(s)-1]
	return s[:len(s)-1], true
}

func addItem[S ~[]E, E comparable](s S, e E) (S, bool) {
	if slices.Contains(s, e) {
		return s, false
	}
	return append(s, e), true
}
