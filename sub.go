package spubtream

import (
	"context"
	"errors"
	"slices"
	"sort"
)

type Receiver[T Message] interface {
	Receive(ctx context.Context, message T) error
}

type Subscription[T Message] struct {
	tags     []string
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

func (s *Stream[T]) Sub(receiver Receiver[T], pos Position, tags []string) *Subscription[T] {
	sub := &Subscription[T]{
		tags:     tags,
		receiver: receiver,
		pos:      pos.pos,
		status:   Idle,
	}

	if len(tags) > 0 {
		s.mx.Lock()
		defer s.mx.Unlock()
		for _, tag := range tags {
			shard := s.shard(tag)
			s.subTags[shard] = append(s.subTags[shard], sub)
		}
		s.enqSub(sub)
	}

	return sub
}

func (s *Stream[T]) ReSub(sub *Subscription[T], add, remove []string) {
	s.mx.Lock()
	defer s.mx.Unlock()

	for _, tag := range add {
		sub.tags = append(sub.tags, tag)
		shard := s.shard(tag)
		s.subTags[shard] = append(s.subTags[shard], sub)
	}

	for _, tag := range remove {
		sub.tags = slices.DeleteFunc(sub.tags, func(item string) bool {
			return item == tag
		})

		shard := s.shard(tag)
		s.subTags[shard] = slices.DeleteFunc(s.subTags[shard], func(item *Subscription[T]) bool {
			return item == sub
		})
	}

	// s.enqSub(sub) // ??? maybe only if new tag add
}

func (s *Stream[T]) Newest() Position {
	s.mx.Lock()
	defer s.mx.Unlock()
	return Position{
		pos: s.offset + len(s.messages),
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
