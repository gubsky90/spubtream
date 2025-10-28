package spubtream

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type Stream[R comparable, M any] struct {
	tmpKeys       []*Key[R]
	tags          map[string]*Key[R]
	messages      *Store[*Key[R], M]
	in            *QP[R]
	subscriptions map[R]*Subscription[R]
	stats         Stats
}

type Subscription[R comparable] struct {
	offset   atomic.Int64
	keys     []*Key[R]
	next     *Subscription[R]
	receiver R
}

// Sub to tags
func (stream *Stream[R, M]) Sub(receiver R, tags ...string) {
	sub := stream.subscriptions[receiver]
	if sub == nil {
		sub = &Subscription[R]{
			keys:     make([]*Key[R], 0, len(tags)),
			receiver: receiver,
		}
		stream.subscriptions[receiver] = sub
		sub.offset.Store(-1)
	}
	for _, tag := range tags {
		key := stream.tags[tag]
		if key == nil {
			key = &Key[R]{}
			stream.tags[tag] = key
		} else if !slices.Contains(sub.keys, key) {
			sub.keys = append(sub.keys, key)
			key.AddSubscription(sub)
		}
	}
}

// UnSub from tags; if tags is nil, unsubscribe from all tags
func (stream *Stream[R, M]) UnSub(receiver R, tags ...string) {

}

func (stream *Stream[R, M]) Pub(msg M, tags ...string) {
	atomic.AddInt64(&stream.stats.Published, 1)
	defer func() {
		stream.tmpKeys = stream.tmpKeys[:0]
	}()
	for _, tag := range tags {
		if key := stream.tags[tag]; key != nil {
			stream.tmpKeys = append(stream.tmpKeys, key)
		}
	}

	var first, last *Subscription[R]
	id, size := stream.messages.Append(msg, stream.tmpKeys)
	atomic.StoreInt64(&stream.stats.Messages, int64(size))

	var used int32
	for _, key := range stream.tmpKeys {
		key.Range(func(sub *Subscription[R]) {
			if sub.offset.CompareAndSwap(-1, id) {
				used++
				if first == nil {
					first = sub
				}
				if last != nil {
					last.next = sub
				}
				last = sub
			}
		})
	}
	stream.messages.Used(id, used)

	if first != nil {
		stream.in.Put(first, last)
	}
}

func (stream *Stream[R, M]) Start(fn func(R, M)) {
	stream.in = &QP[R]{
		Cond: sync.Cond{L: &Spinlock{}},
		// Cond: sync.Cond{L: &sync.Mutex{}},
		fn: func(sub *Subscription[R]) {
			start := time.Now()
			current := sub.offset.Load()
			for {
				atomic.AddInt64(&stream.stats.Received, 1)
				fn(sub.receiver, stream.messages.Get(current))
				current = stream.messages.Next(current, sub.keys)
				if current == -1 || time.Since(start) > time.Second {
					break
				}
			}
			sub.offset.Store(current)
			if current != -1 {
				stream.in.Put(sub, sub)
			}
		},
	}
	stream.in.Start(1024)
}

func NewStream[R comparable, M any]() *Stream[R, M] {
	stream := &Stream[R, M]{
		tags:          map[string]*Key[R]{},
		subscriptions: map[R]*Subscription[R]{},
		messages:      NewStore[*Key[R], M](),
	}
	return stream
}
