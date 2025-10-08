package spubtream

import (
	"sync/atomic"
)

type Stream[R comparable, M any] struct {
	tmpKeys       []*Key[R]
	tags          map[string]*Key[R]
	messages      *Messages[*Key[R], M]
	in            func(first, last *Subscription[R])
	out           chan *Subscription[R]
	subscriptions map[R]*Subscription[R]
	stats         Stats
}

type Subscription[R comparable] struct {
	// last     int64
	offset   atomic.Int64
	keys     []*Key[R]
	next     *Subscription[R]
	receiver R
}

func (stream *Stream[R, M]) Sub(receiver R, tags ...string) {
	sub := &Subscription[R]{
		keys:     make([]*Key[R], len(tags)),
		receiver: receiver,
	}
	stream.subscriptions[receiver] = sub
	sub.offset.Store(-1)
	for i, tag := range tags {
		key := stream.tags[tag]
		if key == nil {
			key = &Key[R]{}
			stream.tags[tag] = key
		}
		sub.keys[i] = key
		key.AddSubscription(sub)
	}
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

	var used int32
	id, l := stream.messages.Add(msg, 1, stream.tmpKeys)
	atomic.StoreInt64(&stream.stats.Messages, l)
	var first, last *Subscription[R]
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

	if first != nil {
		stream.in(first, last)
	}

	stream.messages.Used(id, used-1)
}

func (stream *Stream[R, M]) Start(fn func(R, M)) {
	//for i := 0; i < 8; i++ {
	//	go stream.worker(fn)
	//}

	stream.in = loop2[R](func(sub *Subscription[R]) {
		atomic.AddInt64(&stream.stats.Received, 1)

		current := sub.offset.Load()
		fn(sub.receiver, stream.messages.Get(current))

		stream.in(sub, sub)

		//if stream.messages.NextMessage(current, &sub.offset, sub.keys) {
		//	stream.in(sub, sub)
		//}
	})
}

func NewStream[R comparable, M any]() *Stream[R, M] {
	stream := &Stream[R, M]{
		out:           make(chan *Subscription[R], 64),
		tags:          map[string]*Key[R]{},
		subscriptions: map[R]*Subscription[R]{},
		messages:      NewMessages[*Key[R], M](),
	}

	// stream.in = loop[R](stream.out)

	return stream
}
