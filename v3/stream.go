package spubtream

import (
	"slices"
	"sync"
	"sync/atomic"
)

type Subscription[R comparable] struct {
	lockSlot uint8
	offset   atomic.Int64
	keys     []*Key[R]
	next     *Subscription[R]
	receiver R
}

type Stream[R comparable, M any] struct {
	subC          Counter
	keyC          Counter
	keyMsgLocks   [256]sync.RWMutex
	subLocks      [256]sync.RWMutex
	in            chan *Subscription[R]
	out           chan *Subscription[R]
	messages      *Messages[M]
	tags          map[string]*Key[R]
	subscriptions map[R]*Subscription[R]

	mx      sync.Mutex
	tmpKeys []*Key[R]

	//keySubLocks     [256]sync.RWMutex
	//pubLock         chan []*Key[R]
	//tagsMx          sync.Mutex
	//subscriptionsMx sync.Mutex

	stats Stats
}

func (stream *Stream[R, M]) Sub(receiver R, tags ...string) {
	tags = slices.Compact(tags)

	sub := &Subscription[R]{
		lockSlot: stream.subC.Next(),
		receiver: receiver,
	}

	stream.mx.Lock()
	defer stream.mx.Unlock()
	if _, exists := stream.subscriptions[receiver]; exists {
		return
	}
	stream.subscriptions[receiver] = sub
	sub.offset.Store(-1)

	if len(tags) > 0 {
		sub.keys = make([]*Key[R], len(tags))
		for i, tag := range tags {
			sub.keys[i] = stream.subTag(sub, tag)
		}
	}

	atomic.AddInt64(&stream.stats.Subscriptions, 1)
}

func (stream *Stream[R, M]) UnSub(receiver R) {
	stream.mx.Lock()
	defer stream.mx.Unlock()

	sub := stream.subscriptions[receiver]
	if sub == nil {
		return
	}
	delete(stream.subscriptions, receiver)

	stream.subLocks[sub.lockSlot].Lock()
	for _, key := range sub.keys {
		key.DeleteSubscription(sub)
	}
	clear(sub.keys)
	sub.keys = nil
	// sub.receiver = zero
	stream.subLocks[sub.lockSlot].Unlock()

	atomic.AddInt64(&stream.stats.Subscriptions, -1)
}

func (stream *Stream[R, M]) ReSub(receiver R, add, remove []string) {
	add = slices.Compact(add)
	remove = slices.Compact(remove)

	stream.mx.Lock()
	defer stream.mx.Unlock()
	sub := stream.subscriptions[receiver]
	if sub == nil {
		return
	}

	stream.subLocks[sub.lockSlot].Lock()
	for _, tag := range remove {
		key := stream.tags[tag]
		if key == nil {
			continue
		}
		idx := slices.Index(sub.keys, key)
		if idx >= 0 {
			sub.keys = slices.Delete(sub.keys, idx, idx+1)
			key.DeleteSubscription(sub)
		}
	}
	for _, tag := range add {
		key := stream.subTag(sub, tag)
		sub.keys = append(sub.keys, key)
	}
	stream.subLocks[sub.lockSlot].Unlock()
}

func (stream *Stream[R, M]) Start(fn func(R, M)) {
	for i := 0; i < 10; i++ {
		go stream.worker(fn)
	}
}

func (stream *Stream[R, M]) worker(fn func(R, M)) {
	for sub := range stream.out {
		atomic.AddInt64(&stream.stats.Received, 1)

		current := sub.offset.Load()

		fn(sub.receiver, stream.messages.Get(current))

		var next int64 = -1
		stream.subLocks[sub.lockSlot].Lock()
		for _, key := range sub.keys {
			stream.keyMsgLocks[key.lockSlot].RLock()
			msgID, ok := key.NextMessage(current)
			stream.keyMsgLocks[key.lockSlot].RUnlock()
			if ok && (next == -1 || msgID < next) {
				next = msgID
			}
		}
		stream.subLocks[sub.lockSlot].Unlock()
		sub.offset.Store(next)

		if next != -1 {
			stream.messages.Used(next, +1)
			stream.in <- sub
		}
		stream.messages.Used(current, -1)
	}
}

func (stream *Stream[R, M]) subTag(sub *Subscription[R], tag string) *Key[R] {
	key := stream.tags[tag]
	if key == nil {
		key = &Key[R]{
			lockSlot: stream.keyC.Next(),
		}
		key.AddSubscription(sub)
		stream.tags[tag] = key
	} else {
		key.AddSubscription(sub)
	}
	return key
}

func NewStream[R comparable, M any]() *Stream[R, M] {
	stream := &Stream[R, M]{
		in:            make(chan *Subscription[R]),
		out:           make(chan *Subscription[R]),
		subscriptions: map[R]*Subscription[R]{},
		tags:          map[string]*Key[R]{},
		messages: &Messages[M]{
			offset: 1000,
		},
	}

	for i := 0; i < 8; i++ {
		go loop[R](stream.in, stream.out)
	}

	return stream
}
