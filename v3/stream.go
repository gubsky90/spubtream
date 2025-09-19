package spubtream

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type Subscription[R comparable] struct {
	sync.Mutex
	offset   int
	keys     []*Key[R]
	next     *Subscription[R]
	receiver R
}

type Stream[M any, R comparable] struct {
	in  chan *Subscription[R]
	out chan *Subscription[R]

	messages *Messages[M]

	mx            sync.Mutex
	tags          map[string]*Key[R]
	subscriptions map[R]*Subscription[R]

	stats Stats
}

func (stream *Stream[M, R]) Sub(receiver R, tags ...string) {
	tags = slices.Compact(tags)

	stream.mx.Lock()
	defer stream.mx.Unlock()
	if _, exists := stream.subscriptions[receiver]; exists {
		return
	}

	atomic.AddInt64(&stream.stats.Subscriptions, 1)

	sub := &Subscription[R]{
		offset:   -1,
		receiver: receiver,
	}
	stream.subscriptions[receiver] = sub
	if len(tags) == 0 {
		return
	}

	sub.keys = make([]*Key[R], len(tags))
	for i, tag := range tags {
		sub.keys[i] = stream.subTag(sub, tag)
	}
}

func (stream *Stream[M, R]) UnSub(receiver R) {
	stream.mx.Lock()
	defer stream.mx.Unlock()

	sub := stream.subscriptions[receiver]
	if sub == nil {
		return
	}

	atomic.AddInt64(&stream.stats.Subscriptions, -1)

	delete(stream.subscriptions, receiver)

	for _, key := range sub.keys {
		key.Lock()
		key.DeleteSubscription(sub)
		key.Unlock()
	}
	*sub = Subscription[R]{offset: sub.offset}
}

func (stream *Stream[M, R]) subTag(sub *Subscription[R], tag string) *Key[R] {
	key := stream.tags[tag]
	if key == nil {
		key = &Key[R]{}
		key.AddSubscription(sub)
		stream.tags[tag] = key
	} else {
		key.Lock()
		key.AddSubscription(sub)
		key.Unlock()
	}
	return key
}

func (stream *Stream[M, R]) ReSub(receiver R, add, remove []string) {
	add = slices.Compact(add)
	remove = slices.Compact(remove)

	stream.mx.Lock()
	defer stream.mx.Unlock()

	sub := stream.subscriptions[receiver]
	if sub == nil {
		return
	}

	for _, tag := range remove {
		key := stream.tags[tag]
		if key == nil {
			continue
		}
		idx := slices.Index(sub.keys, key)
		if idx >= 0 {
			sub.keys = slices.Delete(sub.keys, idx, idx+1)
			key.Lock()
			key.DeleteSubscription(sub)
			key.Unlock()
		}
	}

	for _, tag := range add {
		key := stream.subTag(sub, tag)
		sub.keys = append(sub.keys, key)
	}
}

func (stream *Stream[M, R]) Pub(msg M, tags ...string) {
	if len(tags) == 0 {
		return
	}
	tags = slices.Compact(tags)

	stream.mx.Lock()
	defer stream.mx.Unlock()

	var used int
	var ok bool
	msgID := stream.messages.LockForAdd()
	for _, tag := range tags {
		key := stream.tags[tag]
		if key == nil {
			continue
		}
		key.Lock()
		if key.head == nil {
			key.Unlock()
			continue
		}
		ok = true
		key.msgIDs = append(key.msgIDs, msgID)
		for cur := key.head; cur != nil; cur = cur.next {
			cur.sub.Lock()
			if cur.sub.offset == -1 {
				cur.sub.offset = msgID
				used++
				stream.in <- cur.sub
			}
			cur.sub.Unlock()
		}
		key.Unlock()
	}

	if ok {
		atomic.StoreInt64(&stream.stats.Messages, int64(len(stream.messages.messages)))
		atomic.AddInt64(&stream.stats.Published, 1)
		stream.messages.AddAndUnlock(msg, used)
	} else {
		stream.messages.Unlock()
	}
}

func (stream *Stream[M, R]) Done(sub *Subscription[R]) {
	var next = -1

	sub.Lock()
	offset := sub.offset
	for _, key := range sub.keys {
		key.Lock()
		msgID, ok := key.NextMessage(offset)
		key.Unlock()
		if ok && (next == -1 || msgID < next) {
			next = msgID
		}
	}
	sub.offset = next
	sub.Unlock()

	if next != -1 {
		stream.messages.Used(next, +1)
		stream.in <- sub
	}
	stream.messages.Used(offset, -1)
}

func (stream *Stream[M, R]) Start(fn func(R, M)) {
	for i := 0; i < 10; i++ {
		go func() {
			for sub := range stream.out {
				atomic.AddInt64(&stream.stats.Received, 1)
				fn(sub.receiver, stream.messages.Get(sub.offset))
				stream.Done(sub)
			}
		}()
	}
}

func (stream *Stream[M, R]) cleanup() {
	drop, dropOffset := stream.messages.GetDrop()
	if drop < 5000 {
		return
	}

	fmt.Println("cleanup", drop, dropOffset)

	stream.mx.Lock()
	for tag, key := range stream.tags {
		key.Lock()
		if key.head == nil {
			delete(stream.tags, tag)
		} else if len(key.msgIDs) > 0 && key.msgIDs[0] <= dropOffset {
			i, _ := slices.BinarySearch(key.msgIDs, dropOffset) // TODO: check if dropOffset not found
			key.msgIDs = key.msgIDs[:copy(key.msgIDs, key.msgIDs[i:])]
		}
		key.Unlock()
	}
	stream.mx.Unlock()

	stream.messages.Drop(drop)
}

func NewStream[M any, R comparable]() *Stream[M, R] {
	stream := &Stream[M, R]{
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

	go func() {
		for {
			time.Sleep(time.Second)
			stream.cleanup()
		}
	}()

	return stream
}
