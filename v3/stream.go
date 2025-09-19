package spubtream

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type Subscription[R comparable] struct {
	lockSlot byte
	offset   atomic.Int64
	keys     []*Key[R]
	next     *Subscription[R]
	receiver R
}

type Stream[R comparable, M any] struct {
	keyLocks [256]sync.RWMutex
	subLocks [256]sync.RWMutex

	in  chan *Subscription[R]
	out chan *Subscription[R]

	messages *Messages[M]

	mx            sync.Mutex
	tags          map[string]*Key[R]
	subscriptions map[R]*Subscription[R]

	stats Stats
}

func (stream *Stream[R, M]) Sub(receiver R, tags ...string) {
	tags = slices.Compact(tags)

	sub := &Subscription[R]{
		receiver: receiver,
	}
	sub.offset.Store(-1)

	stream.mx.Lock()
	defer stream.mx.Unlock()
	if _, exists := stream.subscriptions[receiver]; exists {
		return
	}
	atomic.AddInt64(&stream.stats.Subscriptions, 1)
	stream.subscriptions[receiver] = sub
	if len(tags) > 0 {
		sub.keys = make([]*Key[R], len(tags))
		for i, tag := range tags {
			sub.keys[i] = stream.subTag(sub, tag)
		}
	}
}

func (stream *Stream[R, M]) UnSub(receiver R) {
	var zero R

	stream.mx.Lock()
	sub := stream.subscriptions[receiver]
	if sub == nil {
		stream.mx.Unlock()
		return
	}
	delete(stream.subscriptions, receiver)
	stream.mx.Unlock()

	stream.subLocks[sub.lockSlot].Lock()
	for _, key := range sub.keys {
		stream.keyLocks[key.lockSlot].Lock()
		key.DeleteSubscription(sub)
		stream.keyLocks[key.lockSlot].Unlock()
	}
	clear(sub.keys)
	sub.keys = nil
	sub.receiver = zero
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
			stream.keyLocks[key.lockSlot].Lock()
			key.DeleteSubscription(sub)
			stream.keyLocks[key.lockSlot].Unlock()
		}
	}

	for _, tag := range add {
		key := stream.subTag(sub, tag)
		sub.keys = append(sub.keys, key)
	}

	stream.subLocks[sub.lockSlot].Unlock()
}

func (stream *Stream[R, M]) Pub(msg M, tags ...string) {
	if len(tags) == 0 {
		return
	}
	tags = slices.Compact(tags)

	stream.mx.Lock()
	defer stream.mx.Unlock()

	var used int
	var msgID int64 = -1
	for _, tag := range tags {
		key := stream.tags[tag]
		if key == nil {
			continue
		}
		stream.keyLocks[key.lockSlot].Lock()
		if key.head == nil {
			stream.keyLocks[key.lockSlot].Unlock()
			continue
		}
		if msgID == -1 {
			msgID = stream.messages.Add(msg, 0)
		}
		key.msgIDs = append(key.msgIDs, msgID)
		for cur := key.head; cur != nil; cur = cur.next {
			if cur.sub.offset.CompareAndSwap(-1, msgID) {
				used++
				stream.in <- cur.sub
			}
		}
		stream.keyLocks[key.lockSlot].Unlock()
	}

	if msgID != -1 {
		atomic.AddInt64(&stream.stats.Published, 1)
		stream.messages.Used(msgID, used)
	}
}

func (stream *Stream[R, M]) Start(fn func(R, M)) {
	for i := 0; i < 10; i++ {
		go func() {
			for sub := range stream.out {
				atomic.AddInt64(&stream.stats.Received, 1)

				var next int64 = -1
				current := sub.offset.Load()

				fn(sub.receiver, stream.messages.Get(current))

				stream.subLocks[sub.lockSlot].RLock()
				for _, key := range sub.keys {
					next = stream.nextOffset(key, current, next)
				}
				stream.subLocks[sub.lockSlot].RUnlock()
				sub.offset.Store(next)

				if next != -1 {
					stream.messages.Used(next, +1)
					stream.in <- sub
				}
				stream.messages.Used(current, -1)
			}
		}()
	}
}

func (stream *Stream[R, M]) subTag(sub *Subscription[R], tag string) *Key[R] {
	key := stream.tags[tag]
	if key == nil {
		key = &Key[R]{}
		key.AddSubscription(sub)
		stream.tags[tag] = key
	} else {
		stream.keyLocks[key.lockSlot].Lock()
		key.AddSubscription(sub)
		stream.keyLocks[key.lockSlot].Unlock()
	}
	return key
}

func (stream *Stream[R, M]) nextOffset(key *Key[R], current, next int64) int64 {
	stream.keyLocks[key.lockSlot].RLock()
	msgID, ok := key.NextMessage(current)
	stream.keyLocks[key.lockSlot].RUnlock()
	if ok && (next == -1 || msgID < next) {
		next = msgID
	}
	return next
}

func (stream *Stream[R, M]) cleanup() {
	stream.mx.Lock()
	defer stream.mx.Unlock()

	drop, dropOffset := stream.messages.GetDrop()
	if drop < 5000 {
		return
	}

	fmt.Println("cleanup", drop, dropOffset)

	for tag, key := range stream.tags {
		stream.keyLocks[key.lockSlot].Lock()
		if key.head == nil {
			delete(stream.tags, tag)
		} else if len(key.msgIDs) > 0 && key.msgIDs[0] <= dropOffset {
			i, _ := slices.BinarySearch(key.msgIDs, dropOffset) // TODO: check if dropOffset not found
			key.msgIDs = key.msgIDs[:copy(key.msgIDs, key.msgIDs[i:])]
		}
		stream.keyLocks[key.lockSlot].Unlock()
	}

	stream.messages.Drop(drop)
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

	go func() {
		for {
			time.Sleep(time.Second)
			stream.cleanup()
		}
	}()

	return stream
}
