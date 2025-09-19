package spubtream

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type Subscription[R comparable] struct {
	offset   int64
	tags     []*TagRoot[R]
	next     *Subscription[R]
	receiver R
}

type Stream[M any, R comparable] struct {
	in  chan *Subscription[R]
	out chan *Subscription[R]

	messages *Messages[M]

	mx            sync.Mutex
	tags          map[string]*TagRoot[R]
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

	sub.tags = make([]*TagRoot[R], len(tags))
	for i, tag := range tags {
		root := stream.tags[tag]
		if root == nil {
			root = &TagRoot[R]{}
			root.AddSubscription(sub)
			stream.tags[tag] = root
		} else {
			mx := stream.rootMutex(root)
			mx.Lock()
			root.AddSubscription(sub)
			mx.Unlock()
		}
		sub.tags[i] = root
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

	for _, tag := range sub.tags {
		mx := stream.rootMutex(tag)
		mx.Lock()
		tag.DeleteSubscription(sub)
		if tag.subscriptions == nil {
			// need cleanup
		}
		mx.Unlock()
	}
	*sub = Subscription[R]{offset: sub.offset}
}

func (stream *Stream[M, R]) ReSub(receiver R, add, remove []string) {
	stream.mx.Lock()
	defer stream.mx.Unlock()
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
		root := stream.tags[tag]
		if root == nil {
			continue
		}
		mx := stream.rootMutex(root)
		mx.Lock()
		if root.subscriptions == nil {
			mx.Unlock()
			continue
		}
		ok = true
		root.msgIDs = append(root.msgIDs, msgID)
		for cur := root.subscriptions; cur != nil; cur = cur.next {
			if atomic.CompareAndSwapInt64(&cur.subscription.offset, -1, msgID) {
				used++
				stream.in <- cur.subscription
			}
		}
		mx.Unlock()
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
	var next int64 = -1
	offset := atomic.LoadInt64(&sub.offset)
	for _, root := range sub.tags {
		mx := stream.rootMutex(root)
		mx.RLock()
		msgID, ok := root.NextMessage(offset)
		mx.RUnlock()
		if ok && (next == -1 || msgID < next) {
			next = msgID
		}
	}
	if atomic.CompareAndSwapInt64(&sub.offset, offset, next) && next != -1 {
		stream.messages.Used(sub.offset, +1)
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

	for tag, root := range stream.tags {
		mx := stream.rootMutex(root)
		mx.Lock()
		if root.subscriptions == nil {
			delete(stream.tags, tag)
		} else if len(root.msgIDs) > 0 && root.msgIDs[0] <= dropOffset {
			i, _ := slices.BinarySearch(root.msgIDs, dropOffset) // TODO: check if dropOffset not found
			root.msgIDs = root.msgIDs[:copy(root.msgIDs, root.msgIDs[i:])]
		}
		mx.Unlock()
	}

	stream.messages.Drop(drop)
}

var locks = [64]sync.RWMutex{}

func (stream *Stream[M, R]) rootMutex(root *TagRoot[R]) *sync.RWMutex {
	idx := uintptr(unsafe.Pointer(root)) / 99 % 64
	return &locks[idx]
}

func NewStream[M any, R comparable]() *Stream[M, R] {

	stream := &Stream[M, R]{
		in:            make(chan *Subscription[R]),
		out:           make(chan *Subscription[R]),
		subscriptions: map[R]*Subscription[R]{},
		tags:          map[string]*TagRoot[R]{},
		messages: &Messages[M]{
			offset: 1000,
		},
	}

	go loop[R](stream.in, stream.out)
	go loop[R](stream.in, stream.out)

	go func() {
		for {
			time.Sleep(time.Second)
			stream.cleanup()
		}
	}()

	return stream
}
