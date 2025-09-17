package v3

import (
	"slices"
	"sort"
)

type Subscription[R comparable] struct {
	offset   int
	tags     []*TagRoot[R]
	next     *Subscription[R]
	receiver R
}

type Stream[M any, R comparable] struct {
	offset        int
	messages      []M
	used          []int
	subscriptions map[R]*Subscription[R]
	tags          map[string]*TagRoot[R]
	ready         chan *Subscription[R]
	out           chan *Subscription[R]
	done          chan *Subscription[R]
}

func (stream *Stream[M, R]) Sub(receiver R, tags ...string) {
	if _, exists := stream.subscriptions[receiver]; exists {
		return
	}

	tags = slices.Compact(tags)
	sub := &Subscription[R]{
		offset:   -1,
		receiver: receiver,
	}
	stream.subscriptions[receiver] = sub
	if len(tags) > 0 {
		sub.tags = make([]*TagRoot[R], len(tags))
		for i, tag := range tags {
			tagRoot := stream.tags[tag]
			if tagRoot == nil {
				tagRoot = &TagRoot[R]{}
				stream.tags[tag] = tagRoot
			}
			tagRoot.AddSubscription(sub)
			sub.tags[i] = tagRoot
		}
	}
}

func (stream *Stream[M, R]) UnSub(receiver R) {
	if sub := stream.subscriptions[receiver]; sub != nil {
		for _, tag := range sub.tags {
			tag.DeleteSubscription(sub)
			if tag.subscriptions == nil {
				// need cleanup
			}
		}
		*sub = Subscription[R]{offset: sub.offset}
		delete(stream.subscriptions, receiver)
	}
}

func (stream *Stream[M, R]) ReSub(receiver R, add, remove []string) {

}

func (stream *Stream[M, R]) Pub(msg M, tags ...string) {
	if len(tags) == 0 {
		return
	}

	tags = slices.Compact(tags)
	msgIDx := len(stream.messages)
	msgID := stream.offset + msgIDx
	used := 0
	hasReceiver := false

	for _, tag := range tags {
		tagRoot := stream.tags[tag]
		if tagRoot == nil || tagRoot.subscriptions == nil {
			continue
		}
		hasReceiver = true
		tagRoot.msgIDs = append(tagRoot.msgIDs, msgID)
		for cur := tagRoot.subscriptions; cur != nil; cur = cur.next {
			if cur.subscription.offset == -1 {
				cur.subscription.offset = msgID
				used++
				stream.ready <- cur.subscription
			}
		}
	}
	if hasReceiver {
		stream.messages = append(stream.messages, msg)
		stream.used = append(stream.used, used)
	}
}

func (stream *Stream[M, R]) Get(sub *Subscription[R]) (R, M) {
	return sub.receiver, stream.messages[sub.offset-stream.offset]
}

func (sub *Subscription[R]) Next() bool {
	next := -1
	for _, tagRoot := range sub.tags {
		msgID, ok := tagRoot.NextMessage(sub.offset)
		if ok && (next == -1 || msgID < next) {
			next = msgID
		}
	}
	sub.offset = next
	return next != -1
}

func (stream *Stream[M, R]) Done(sub *Subscription[R]) {
	stream.done <- sub
}

func (stream *Stream[M, R]) Start(fn func(R, M)) {
	go func() {
		for sub := range stream.out {
			receiver, message := stream.Get(sub)
			fn(receiver, message)
			stream.Done(sub)
		}
	}()
}

func (stream *Stream[M, R]) cleanup() {
	if len(stream.messages) == 0 {
		return
	}
	var drop int
	for _, count := range stream.used {
		if count > 0 {
			break
		}
		drop++
	}
	if drop == 0 {
		return
	}

	dropOffset := stream.offset + drop
	for tag, root := range stream.tags {
		if root.subscriptions == nil {
			delete(stream.tags, tag)
		} else if len(root.msgIDs) > 0 && root.msgIDs[0] <= dropOffset {
			i := sort.SearchInts(root.msgIDs, dropOffset)
			root.msgIDs = root.msgIDs[:copy(root.msgIDs, root.msgIDs[i:])]
		}
	}

	n := copy(stream.messages, stream.messages[drop:])
	copy(stream.used, stream.used[drop:])
	clear(stream.messages[n:])
	stream.messages = stream.messages[:n]
	stream.used = stream.used[:n]
	stream.offset += drop
}

func (stream *Stream[M, R]) loop() {
	var head, tail, cur *Subscription[R]
	var out chan *Subscription[R]

	enq := func(sub *Subscription[R]) {
		if cur == nil {
			cur = sub
			out = stream.out
			return
		}

		// add to list
		if tail == nil {
			tail = sub
		} else {
			head.next = sub
		}
		head = sub
	}

	for {
		select {
		case sub := <-stream.done:
			stream.used[sub.offset-stream.offset]--
			if sub.tags == nil {
				continue // unsubscribed
			}
			if sub.Next() {
				stream.used[sub.offset-stream.offset]++
				enq(sub)
			}
		case out <- cur:
			cur = tail
			if cur == nil {
				out = nil
			} else {
				tail = cur.next
				cur.next = nil
				if tail == nil {
					head = nil
				}
			}
		case sub := <-stream.ready:
			enq(sub)
		}
	}
}

func NewStream[M any, R comparable]() *Stream[M, R] {
	stream := &Stream[M, R]{
		offset:        1000,
		subscriptions: map[R]*Subscription[R]{},
		tags:          map[string]*TagRoot[R]{},
		ready:         make(chan *Subscription[R]),
		out:           make(chan *Subscription[R]),
		done:          make(chan *Subscription[R]),
	}
	go stream.loop()
	return stream
}
