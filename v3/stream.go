package v3

import (
	"fmt"
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
	head          *Subscription[R]
	tail          *Subscription[R]
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
				stream.enqReceiver(cur.subscription)
			}
		}
	}
	if hasReceiver {
		stream.messages = append(stream.messages, msg)
		stream.used = append(stream.used, used)
	}
}

func (stream *Stream[M, R]) Read() {
	if stream.tail == nil {
		return
	}

	sub := stream.tail
	if sub.next == nil {
		stream.tail = nil
		stream.head = nil
	} else {
		stream.tail = sub.next
		sub.next = nil
	}

	// process sub

	fmt.Println(sub.receiver, sub, stream.messages[sub.offset-stream.offset])
	stream.onDone(sub)
}

func (stream *Stream[M, R]) Start(fn func(R, M)) {

}

func (stream *Stream[M, R]) Stop() {

}

func (stream *Stream[M, R]) onDone(sub *Subscription[R]) {
	next := -1
	for _, tagRoot := range sub.tags {
		msgID, ok := tagRoot.NextMessage(sub.offset)
		if ok && (next == -1 || msgID < next) {
			next = msgID
		}
	}

	stream.used[sub.offset-stream.offset]--
	sub.offset = next
	if next != -1 {
		stream.used[sub.offset-stream.offset]++
		stream.enqReceiver(sub)
	}
}

func (stream *Stream[M, R]) enqReceiver(sub *Subscription[R]) {
	if stream.tail == nil {
		stream.tail = sub
	} else {
		stream.head.next = sub
	}
	stream.head = sub
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
	for tag, tagRoot := range stream.tags {
		if tagRoot.subscriptions == nil {
			delete(stream.tags, tag)
			continue
		}
		if len(tagRoot.msgIDs) == 0 {
			continue
		}
		if tagRoot.msgIDs[0] > dropOffset {
			continue
		}
		i := sort.SearchInts(tagRoot.msgIDs, dropOffset)
		tagRoot.msgIDs = tagRoot.msgIDs[:copy(tagRoot.msgIDs, tagRoot.msgIDs[i:])]
	}

	n := copy(stream.messages, stream.messages[drop:])
	copy(stream.used, stream.used[drop:])
	clear(stream.messages[n:])
	stream.messages = stream.messages[:n]
	stream.used = stream.used[:n]
	stream.offset += drop
}

func NewStream[M any, R comparable]() *Stream[M, R] {
	return &Stream[M, R]{
		offset:        1000,
		subscriptions: map[R]*Subscription[R]{},
		tags:          map[string]*TagRoot[R]{},
	}
}
