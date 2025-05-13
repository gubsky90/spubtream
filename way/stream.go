package way

import (
	"context"
	"sync"
)

type Message interface {
	MessageTags() []string
}

type Receiver[T Message] interface {
	Receive(ctx context.Context, message T) error
}

type Subscription[T Message] struct {
	next     *Subscription[T]
	offset   int
	receiver Receiver[T]
	tagIDs   []int
}

type IndexItem[T Message] struct {
	subs   []*Subscription[T]
	msgIDs []int
}

type Task[T Message] struct {
	sub *Subscription[T]
	msg T
	err error
}

type ReSub[T Message] struct {
	sub    *Subscription[T]
	add    []string
	remove []string
}

type Stream[T Message] struct {
	wg sync.WaitGroup

	offset   int
	messages []T
	used     []int
	index    map[int]IndexItem[T]

	head *Subscription[T]
	tail *Subscription[T]

	sub     chan *Subscription[T]
	resub   chan ReSub[T]
	unsub   chan *Subscription[T]
	pub     chan T
	process chan Task[T]
	done    chan Task[T]

	requestStats chan chan Stats
	stats        Stats
}

func (stream *Stream[T]) Pub(ctx context.Context, msg T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case stream.pub <- msg:
		return nil
	}
}

func (stream *Stream[T]) Sub(receiver Receiver[T], offset int, tags ...string) *Subscription[T] {
	sub := &Subscription[T]{
		offset:   offset,
		receiver: receiver,
		tagIDs:   EncodeAll(tags...),
	}
	stream.sub <- sub
	return sub
}

func (stream *Stream[T]) UnSub(sub *Subscription[T]) {
	stream.unsub <- sub
}

func (stream *Stream[T]) ReSub(sub *Subscription[T], add, remove []string) {
	stream.resub <- ReSub[T]{
		sub:    sub,
		add:    add,
		remove: remove,
	}
}

func (stream *Stream[T]) WaitWorkers() {
	stream.wg.Wait()
}

func (stream *Stream[T]) selectTask() (Task[T], bool) {
repeat:
	sub := stream.tail
	if sub == nil {
		return Task[T]{}, false
	}

	msgIDx := sub.offset - stream.offset
	msg := stream.messages[msgIDx]

	if sub.next == sub {
		stream.tail = nil
		stream.head = nil
		// maybe add nextExists to return?
	} else {
		stream.tail = sub.next
	}

	if sub.tagIDs == nil { // unsubscribed
		sub.receiver = nil
		sub.next = nil
		goto repeat
	}

	return Task[T]{
		sub: sub,
		msg: msg,
	}, true
}

func (stream *Stream[T]) inQ(sub *Subscription[T]) bool {
	return sub.next != nil
}

func (stream *Stream[T]) reQ(sub *Subscription[T]) bool {
	if pos, end := stream.nextPos(sub.tagIDs, sub.offset); !end {
		sub.offset = pos
		stream.enQ(sub)
		return true
	}
	sub.next = nil
	return false
}

func (stream *Stream[T]) enQ(sub *Subscription[T]) {
	stream.used[sub.offset-stream.offset]++

	if stream.tail == nil {
		stream.tail = sub
		stream.head = sub
	}
	sub.next = sub
	stream.head.next = sub
	stream.head = sub
}

func (stream *Stream[T]) nextPos(tags []int, pos int) (int, bool) {
	streamHead := stream.offset + len(stream.messages)
	head := streamHead
	for _, tag := range tags {
		head = searchPos(pos, head, stream.index[tag].msgIDs)
	}
	return head, head == streamHead
}

func (stream *Stream[T]) handleSub(sub *Subscription[T]) bool {
	ok := stream.reQ(sub)
	for _, tagID := range sub.tagIDs {
		indexItem := stream.index[tagID]
		indexItem.subs = append(indexItem.subs, sub)
		stream.index[tagID] = indexItem
	}
	return ok
}

func (stream *Stream[T]) handleUnSub(sub *Subscription[T]) {
	for _, tagID := range sub.tagIDs {
		indexItem := stream.index[tagID]
		indexItem.subs, _ = deleteItem(indexItem.subs, sub)
		stream.index[tagID] = indexItem
	}
	sub.tagIDs = nil
}

func (stream *Stream[T]) handleReSub(sub *Subscription[T], add, remove []string) {
	var ok bool
	for _, tag := range add {
		tagID := Encode(tag)
		if sub.tagIDs, ok = addItem(sub.tagIDs, tagID); !ok {
			continue
		}
		indexItem := stream.index[tagID]
		indexItem.subs = append(indexItem.subs, sub)
		stream.index[tagID] = indexItem
	}
	for _, tag := range remove {
		tagID := Encode(tag)
		if sub.tagIDs, ok = deleteItem(sub.tagIDs, tagID); !ok {
			continue
		}
		indexItem := stream.index[tagID]
		indexItem.subs, _ = deleteItem(indexItem.subs, sub)
		stream.index[tagID] = indexItem
	}
}

func (stream *Stream[T]) handlePub(msg T) bool {
	msgID := stream.offset + len(stream.messages)
	stream.messages = append(stream.messages, msg)
	stream.used = append(stream.used, 0)

	var ok bool
	for _, tag := range msg.MessageTags() {
		tagID := Encode(tag)

		indexItem := stream.index[tagID]
		indexItem.msgIDs = append(indexItem.msgIDs, msgID)
		stream.index[tagID] = indexItem

		for _, sub := range indexItem.subs {
			if !stream.inQ(sub) {
				sub.offset = msgID
				stream.enQ(sub)
				ok = true
			}
		}
	}
	return ok
}

const WORKERS = 128

func NewStream[T Message]() *Stream[T] {
	stream := Stream[T]{
		index:        map[int]IndexItem[T]{},
		sub:          make(chan *Subscription[T]),
		resub:        make(chan ReSub[T]),
		unsub:        make(chan *Subscription[T]),
		pub:          make(chan T),
		process:      make(chan Task[T]),
		done:         make(chan Task[T]),
		requestStats: make(chan chan Stats),
	}

	for i := 0; i < WORKERS; i++ {
		go func() {
			for task := range stream.process {
				task.err = task.sub.receiver.Receive(context.TODO(), task.msg)
				stream.done <- task
			}
		}()
	}

	go stream.chanWorker()

	return &stream
}
