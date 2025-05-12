package way

import (
	"context"
	"slices"
	"sort"
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

type Stream[T Message] struct {
	state State

	wg sync.WaitGroup

	mx       sync.Mutex
	offset   int
	messages []T
	index    map[int]IndexItem[T]

	head *Subscription[T]
	tail *Subscription[T]

	sub     chan *Subscription[T]
	pub     chan T
	process chan Task[T]
	done    chan Task[T]

	signal chan struct{}
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
	for _, tagID := range sub.tagIDs {
		indexItem := stream.index[tagID]
		indexItem.subs, _ = deleteItem(indexItem.subs, sub)
		stream.index[tagID] = indexItem
	}
	sub.tagIDs = nil
}

func (stream *Stream[T]) ReSub(sub *Subscription[T], add, remove []string) {
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

func (stream *Stream[T]) start() {
	for i := 0; i < WORKERS; i++ {
		go func() {
			for task := range stream.process {
				task.err = task.sub.receiver.Receive(context.TODO(), task.msg)
				stream.done <- task
			}
		}()
	}

	go func() {
		for {
			stream.state = stream.spin()
		}
	}()
}

func (stream *Stream[T]) spin() State {
	switch stream.state {
	case StateIdle:
		return stream.stateIdle()
	case StateProcess:
		return stream.stateProcess()
	default:
		panic("unexpected")
	}
}

func (stream *Stream[T]) stateIdle() State {
	select {
	case sub := <-stream.sub:
		return stream.handleSub(sub)
	case msg := <-stream.pub:
		return stream.handlePub(msg)
	}
}

func (stream *Stream[T]) stateProcess() State {
	select {
	case sub := <-stream.sub:
		_ = stream.handleSub(sub)
		return StateProcess
	case msg := <-stream.pub:
		_ = stream.handlePub(msg)
		return StateProcess

	case task := <-stream.done:
		// handle task.err

		if task.sub.tagIDs == nil { // unsubscribed
			task.sub.receiver = nil
			task.sub.next = nil
			return StateProcess
		}

		stream.reQ(task.sub)
		return StateProcess
	default:
		task, ok := stream.selectTask()
		if !ok {
			return StateIdle
		}
		stream.process <- task
		return StateProcess
	}
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

func (stream *Stream[T]) handleSub(sub *Subscription[T]) State {
	ok := stream.reQ(sub)
	for _, tagID := range sub.tagIDs {
		indexItem := stream.index[tagID]
		indexItem.subs = append(indexItem.subs, sub)
		stream.index[tagID] = indexItem
	}
	if ok {
		return StateProcess
	}
	return StateIdle
}

func (stream *Stream[T]) handlePub(msg T) State {
	msgID := stream.offset + len(stream.messages)
	stream.messages = append(stream.messages, msg)

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
	if ok {
		return StateProcess
	}
	return StateIdle
}

func searchPos(pos, head int, items []int) int {
	n := sort.Search(len(items), func(i int) bool { return items[i] > pos })
	if n < len(items) && items[n] < head {
		return items[n]
	}
	return head
}

func deleteItem[S ~[]E, E comparable](s S, e E) (S, bool) {
	i := slices.Index(s, e)
	if i < 0 {
		return s, false
	}
	s[i] = s[len(s)-1]
	return s[:len(s)-1], true
}

func addItem[S ~[]E, E comparable](s S, e E) (S, bool) {
	if slices.Contains(s, e) {
		return s, false
	}
	return append(s, e), true
}

const WORKERS = 1
const TASKS = 32

func NewStream[T Message]() *Stream[T] {
	stream := Stream[T]{
		state:   StateIdle,
		index:   map[int]IndexItem[T]{},
		sub:     make(chan *Subscription[T]),
		pub:     make(chan T),
		process: make(chan Task[T]),
		done:    make(chan Task[T]),

		signal: make(chan struct{}),
	}
	stream.start()
	return &stream
}
