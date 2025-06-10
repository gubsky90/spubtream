package way

import (
	"context"
	"sync"
)

type Subscription[R comparable] struct {
	next   R
	offset int
	tagIDs []int
}

type IndexItem[R comparable] struct {
	receivers []R
	msgIDs    []int
}

type Task[M any, R comparable] struct {
	sub      *Subscription[R]
	receiver R
	msg      M
	err      error
}

type ReSub[R comparable] struct {
	receiver R
	add      []string
	remove   []string
}

type Stream[M any, R comparable] struct {
	wg sync.WaitGroup

	receivers map[R]*Subscription[R]

	offset   int
	messages []M
	used     []int
	index    map[int]IndexItem[R]

	head R
	tail R

	sub     chan Sub[R]
	resub   chan ReSub[R]
	unsub   chan R
	pub     chan Pub[M]
	process chan Task[M, R]
	done    chan Task[M, R]

	requestStats chan chan Stats
	stats        Stats
}

type Sub[R comparable] struct {
	offset   int
	tagIDs   []int
	receiver R
}

type Pub[M any] struct {
	msg  M
	tags []string
}

func (stream *Stream[M, R]) Pub(ctx context.Context, msg M, tags ...string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case stream.pub <- Pub[M]{msg: msg, tags: tags}:
		return nil
	}
}

func (stream *Stream[M, R]) Sub(receiver R, offset int, tags ...string) {
	stream.sub <- Sub[R]{
		offset:   offset,
		tagIDs:   EncodeAll(tags...),
		receiver: receiver,
	}
}

func (stream *Stream[M, R]) UnSub(receiver R) {
	stream.unsub <- receiver
}

func (stream *Stream[M, R]) ReSub(receiver R, add, remove []string) {
	stream.resub <- ReSub[R]{
		receiver: receiver,
		add:      add,
		remove:   remove,
	}
}

func (stream *Stream[M, R]) WaitWorkers() {
	stream.wg.Wait()
}

func (stream *Stream[M, R]) selectTask() (Task[M, R], bool) {
repeat:
	if IsZero(stream.tail) {
		return Task[M, R]{}, false
	}

	receiver := stream.tail
	sub := stream.receivers[receiver]

	msgIDx := sub.offset - stream.offset
	msg := stream.messages[msgIDx]

	if sub.next == receiver {
		stream.tail = Zero[R]()
		stream.head = Zero[R]()
		// maybe add nextExists to return?
	} else {
		stream.tail = sub.next
	}

	if sub.tagIDs == nil { // unsubscribed
		//*sub = Subscription[R]{}
		goto repeat
	}

	return Task[M, R]{
		receiver: receiver,
		sub:      sub,
		msg:      msg,
	}, true
}

func (stream *Stream[M, R]) inQ(sub *Subscription[R]) bool {
	return !IsZero(sub.next)
}

func (stream *Stream[M, R]) reQ(receiver R, sub *Subscription[R]) bool {
	if pos, end := stream.nextPos(sub.tagIDs, sub.offset); !end {
		sub.offset = pos
		stream.enQ(receiver, sub)
		return true
	}
	sub.next = Zero[R]()
	return false
}

func (stream *Stream[M, R]) enQ(receiver R, sub *Subscription[R]) {
	stream.used[sub.offset-stream.offset]++
	if IsZero(stream.tail) {
		stream.tail = receiver
		stream.head = receiver
	}
	sub.next = receiver
	stream.receivers[stream.head].next = receiver
	stream.head = receiver
}

func (stream *Stream[M, R]) nextPos(tags []int, pos int) (int, bool) {
	streamHead := stream.offset + len(stream.messages)
	head := streamHead
	for _, tag := range tags {
		head = searchPos(pos, head, stream.index[tag].msgIDs)
	}
	return head, head == streamHead
}

func (stream *Stream[M, R]) handleSub(s Sub[R]) bool {
	sub := &Subscription[R]{
		offset: s.offset,
		tagIDs: s.tagIDs,
	}

	stream.receivers[s.receiver] = sub

	ok := stream.reQ(s.receiver, sub)
	for _, tagID := range sub.tagIDs {
		indexItem := stream.index[tagID]
		indexItem.receivers = append(indexItem.receivers, s.receiver)
		stream.index[tagID] = indexItem
	}
	return ok
}

func (stream *Stream[M, R]) subByReceiver(receiver R) *Subscription[R] {
	return stream.receivers[receiver]
}

func (stream *Stream[M, R]) handleUnSub(receiver R) {
	//sub := stream.subByReceiver(receiver)
	//if sub == nil {
	//	return
	//}
	//for _, tagID := range sub.tagIDs {
	//	indexItem := stream.index[tagID]
	//	indexItem.subs, _ = deleteItem(indexItem.subs, sub)
	//	stream.index[tagID] = indexItem
	//}
	//sub.tagIDs = nil
}

func (stream *Stream[M, R]) handleReSub(receiver R, add, remove []string) {
	//var ok bool
	//for _, tag := range add {
	//	tagID := Encode(tag)
	//	if sub.tagIDs, ok = addItem(sub.tagIDs, tagID); !ok {
	//		continues
	//	}
	//	indexItem := stream.index[tagID]
	//	indexItem.subs = append(indexItem.subs, sub)
	//	stream.index[tagID] = indexItem
	//}
	//for _, tag := range remove {
	//	tagID := Encode(tag)
	//	if sub.tagIDs, ok = deleteItem(sub.tagIDs, tagID); !ok {
	//		continue
	//	}
	//	indexItem := stream.index[tagID]
	//	indexItem.subs, _ = deleteItem(indexItem.subs, sub)
	//	stream.index[tagID] = indexItem
	//}
}

func (stream *Stream[M, R]) handlePub(msg M, tags []string) bool {
	msgID := stream.offset + len(stream.messages)
	stream.messages = append(stream.messages, msg)
	stream.used = append(stream.used, 0)

	var ok bool
	for _, tag := range tags {
		tagID := Encode(tag)

		indexItem := stream.index[tagID]
		indexItem.msgIDs = append(indexItem.msgIDs, msgID)
		stream.index[tagID] = indexItem

		for _, receiver := range indexItem.receivers {
			sub := stream.receivers[receiver]
			if !stream.inQ(sub) {
				sub.offset = msgID
				stream.enQ(receiver, sub)
				ok = true
			}
		}
	}
	return ok
}

const WORKERS = 128

type ConsumerFunc[M any, R comparable] func(ctx context.Context, receiver R, msg M) error

func NewStream[M any, R comparable](consumerFunc ConsumerFunc[M, R]) *Stream[M, R] {
	stream := Stream[M, R]{
		receivers: map[R]*Subscription[R]{},

		index:        map[int]IndexItem[R]{},
		sub:          make(chan Sub[R]),
		resub:        make(chan ReSub[R]),
		unsub:        make(chan R),
		pub:          make(chan Pub[M]),
		process:      make(chan Task[M, R]),
		done:         make(chan Task[M, R]),
		requestStats: make(chan chan Stats),
	}

	for i := 0; i < WORKERS; i++ {
		go func() {
			for task := range stream.process {
				task.err = consumerFunc(context.TODO(), task.receiver, task.msg)
				stream.done <- task
			}
		}()
	}

	go stream.chanWorker()

	return &stream
}
