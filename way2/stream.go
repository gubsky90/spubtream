package way

import (
	"context"
	"sync/atomic"
)

type Subscription[R comparable] struct {
	tagIDs []int
	offset int
	next   R
}

func (sub *Subscription[R]) unsubscribed() bool {
	return sub.tagIDs == nil
}

func (sub *Subscription[R]) inQ() bool {
	return sub.next != Zero[R]()
}

type Task[M any, R comparable] struct {
	sub      *Subscription[R]
	receiver R
	msg      M
}

type Stream[M any, R comparable] struct {
	offset    int
	messages  []M
	used      []int
	index     *Index[R]
	receivers map[R]*Subscription[R]
	head      R
	tail      R

	stats Stats

	lock   chan struct{}
	unlock chan bool

	pub     chan Pub[M]
	process chan Task[M, R]
	done    chan Task[M, R]
}

func (stream *Stream[M, R]) Pub(ctx context.Context, msg M, tags ...string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case stream.pub <- Pub[M]{msg: msg, tags: tags}:
		return nil
	}
}

func (stream *Stream[M, R]) Sub(receiver R, pos Positioner[M], tags ...string) error {
	var needSelectTask bool
	tagIDs := EncodeAll(tags...)

	stream.lock <- struct{}{}
	defer func() {
		stream.unlock <- needSelectTask
	}()

	offset, err := pos(stream.messages)
	if err != nil {
		return err
	}

	atomic.AddInt64(&stream.stats.Subscriptions, 1)
	needSelectTask = stream.handleSub(receiver, offset, tagIDs)

	return nil
}

func (stream *Stream[M, R]) UnSub(receiver R) {
	stream.lock <- struct{}{}
	defer func() {
		stream.unlock <- false
	}()

	sub := stream.receivers[receiver]
	if sub == nil {
		return
	}

	// delete(stream.receivers, receiver) // <<< do in selectTask
	//if sub == nil {
	//	return
	//}
	for _, tagID := range sub.tagIDs {
		stream.index.deleteReceiver(tagID, receiver)
	}
	sub.tagIDs = nil
}

func (stream *Stream[M, R]) ReSub(receiver R, add, remove []string) {
	stream.lock <- struct{}{}
	stream.handleReSub(receiver, add, remove)
	stream.unlock <- false
}

func (stream *Stream[M, R]) selectTask() (Task[M, R], bool) {
repeat:
	if stream.tail == Zero[R]() {
		return Task[M, R]{}, false
	}

	receiver := stream.tail
	sub := stream.receivers[receiver]
	if sub.next == receiver {
		stream.tail = Zero[R]()
		stream.head = Zero[R]()
		// maybe add nextExists to return?
	} else {
		stream.tail = sub.next
	}

	if sub.unsubscribed() {
		delete(stream.receivers, receiver)
		goto repeat
	}

	msgIDx := sub.offset - stream.offset
	msg := stream.messages[msgIDx]
	stream.used[msgIDx]--

	return Task[M, R]{
		receiver: receiver,
		sub:      sub,
		msg:      msg,
	}, true
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
	if stream.tail == Zero[R]() {
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
		head = searchPos(pos, head, stream.index.getMessageIDs(tag))
	}
	return head, head == streamHead
}

func (stream *Stream[M, R]) handleSub(receiver R, offset int, tagIDs []int) bool {
	sub := &Subscription[R]{
		offset: stream.offset + offset,
		tagIDs: tagIDs,
	}

	stream.receivers[receiver] = sub

	ok := stream.reQ(receiver, sub)
	for _, tagID := range sub.tagIDs {
		stream.index.addReceiver(tagID, receiver)
	}
	return ok
}

func (stream *Stream[M, R]) handleReSub(receiver R, add, remove []string) {
	var ok bool
	sub := stream.receivers[receiver]
	//if sub == nil {
	//	return
	//}
	for _, tag := range add {
		tagID := Encode(tag)
		if sub.tagIDs, ok = addItem(sub.tagIDs, tagID); !ok {
			continue
		}
		stream.index.addReceiver(tagID, receiver)
	}
	for _, tag := range remove {
		tagID := Encode(tag)
		if sub.tagIDs, ok = deleteItem(sub.tagIDs, tagID); !ok {
			continue
		}
		stream.index.deleteReceiver(tagID, receiver)
	}
}

func (stream *Stream[M, R]) handlePub(msg M, tags []string) bool {
	msgID := stream.offset + len(stream.messages)
	stream.messages = append(stream.messages, msg)
	stream.used = append(stream.used, 0)

	var ok bool
	for _, tag := range tags {
		tagID := Encode(tag)

		stream.index.addMessageID(tagID, msgID)

		stream.index.rangeReceivers(tagID, func(receiver R) {
			sub := stream.receivers[receiver]
			if !sub.inQ() {
				sub.offset = msgID
				stream.enQ(receiver, sub)
				ok = true
			}
		})
	}
	return ok
}

func NewStream[M any, R comparable]() *Stream[M, R] {
	stream := Stream[M, R]{
		receivers: map[R]*Subscription[R]{},
		index:     NewIndex[R](),
		pub:       make(chan Pub[M]),
		process:   make(chan Task[M, R]),
		done:      make(chan Task[M, R]),
		lock:      make(chan struct{}),
		unlock:    make(chan bool),
	}

	go stream.chanWorker()

	return &stream
}
