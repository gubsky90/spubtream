package way

import (
	"context"
	"errors"
)

var ErrClosed = errors.New("stream closed")

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
}

type ReSub[R comparable] struct {
	receiver R
	add      []string
	remove   []string
}

type Stream[M any, R comparable] struct {
	offset    int
	messages  []M
	used      []int
	index     map[int]IndexItem[R]
	receivers map[R]*Subscription[R]
	stats     Stats
	head      R
	tail      R

	sub          chan Sub[M, R]
	resub        chan ReSub[R]
	unsub        chan R
	pub          chan Pub[M]
	process      chan Task[M, R]
	done         chan Task[M, R]
	requestStats chan chan Stats
}

type Sub[M any, R comparable] struct {
	done     chan error
	pos      Positioner[M]
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

func (stream *Stream[M, R]) Sub(receiver R, pos Positioner[M], tags ...string) error {
	done := make(chan error)
	stream.sub <- Sub[M, R]{
		done:     done,
		pos:      pos,
		tagIDs:   EncodeAll(tags...),
		receiver: receiver,
	}
	return <-done
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

	if sub.tagIDs == nil { // unsubscribed
		//*sub = Subscription[R]{}
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

func (stream *Stream[M, R]) inQ(sub *Subscription[R]) bool {
	return sub.next != Zero[R]()
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
		head = searchPos(pos, head, stream.index[tag].msgIDs)
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
		indexItem := stream.index[tagID]
		indexItem.receivers = append(indexItem.receivers, receiver)
		stream.index[tagID] = indexItem
	}
	return ok
}

func (stream *Stream[M, R]) handleUnSub(receiver R) {
	sub := stream.receivers[receiver]
	//if sub == nil {
	//	return
	//}
	for _, tagID := range sub.tagIDs {
		indexItem := stream.index[tagID]
		indexItem.receivers, _ = deleteItem(indexItem.receivers, receiver)
		stream.index[tagID] = indexItem
	}
	sub.tagIDs = nil
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
		indexItem := stream.index[tagID]
		indexItem.receivers = append(indexItem.receivers, receiver)
		stream.index[tagID] = indexItem
	}
	for _, tag := range remove {
		tagID := Encode(tag)
		if sub.tagIDs, ok = deleteItem(sub.tagIDs, tagID); !ok {
			continue
		}
		indexItem := stream.index[tagID]
		indexItem.receivers, _ = deleteItem(indexItem.receivers, receiver)
		stream.index[tagID] = indexItem
	}
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

func NewStream[M any, R comparable]() *Stream[M, R] {
	stream := Stream[M, R]{
		receivers:    map[R]*Subscription[R]{},
		index:        map[int]IndexItem[R]{},
		sub:          make(chan Sub[M, R]),
		resub:        make(chan ReSub[R]),
		unsub:        make(chan R),
		pub:          make(chan Pub[M]),
		process:      make(chan Task[M, R]),
		done:         make(chan Task[M, R]),
		requestStats: make(chan chan Stats),
	}

	go stream.chanWorker()

	return &stream
}
