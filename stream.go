package spubtream

import (
	"log/slog"
	"slices"
	"sync"
)

const (
	Idle    = 0
	Ready   = 1
	Process = 2
)

type Message interface {
	MessageTags() []string
}

type Receiver[T Message] interface {
	Receive(T) error
}

type Stream[T Message] struct {
	mx           sync.Mutex
	wg           sync.WaitGroup
	workers      int
	workersLimit int
	offset       int
	messages     []T
	tags         map[string][]int
	subTags      [512][]*Subscription[T]
	readyq       []*Subscription[T]
}

func (s *Stream[T]) Pub(msg T) {
	tags := msg.MessageTags()
	//if len(tags) == 0 {
	//	return
	//}

	s.mx.Lock()
	defer s.mx.Unlock()

	msgID := s.offset + len(s.messages)
	s.messages = append(s.messages, msg)

	for _, tag := range tags {
		s.tags[tag] = append(s.tags[tag], msgID)

		shard := s.shard(tag)
		for _, sub := range s.subTags[shard] {
			if sub.status != Idle {
				continue
			}
			if !slices.Contains(sub.tags, tag) {
				continue
			}
			sub.pos = msgID
			s.ready(sub)
		}
	}
}

func (s *Stream[T]) shard(tag string) int {
	return int(simpleHash(tag)) % len(s.subTags)
}

func (s *Stream[T]) worker() {
	s.mx.Lock()
	for {
		if len(s.readyq) == 0 {
			s.workers--
			s.mx.Unlock()
			s.wg.Done()
			return
		}

		sub := s.readyq[0]
		s.readyq = s.readyq[1:]
		sub.status = Process
		message := s.messages[sub.pos-s.offset]
		s.mx.Unlock()

		err := sub.receiver.Receive(message)

		s.mx.Lock()
		if err != nil {
			slog.Warn("subscriber failed", "err", err)

			//for _, tag := range sub.tags {
			//	shard := s.shard(tag)
			//	tagSubs := s.subTags[shard]
			//	n := slices.Index(tagSubs, sub)
			//	tagSubs[n] = tagSubs[len(tagSubs)-1]
			//	tagSubs = tagSubs[:len(tagSubs)-1]
			//	s.subTags[shard] = tagSubs
			//}
		} else {
			s.enqSub(sub)
		}
	}
}

func (s *Stream[T]) enqSub(sub *Subscription[T]) {
	pos, end := s.nextPos(sub.tags, sub.pos)
	sub.pos = pos
	if end {
		sub.status = Idle
	} else {
		s.ready(sub)
	}
}

func (s *Stream[T]) ready(sub *Subscription[T]) {
	sub.status = Ready
	s.readyq = append(s.readyq, sub)

	if s.workers < s.workersLimit {
		s.workers++
		s.wg.Add(1)
		go s.worker()
	}
}

func (s *Stream[T]) nextPos(tags []string, pos int) (int, bool) {
	streamHead := s.offset + len(s.messages)
	head := streamHead
	for _, tag := range tags {
		head = searchPos(pos, head, s.tags[tag])
	}
	return head, head == streamHead
}

func (s *Stream[T]) gc(fn func(messages []T) int) {
	s.mx.Lock()
	defer s.mx.Unlock()

	if len(s.messages) == 0 {
		return
	}

	lastSubN := s.offset + len(s.messages)
	for _, st := range s.subTags {
		for _, sub := range st {
			if sub.status != Idle && sub.pos < lastSubN {
				lastSubN = sub.pos
			}
		}
	}

	n := min(len(s.messages), max(0, fn(s.messages)))
	for i, msg := range s.messages[:n] {
		msgID := s.offset + i
		for _, tag := range msg.MessageTags() {
			s.tags[tag] = slices.DeleteFunc(s.tags[tag], func(i int) bool {
				return i == msgID
			})
		}
	}

	s.messages = append(s.messages[:0], s.messages[n:]...)
	s.offset += n
}

func NewStream[T Message]() *Stream[T] {
	return &Stream[T]{
		tags:         map[string][]int{},
		workersLimit: 128,
	}
}
