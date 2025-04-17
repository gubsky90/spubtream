package spubtream

import (
	"context"
	"log/slog"
	"slices"
	"sort"
	"sync"
)

type Message interface {
	MessageTags() []string
}

type Stream[T Message] struct {
	ctx      context.Context
	mx       sync.Mutex
	commonWG sync.WaitGroup

	workersWG       sync.WaitGroup
	workers         int
	workersLimit    int
	waitForLaggards bool

	offset   int
	messages []T
	tags     map[string][]int
	subTags  [512][]*Subscription[T]
	readyq   []*Subscription[T]
}

func (s *Stream[T]) WaitWorkers() {
	s.workersWG.Wait()
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
			s.workersWG.Done()
			return
		}

		sub := s.readyq[0]
		s.readyq = s.readyq[1:]
		if len(sub.tags) == 0 { // unsubscribed
			continue
		}

		message := s.messages[sub.pos-s.offset]
		s.mx.Unlock()

		// TODO: handle panic
		err := sub.receiver.Receive(s.ctx, message)
		if err != nil {
			s.handleReceiverError(sub, err)
		}

		s.mx.Lock()
		if err != nil {
			s.deleteSub(sub)
		} else {
			s.enqSub(sub)
		}
	}
}

func (s *Stream[T]) handleReceiverError(sub *Subscription[T], err error) {
	//if fn, ok := sub.receiver.(interface{Some()}); ok {
	//	go fn.Some()
	//}

	slog.Warn("subscriber failed", "receiver", sub.receiver, "tags", sub.tags, "err", err)
}

func (s *Stream[T]) deleteSub(sub *Subscription[T]) {
	for _, tag := range sub.tags {
		shard := s.shard(tag)
		tagSubs := s.subTags[shard]
		n := slices.Index(tagSubs, sub)
		tagSubs[n] = tagSubs[len(tagSubs)-1]
		tagSubs = tagSubs[:len(tagSubs)-1]
		s.subTags[shard] = tagSubs
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
		s.workersWG.Add(1)
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

	usage := 0
	if s.waitForLaggards {
		lastSubN := s.offset + len(s.messages)
		for _, st := range s.subTags {
			for _, sub := range st {
				if sub.status != Idle && sub.pos < lastSubN {
					lastSubN = sub.pos
				}
			}
		}
		usage = lastSubN - s.offset
	}

	n := min(len(s.messages), min(usage, fn(s.messages)))
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

	//slog.Info("[GC]",
	//	"messages", len(s.messages),
	//	"cap", cap(s.messages),
	//	"offset", s.offset,
	//	"n", n,
	//	"usage", usage,
	//	"waitForLaggards", s.waitForLaggards,
	//)
}

func simpleHash(str string) (sum uint32) {
	for i := 0; i < len(str); i++ {
		sum ^= uint32(str[i])
		sum *= 0x01000193
	}
	return
}

func searchPos(pos, head int, items []int) int {
	n := sort.Search(len(items), func(i int) bool { return items[i] > pos })
	if n < len(items) && items[n] < head {
		return items[n]
	}
	return head
}
