package spubtream

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
)

type Message interface {
	MessageTags() []string
}

type Stream[T Message] struct {
	ctx       context.Context
	mx        sync.Mutex
	commonWG  sync.WaitGroup
	workersWG sync.WaitGroup

	workersLimit    int
	bufferSizeLimit int
	waitForLaggards bool

	workers  int
	offset   int
	messages []T
	tags     map[int][]int
	idleSubs map[int][]*Subscription[T]

	// readyq    []*Subscription[T]
	readyq Q[*Subscription[T]]

	inProcess map[*Subscription[T]]struct{}
}

func (s *Stream[T]) WaitWorkers() {
	s.workersWG.Wait()
}

func (s *Stream[T]) worker() {
	s.mx.Lock()
	for {
		if s.readyq.Empty() {
			s.workers--
			s.mx.Unlock()
			s.workersWG.Done()
			return
		}

		sub := s.readyq.Deq()

		if len(sub.tags) == 0 { // unsubscribed
			continue
		}

		//if sub.pos-s.offset < 0 {
		//	handle laggard
		//}

		s.inProcess[sub] = struct{}{}
		message := s.messages[sub.pos-s.offset]
		s.mx.Unlock()

		// TODO: handle panic
		err := sub.receiver.Receive(s.ctx, message)
		if err != nil {
			s.handleReceiverError(sub, err)
		}

		s.mx.Lock()
		delete(s.inProcess, sub)
		if err == nil {
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

func (s *Stream[T]) enqReady(sub *Subscription[T]) {
	sub.status = Ready
	s.readyq.Enq(sub)
	if s.workers < s.workersLimit {
		s.workers++
		s.workersWG.Add(1)
		fmt.Println("spawn")
		go s.worker()
	}
}

func (s *Stream[T]) enqSub(sub *Subscription[T]) {
	pos, end := s.nextPos(sub.tags, sub.pos)
	if end {
		s.idle(sub)
	} else {
		sub.pos = pos
		s.ready(sub)
	}
}

func (s *Stream[T]) idle(sub *Subscription[T]) {
	if sub.status == Idle {
		panic("unexpected")
	}
	for _, tagID := range sub.readyTags {
		s.idleSubs[tagID] = append(s.idleSubs[tagID], sub)
	}
	sub.readyTags = sub.readyTags[:0]
	sub.status = Idle
}

func (s *Stream[T]) ready(sub *Subscription[T]) {
	if sub.status == Idle {
		panic("unexpected")
	}
	if sub.status == Unknown {
		s.enqReady(sub)
	} else {
		s.readyq.Enq(sub)
	}
}

func (s *Stream[T]) nextPos(tags []int, pos int) (int, bool) {
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
		s.readyq.Scan(func(sub *Subscription[T]) {
			if sub.pos < lastSubN {
				lastSubN = sub.pos
			}
		})
		for sub := range s.inProcess {
			if sub.pos < lastSubN {
				lastSubN = sub.pos
			}
		}
		usage = lastSubN - s.offset
	}

	n := min(len(s.messages), min(usage, fn(s.messages)))

	// TODO: optimize
	for i, msg := range s.messages[:n] {
		msgID := s.offset + i
		for _, tag := range EncodeAll(msg.MessageTags()...) {
			s.tags[tag] = slices.DeleteFunc(s.tags[tag], func(i int) bool {
				return i == msgID
			})
		}
	}

	s.messages = append(s.messages[:0], s.messages[n:]...)
	s.offset += n

	slog.Info("[GC]",
		"messages", len(s.messages),
		"cap", cap(s.messages),
		"offset", s.offset,
		"n", n,
		"usage", usage,
		"waitForLaggards", s.waitForLaggards,

		"inProcess", len(s.inProcess),
		"readyq", s.readyq.Len(),
		"tags", infoMapSlice(s.tags),
		"idleSubs", infoMapSlice(s.idleSubs),
	)
}

func infoMapSlice[K comparable, E any](m map[K][]E) string {
	var keys, elms int
	for _, v := range m {
		keys++
		elms += len(v)
	}
	return fmt.Sprintf("[%d,%d]", keys, elms)
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
