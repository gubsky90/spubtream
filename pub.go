package spubtream

import (
	"slices"
	"time"
)

func (s *Stream[T]) Pub(msg T) {
	tags := msg.MessageTags()
	if len(tags) == 0 {
		return
	}

	// TODO: add to config
	limit := 10000
	for {
		s.mx.Lock()
		if len(s.messages) > limit {
			s.mx.Unlock()
			time.Sleep(time.Millisecond / 10)
			continue
		}
		break
	}
	defer s.mx.Unlock()

	msgID := s.offset + len(s.messages)
	s.messages = append(s.messages, msg)

	for _, tag := range tags {
		s.tags[tag] = append(s.tags[tag], msgID)

		shard := s.shard(tag)
		idle := s.idleSubs[shard]
		var j int
		for _, sub := range idle {
			if sub.status != Idle {
				// panic("unexpected")
				// while subscribe we can have two same sub in one shard (tag collision)
				continue
			}
			// recheck for collision
			// TODO: optimize this
			if !slices.Contains(sub.tags, tag) {
				idle[j] = sub
				j++
				continue
			}
			sub.pos = msgID
			s.enqReady(sub)
		}
		s.idleSubs[shard] = idle[:j]
	}
}
