package spubtream

import (
	"slices"
)

func (s *Stream[T]) Pub(msg T) {
	tags := msg.MessageTags()
	if len(tags) == 0 {
		return
	}

	//limit := 10000
	//for {
	//	s.mx.Lock()
	//	if len(s.messages) > limit {
	//		s.mx.Unlock()
	//		time.Sleep(time.Millisecond / 10)
	//		continue
	//	}
	//	break
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
