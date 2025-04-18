package spubtream

import (
	"time"
)

func (s *Stream[T]) Pub(msg T) {
	messageTags := msg.MessageTags()
	if len(messageTags) == 0 {
		return
	}

	tags := EncodeAll(messageTags...)

	if s.bufferSizeLimit > 0 {
		for {
			s.mx.Lock()
			if len(s.messages) > s.bufferSizeLimit {
				s.mx.Unlock()
				time.Sleep(time.Millisecond / 10)
				continue
			}
			break
		}
	} else {
		s.mx.Lock()
	}

	defer s.mx.Unlock()

	msgID := s.offset + len(s.messages)
	s.messages = append(s.messages, msg)

	for _, tag := range tags {
		s.tags[tag] = append(s.tags[tag], msgID)
		for _, sub := range s.idleSubs[tag] {
			if sub.status != Idle {
				sub.readyTags = append(sub.readyTags, tag)
				continue
			}
			sub.pos = msgID
			sub.readyTags = append(sub.readyTags, tag)
			s.enqReady(sub)
		}
		s.idleSubs[tag] = s.idleSubs[tag][:0]
	}
}
