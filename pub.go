package spubtream

import (
	"context"
	"errors"
	"time"
)

var ErrNoTags = errors.New("no tags")

func (s *Stream[T]) Pub(ctx context.Context, msg T) error {
	messageTags := msg.MessageTags()
	if len(messageTags) == 0 {
		return ErrNoTags
	}

	if s.bufferSizeLimit > 0 {
		for {
			s.mx.Lock()
			if len(s.messages) < s.bufferSizeLimit {
				break
			}
			s.mx.Unlock()

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Millisecond):
				continue
			}
		}
	} else {
		s.mx.Lock()
	}
	defer s.mx.Unlock()

	msgID := s.offset + len(s.messages)
	s.messages = append(s.messages, msg)

	for _, messageTag := range messageTags {
		tag := Encode(messageTag)
		s.tags[tag] = append(s.tags[tag], msgID)

		s.idleSubs.Scan(tag, func(sub *Subscription[T]) {
			if sub.status != Idle {
				return
			}
			sub.pos = msgID
			s.enqReady(sub)
		})

		//s.idleSubs.Extract(tag, func(sub *Subscription[T]) {
		//	if sub.status != Idle {
		//		sub.readyTags = append(sub.readyTags, tag)
		//		return
		//	}
		//	sub.pos = msgID
		//	sub.readyTags = append(sub.readyTags, tag)
		//	s.enqReady(sub)
		//})

		//for _, sub := range s.idleSubs[tag] {
		//	if sub.status != Idle {
		//		sub.readyTags = append(sub.readyTags, tag)
		//		continue
		//	}
		//	sub.pos = msgID
		//	sub.readyTags = append(sub.readyTags, tag)
		//	s.enqReady(sub)
		//}
		//s.idleSubs[tag] = s.idleSubs[tag][:0]
	}

	return nil
}
