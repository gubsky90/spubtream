package spubtream

import (
	"slices"
)

// TODO: refactor
func (s *Stream[T]) Pub(msg T) {
	messageTags := msg.MessageTags()
	if len(messageTags) == 0 {
		return
	}

	tags := EncodeAll(messageTags...)

	// TODO: add to config
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
		idle := s.idleSubs[shard]
		var j int
		for _, sub := range idle {
			if sub.status != Idle {
				sub.readyShards = append(sub.readyShards, shard)
				// panic("unexpected")
				// while subscribe we can have two same sub in one shard (tag collision)
				// also same subscribe can be in several idleSubs shards (multiple tags)
				continue
			}

			// recheck for collision
			// TODO: optimize this

			contains := existsInOrdered(sub.tags, tag)
			if !contains {
				idle[j] = sub
				j++
				continue
			}

			sub.readyShards = append(sub.readyShards, shard)
			sub.pos = msgID
			s.enqReady(sub)
		}
		s.idleSubs[shard] = idle[:j]
	}
}

func existsInOrdered(s []int, e int) bool {
	_, ok := slices.BinarySearch(s, e)
	return ok
}
