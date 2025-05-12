package spubtream

import "time"

func (s *Stream[T]) worker2() {
	defer s.workersWG.Done()

	var sub *Subscription[T]

	t := time.NewTimer(0)
	defer t.Stop()

	s.mx.Lock()
	for {
		if s.readyq.Empty() {
			s.mx.Unlock()

			t.Reset(time.Second)
			select {
			case <-t.C:
				s.mx.Lock()
				s.workers--
				s.mx.Unlock()
			case sub = <-s.readyCh:
				s.mx.Lock()
			}
		} else {
			sub = s.readyq.Deq()
		}

		if len(sub.tags) == 0 { // unsubscribed
			continue
		}

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

			//if sub.pos-s.offset < 0 {
			//	panic("laggard 2")
			//}

			s.enqSub(sub)
		}
	}
}
