package spubtream

//func loop[R comparable](qin, qout chan *Subscription[R]) {
//	var head, tail, cur *Subscription[R]
//	var out chan *Subscription[R]
//
//	for {
//		select {
//		case out <- cur:
//			cur = tail
//			if cur == nil {
//				out = nil
//			} else {
//				tail = cur.next
//				cur.next = nil
//				if tail == nil {
//					head = nil
//				}
//			}
//		case sub := <-qin:
//			if cur == nil {
//				cur = sub
//				out = qout
//			} else {
//				// add to list
//				if tail == nil {
//					tail = sub
//				} else {
//					head.next = sub
//				}
//				head = sub
//			}
//		}
//	}
//}

func loop[R comparable](qin, qout chan *Subscription[R]) {
	var tail, cur *Subscription[R]
	var out chan *Subscription[R]

	for {
		select {
		case out <- cur:
			cur, cur.next = cur.next, nil
			if cur == nil {
				out = nil
			}
		case sub := <-qin:
			if cur == nil {
				cur = sub
				out = qout
			} else {
				tail.next = sub
			}
			tail = sub
		}
	}
}
