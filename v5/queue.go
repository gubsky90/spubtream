package spubtream

func loop[R comparable](qin chan [2]*Subscription[R], qout chan *Subscription[R]) {
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
				cur = sub[0]
				out = qout
			} else {
				tail.next = sub[0]
			}
			tail = sub[1]
		}
	}
}
