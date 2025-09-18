package spubtream

type Queue[R comparable] struct {
	in  chan *Subscription[R]
	out chan *Subscription[R]
}

func (q *Queue[R]) Enq(sub *Subscription[R]) {
	q.in <- sub
}

func (q *Queue[R]) loop() {
	var head, tail, cur *Subscription[R]
	var out chan *Subscription[R]

	for {
		select {
		case out <- cur:
			cur = tail
			if cur == nil {
				out = nil
			} else {
				tail = cur.next
				cur.next = nil
				if tail == nil {
					head = nil
				}
			}
		case sub := <-q.in:
			if cur == nil {
				cur = sub
				out = q.out
			} else {
				// add to list
				if tail == nil {
					tail = sub
				} else {
					head.next = sub
				}
				head = sub
			}
		}
	}
}

func NewQueue[R comparable]() *Queue[R] {
	q := &Queue[R]{
		in:  make(chan *Subscription[R]),
		out: make(chan *Subscription[R]),
	}
	go q.loop()
	return q
}
