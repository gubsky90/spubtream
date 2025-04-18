package spubtream

type Q[T any] struct {
	offset int
	items  []T
}

func (q *Q[T]) Enq(item T) {
	if q.offset > 0 && len(q.items) == cap(q.items) {
		n := copy(q.items, q.items[q.offset:])
		q.items = q.items[:n]
		q.offset = 0
	}
	q.items = append(q.items, item)
}

func (q *Q[T]) Deq() T {
	item := q.items[q.offset]
	q.offset++
	clear(q.items[q.offset-1 : q.offset])
	return item
}

func (q *Q[T]) Empty() bool {
	return len(q.items) == q.offset
}

func (q *Q[T]) Len() int {
	return len(q.items) - q.offset
}

func (q *Q[T]) Scan(fn func(T)) {
	for _, item := range q.items[q.offset:] {
		fn(item)
	}
}

//func (q *Q[T]) Empty() bool {
//	return len(q.items) == 0
//}
//
//func (q *Q[T]) Len() int {
//	return len(q.items)
//}
//
//func (q *Q[T]) Enq(item T) {
//	if len(q.items) == cap(q.items) {
//		fmt.Println("gc")
//		runtime.GC()
//	}
//	q.items = append(q.items, item)
//}
//
//func (q *Q[T]) Deq() T {
//	item := q.items[0]
//	q.items = q.items[1:]
//	return item
//}
//
//func (q *Q[T]) Scan(fn func(T)) {
//	for _, item := range q.items {
//		fn(item)
//	}
//}
