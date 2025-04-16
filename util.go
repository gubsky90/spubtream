package spubtream

import "sort"

func simpleHash(str string) (sum uint32) {
	for i := 0; i < len(str); i++ {
		sum ^= uint32(str[i])
		sum *= 0x01000193
	}
	return
}

func searchPos(pos, head int, items []int) int {
	n := sort.Search(len(items), func(i int) bool { return items[i] > pos })
	if n < len(items) && items[n] < head {
		return items[n]
	}
	return head
}

type ReceiverFunc[T Message] func(T) error

func (fn ReceiverFunc[T]) Receive(msg T) error {
	return fn(msg)
}
