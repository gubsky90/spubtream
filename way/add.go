package way

import (
	"fmt"
	"slices"
	"sort"
)

func (sub *Subscription[T]) String() string {
	var next string
	if sub.next == nil {
		next = "<nil>"
	} else {
		next = fmt.Sprint(sub.next.receiver)
	}
	return fmt.Sprintf("%v (offset: %d; next: %s)", sub.receiver, sub.offset, next)
}

func searchPos(pos, head int, items []int) int {
	n := sort.Search(len(items), func(i int) bool { return items[i] > pos })
	if n < len(items) && items[n] < head {
		return items[n]
	}
	return head
}

func deleteItem[S ~[]E, E comparable](s S, e E) (S, bool) {
	i := slices.Index(s, e)
	if i < 0 {
		return s, false
	}
	s[i] = s[len(s)-1]
	return s[:len(s)-1], true
}

func addItem[S ~[]E, E comparable](s S, e E) (S, bool) {
	if slices.Contains(s, e) {
		return s, false
	}
	return append(s, e), true
}
