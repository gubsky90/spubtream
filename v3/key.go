package spubtream

import (
	"sort"
	"sync"
)

type Key[R comparable] struct {
	sync.Mutex
	head   *KeySub[R]
	msgIDs []int
}

type KeySub[R comparable] struct {
	next *KeySub[R]
	sub  *Subscription[R]
}

func (key *Key[R]) NextMessage(offset int) (_ int, _ bool) {
	l := len(key.msgIDs)
	n := sort.Search(l, func(i int) bool {
		return key.msgIDs[i] > offset
	})
	if n < l {
		return key.msgIDs[n], true
	}
	return
}

func (key *Key[R]) AddSubscription(sub *Subscription[R]) {
	key.head = &KeySub[R]{
		next: key.head,
		sub:  sub,
	}
}

func (key *Key[R]) DeleteSubscription(sub *Subscription[R]) {
	cur := key.head
	parent := &key.head
	for cur != nil {
		if cur.sub == sub {
			*parent = cur.next
			return
		}
		parent = &cur.next
		cur = cur.next
	}
}
