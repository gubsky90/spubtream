package spubtream

import (
	"sort"
)

type TagRoot[R comparable] struct {
	msgIDs        []int64
	subscriptions *TagReceiver[R]
}

type TagReceiver[R comparable] struct {
	next         *TagReceiver[R]
	subscription *Subscription[R]
}

func (root *TagRoot[R]) NextMessage(offset int64) (_ int64, _ bool) {
	l := len(root.msgIDs)
	n := sort.Search(l, func(i int) bool {
		return root.msgIDs[i] > offset
	})
	if n < l {
		return root.msgIDs[n], true
	}
	return
}

func (root *TagRoot[R]) AddSubscription(subscription *Subscription[R]) {
	root.subscriptions = &TagReceiver[R]{
		next:         root.subscriptions,
		subscription: subscription,
	}
}

func (root *TagRoot[R]) DeleteSubscription(subscription *Subscription[R]) {
	cur := root.subscriptions
	parent := &root.subscriptions
	for cur != nil {
		if cur.subscription == subscription {
			*parent = cur.next
			return
		}
		parent = &cur.next
		cur = cur.next
	}
}
