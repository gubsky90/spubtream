package spubtream

type Key[R comparable] struct {
	head *KeySub[R]
}

type KeySub[R comparable] struct {
	next *KeySub[R]
	sub  *Subscription[R]
}

func (key *Key[R]) Range(fn func(*Subscription[R])) {
	for cur := key.head; cur != nil; cur = cur.next {
		fn(cur.sub)
	}
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

//type Key[R comparable] struct {
//	subs []*Subscription[R]
//}
//
//func (key *Key[R]) Range(fn func(*Subscription[R])) {
//	for _, sub := range key.subs {
//		fn(sub)
//	}
//}
//
//func (key *Key[R]) AddSubscription(sub *Subscription[R]) {
//	key.subs = append(key.subs, sub)
//}
