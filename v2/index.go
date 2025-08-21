package spubtream

type IndexItemNode[R comparable] struct {
	next     *IndexItemNode[R]
	receiver R
}

type IndexItem[R comparable] struct {
	receiverHead *IndexItemNode[R]
	msgIDs       []int
}

type Index[R comparable] struct {
	items map[int]*IndexItem[R]
}

func (index *Index[R]) addMessageID(tagID int, messageID int) {
	item := index.items[tagID]
	if item == nil {
		item = &IndexItem[R]{}
		index.items[tagID] = item
	}
	item.msgIDs = append(item.msgIDs, messageID)
}

func (index *Index[R]) getMessageIDs(tagID int) []int {
	if item := index.items[tagID]; item != nil {
		return item.msgIDs
	}
	return nil
}

func (index *Index[R]) addReceiver(tagID int, receiver R) {
	item := index.items[tagID]
	if item == nil {
		item = &IndexItem[R]{}
		index.items[tagID] = item
	}
	item.receiverHead = &IndexItemNode[R]{
		receiver: receiver,
		next:     item.receiverHead,
	}
}

func (index *Index[R]) deleteReceiver(tagID int, receiver R) {
	// TODO: remove empty index item

	item := index.items[tagID]
	if item == nil {
		return
	}

	cur := item.receiverHead
	if cur != nil && cur.receiver == receiver {
		item.receiverHead = cur.next
		// cur.next = nil
		// cur.receiver = Zero[R]()
		return
	}

	prev := cur
	for cur != nil {
		if cur.receiver == receiver {
			prev.next = cur.next
			// cur.next = nil
			// cur.receiver = Zero[R]()
			return
		}
		prev = cur
		cur = cur.next
	}
}

func (index *Index[R]) rangeReceivers(tagID int, fn func(R)) {
	item := index.items[tagID]
	if item == nil {
		return
	}

	cur := item.receiverHead
	for cur != nil {
		fn(cur.receiver)
		cur = cur.next
	}
}

func (index *Index[R]) rangeItems(fn func(int, *IndexItem[R])) {
	for tagID, item := range index.items {
		fn(tagID, item)

		// TODO: delete empty index item
		//if len(item.msgIDs) == 0 && item.receiverHead == nil {
		//	delete(index.items, tagID)
		//}
	}
}

func NewIndex[R comparable]() *Index[R] {
	return &Index[R]{
		items: map[int]*IndexItem[R]{},
	}
}
