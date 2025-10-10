package spubtream

import (
	"sync"
	"sync/atomic"
)

type Store[K comparable, M any] struct {
	mx sync.RWMutex

	offset int64
	used   []int32
	items  []M
	index  *Index[K]
}

func (store *Store[K, M]) Next(id int64, keys []K) int64 {
	store.mx.RLock()
	defer store.mx.RUnlock()

	var next int64 = -1
	if id < store.offset+int64(len(store.items))-1 {
		var i int
		for ; i < len(keys) && next == -1; i++ {
			next = store.index.Next(keys[i], id)
		}
		for ; i < len(keys); i++ {
			next = min(next, store.index.Next(keys[i], id))
		}
		if next != -1 {
			atomic.AddInt32(&store.used[next-store.offset], 1)
		}
	}

	atomic.AddInt32(&store.used[id-store.offset], -1)

	return next
}

func (store *Store[K, M]) Get(id int64) M {
	store.mx.RLock()
	defer store.mx.RUnlock()
	return store.items[id-store.offset]
}

func (store *Store[K, M]) Append(item M, keys []K) (int64, int) {
	id := store.offset + int64(len(store.items))

	if len(store.items) < cap(store.items) {
		store.items = append(store.items, item)
		store.used = append(store.used, 0)
		store.mx.Lock()
	} else {
		store.mx.Lock()
		store.cleanup()
		store.items = append(store.items, item)
		store.used = append(store.used, 0)
	}
	for _, key := range keys {
		store.index.Append(key, id)
	}
	store.mx.Unlock()

	return id, len(store.items)
}

func (store *Store[K, M]) Used(id int64, delta int32) {
	store.mx.RLock()
	atomic.AddInt32(&store.used[id-store.offset], delta)
	store.mx.RUnlock()
}

func (store *Store[K, M]) cleanup() {
	var pad int
	for ; pad < len(store.used) && store.used[pad] == 0; pad++ {
	}
	if pad == 0 {
		return
	}

	n := copy(store.items, store.items[pad:])
	copy(store.used, store.used[pad:])
	clear(store.items[n:])
	store.items = store.items[:n]
	store.used = store.used[:n]
	store.offset += int64(pad)
	store.index.Drop(store.offset)
}

func NewStore[K comparable, M any]() *Store[K, M] {
	size := 1024
	return &Store[K, M]{
		offset: 1000,
		items:  make([]M, 0, size),
		used:   make([]int32, 0, size),
		index:  NewIndex[K](),
	}
}
