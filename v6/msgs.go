package spubtream

import (
	"sort"
	"sync"
	"sync/atomic"
)

type Store[K comparable, M any] struct {
	append sync.Mutex
	mx     sync.RWMutex

	offset int64
	used   []int32
	items  []M
	keys   map[K][]int64
}

func (store *Store[K, M]) Next(id int64, keys []K) (int64, bool) {
	store.mx.RLock()
	defer store.mx.RUnlock()

	var next int64 = -1
	for _, key := range keys {
		ids := store.keys[key]
		l := len(ids)
		if l == 0 {
			continue
		}
		n := sort.Search(l, func(i int) bool {
			return ids[i] > id
		})
		if n < l && (next == -1 || ids[n] < next) {
			next = ids[n]
		}
	}

	atomic.AddInt32(&store.used[id-store.offset], -1)

	if next == -1 {
		return next, false
	}

	atomic.AddInt32(&store.used[next-store.offset], 1)

	return next, true
}

func (store *Store[K, M]) Get(id int64) M {
	store.mx.RLock()
	defer store.mx.RUnlock()
	return store.items[id-store.offset]
}

func (store *Store[K, M]) Append(item M, keys []K, fn func(id int64) (used int32)) {
	store.append.Lock()
	defer store.append.Unlock()

	id := store.offset + int64(len(store.items))

	used := fn(id)
	if used == 0 {
		return
	}

	store.mx.Lock()
	defer store.mx.Unlock()

	for _, key := range keys {
		store.keys[key] = append(store.keys[key], id)
	}
	store.items = append(store.items, item)
	store.used = append(store.used, used)
}

func NewStore[K comparable, M any]() *Store[K, M] {
	size := 1024
	return &Store[K, M]{
		offset: 1000,
		items:  make([]M, 0, size),
		used:   make([]int32, 0, size),
		keys:   make(map[K][]int64),
	}
}
