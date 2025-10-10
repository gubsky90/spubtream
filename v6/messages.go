package spubtream

import (
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
)

type Messages[K comparable, M any] struct {
	sync.RWMutex
	offset   int64
	messages []M
	used     []int32
	keys     map[K][]int64
	// pool     [][]int64
}

func NewMessages[K comparable, M any]() *Messages[K, M] {
	size := 1024
	return &Messages[K, M]{
		offset:   1000,
		messages: make([]M, 0, size),
		used:     make([]int32, 0, size),
		keys:     make(map[K][]int64, size),
	}
}

func (m *Messages[K, M]) NextMessage(current int64, target *atomic.Int64, keys []K) bool {
	m.RLock()
	defer m.RUnlock()

	var next int64 = -1
	for _, key := range keys {
		ids := m.keys[key]
		l := len(ids)
		if l == 0 {
			continue
		}
		n := sort.Search(l, func(i int) bool {
			return ids[i] > current
		})
		if n < l && (next == -1 || ids[n] < next) {
			next = ids[n]
		}
	}

	target.Store(next)
	atomic.AddInt32(&m.used[current-m.offset], -1)

	if next == -1 {
		return false
	}

	atomic.AddInt32(&m.used[next-m.offset], 1)

	return true
}

func (m *Messages[K, M]) Add(msg M, used int32, keys []K) (int64, int64) {
	m.Lock()
	l := int64(len(m.messages))
	id := m.offset + l

	for l > 0 && l == int64(cap(m.messages)) && m.used[0] != 0 {
		m.Unlock()
		runtime.Gosched()
		m.Lock()
	}

	defer m.Unlock()

	if l > 0 && l == int64(cap(m.messages)) && m.used[0] == 0 {
		var pad int64 = 1
		for ; pad < l && m.used[pad] == 0; pad++ {
		}
		n := copy(m.messages, m.messages[pad:])
		copy(m.used, m.used[pad:])
		clear(m.messages[n:])
		m.messages = m.messages[:n]
		m.used = m.used[:n]
		m.offset += pad

		for key, ids := range m.keys {
			if ids[0] <= m.offset {
				i, _ := slices.BinarySearch(ids, m.offset) // TODO: check if dropOffset not found
				ids = ids[:copy(ids, ids[i:])]
				if len(ids) > 0 {
					m.keys[key] = ids
				} else {
					delete(m.keys, key)
					// m.pool = append(m.pool, ids)
				}
			}
		}
	}

	m.messages = append(m.messages, msg)
	m.used = append(m.used, used)

	for _, key := range keys {
		data := m.keys[key]
		//if data == nil {
		//	if len(m.pool) > 0 {
		//		data = m.pool[len(m.pool)-1]
		//		m.pool = m.pool[:len(m.pool)-1]
		//	} else {
		//		data = make([]int64, 0, 1)
		//	}
		//}
		m.keys[key] = append(data, id)
	}

	// fmt.Println("len", len(m.messages), "cap", cap(m.messages))

	return id, l
}

func (m *Messages[K, M]) Used(id int64, delta int32) {
	m.Lock()
	m.used[id-m.offset] += delta
	m.Unlock()
}

func (m *Messages[K, M]) Get(id int64) M {
	m.RLock()
	defer m.RUnlock()
	return m.messages[id-m.offset]
}
