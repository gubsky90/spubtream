package spubtream

import (
	"slices"
	"sort"
	"sync"
)

type Messages[K comparable, M any] struct {
	sync.RWMutex
	offset   int64
	messages []M
	used     []int
	keys     map[K][]int64
	pool     [][]int64
}

func NewMessages[K comparable, M any]() *Messages[K, M] {
	return &Messages[K, M]{
		offset:   1000,
		messages: make([]M, 0, 512),
		used:     make([]int, 0, 512),
		keys:     make(map[K][]int64, 512),
	}
}

func (m *Messages[K, M]) NextMessage(offset int64, keys []K) (next int64) {
	m.RLock()
	defer m.RUnlock()

	next = -1
	for _, key := range keys {
		ids := m.keys[key]
		l := len(ids)
		if l == 0 {
			continue
		}
		n := sort.Search(l, func(i int) bool {
			return ids[i] > offset
		})
		if n < l && (next == -1 || ids[n] < next) {
			next = ids[n]
		}
	}

	return
}

func (m *Messages[K, M]) Add(msg M, used int, keys []K) (int64, int64) {
	m.Lock()
	defer m.Unlock()

	l := int64(len(m.messages))
	id := m.offset + l
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
					m.pool = append(m.pool, ids)
				}
			}
		}
	}

	m.messages = append(m.messages, msg)
	m.used = append(m.used, used)

	for _, key := range keys {
		data := m.keys[key]
		if data == nil {
			if len(m.pool) > 0 {
				data = m.pool[len(m.pool)-1]
				m.pool = m.pool[:len(m.pool)-1]
			} else {
				data = make([]int64, 0, 1)
			}
		}
		m.keys[key] = append(data, id)
	}

	// fmt.Println("len", len(m.messages), "cap", cap(m.messages))

	return id, l
}

func (m *Messages[K, M]) Used(id int64, delta int) {
	m.Lock()
	m.used[id-m.offset] += delta
	m.Unlock()
}

func (m *Messages[K, M]) Get(id int64) M {
	m.RLock()
	defer m.RUnlock()
	return m.messages[id-m.offset]
}

func (m *Messages[K, M]) Offset() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.offset
}
