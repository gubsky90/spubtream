package spubtream

import (
	"sync"
)

type Messages[M any] struct {
	sync.RWMutex
	offset   int64
	messages []M
	used     []int
}

func (m *Messages[M]) Add(msg M, used int) (int64, int64) {
	m.Lock()

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
	}

	m.messages = append(m.messages, msg)
	m.used = append(m.used, used)
	m.Unlock()

	// fmt.Println("len", len(m.messages), "cap", cap(m.messages))

	return id, l
}

func (m *Messages[M]) Used(id int64, delta int) {
	m.Lock()
	m.used[id-m.offset] += delta
	m.Unlock()
}

func (m *Messages[M]) Get(id int64) M {
	m.RLock()
	defer m.RUnlock()
	return m.messages[id-m.offset]
}

func (m *Messages[M]) Offset() int64 {
	m.RLock()
	defer m.RUnlock()
	return m.offset
}
