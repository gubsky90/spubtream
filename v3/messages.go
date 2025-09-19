package spubtream

import "sync"

type Messages[M any] struct {
	sync.RWMutex
	offset   int
	messages []M
	used     []int
}

func (m *Messages[M]) LockForAdd() int {
	m.Lock()
	return m.offset + len(m.messages)
}

func (m *Messages[M]) AddAndUnlock(msg M, used int) {
	m.messages = append(m.messages, msg)
	m.used = append(m.used, used)
	m.Unlock()
}

func (m *Messages[M]) Used(id int, delta int) {
	m.Lock()
	m.used[id-m.offset] += delta
	m.Unlock()
}

func (m *Messages[M]) Get(id int) M {
	m.RLock()
	defer m.RUnlock()
	return m.messages[id-m.offset]
}

func (m *Messages[M]) GetDrop() (drop, dropOffset int) {
	m.RLock()
	defer m.RUnlock()
	if len(m.messages) == 0 {
		return
	}
	for _, count := range m.used {
		if count > 0 {
			break
		}
		drop++
	}
	return drop, m.offset + drop
}

func (m *Messages[M]) Drop(drop int) {
	m.Lock()
	n := copy(m.messages, m.messages[drop:])
	copy(m.used, m.used[drop:])
	clear(m.messages[n:])
	m.messages = m.messages[:n]
	m.used = m.used[:n]
	m.offset += drop
	m.Unlock()
}
