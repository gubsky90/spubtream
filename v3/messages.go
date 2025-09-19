package spubtream

import "sync"

type Messages[M any] struct {
	sync.RWMutex
	offset   int64
	messages []M
	used     []int
}

func (m *Messages[M]) Add(msg M, used int) int64 {
	m.Lock()
	id := m.offset + int64(len(m.messages))
	m.messages = append(m.messages, msg)
	m.used = append(m.used, used)
	m.Unlock()
	return id
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

func (m *Messages[M]) GetDrop() (drop int, dropOffset int64) {
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
	return drop, m.offset + int64(drop)
}

func (m *Messages[M]) Drop(drop int) {
	m.Lock()
	n := copy(m.messages, m.messages[drop:])
	copy(m.used, m.used[drop:])
	clear(m.messages[n:])
	m.messages = m.messages[:n]
	m.used = m.used[:n]
	m.offset += int64(drop)
	m.Unlock()
}
