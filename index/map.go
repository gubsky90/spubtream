package index

import (
	"fmt"
	"slices"
)

type Map[K, V comparable] struct {
	data map[K][]V
}

func (m *Map[K, V]) Add(key K, value V) {
	m.data[key] = append(m.data[key], value)
}

func (m *Map[K, V]) Del(key K, value V) {
	m.data[key], _ = deleteItem(m.data[key], value)
}

func (m *Map[K, V]) Scan(key K, fn func(V)) {
	items, ok := m.data[key]
	if !ok || len(items) == 0 {
		return
	}
	for _, value := range items {
		fn(value)
	}
}

func (m *Map[K, V]) Extract(key K, fn func(V)) {
	items, ok := m.data[key]
	if !ok || len(items) == 0 {
		return
	}
	for _, value := range items {
		fn(value)
	}
	clear(items)
	// delete(m.data, key)
	m.data[key] = items[:0]
}

func (m *Map[K, V]) Stats() string {
	var keys, elms int
	for _, v := range m.data {
		keys++
		elms += len(v)
	}
	return fmt.Sprintf("[%d,%d]", keys, elms)
}

func NewMap[K, V comparable]() *Map[K, V] {
	return &Map[K, V]{
		data: map[K][]V{},
	}
}

func deleteItem[S ~[]E, E comparable](s S, e E) (S, bool) {
	i := slices.Index(s, e)
	if i < 0 {
		return s, false
	}
	s[i] = s[len(s)-1]
	return s[:len(s)-1], true
}
