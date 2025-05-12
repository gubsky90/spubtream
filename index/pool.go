package index

import (
	"cmp"
)

type Pool[K cmp.Ordered, V comparable] struct {
	items []*Page[K, V]
}

func (p *Pool[K, V]) Free(page *Page[K, V]) {
	p.items = append(p.items, page)
}

func (p *Pool[K, V]) Alloc(key K, value V) *Page[K, V] {
	if len(p.items) > 0 {
		page := p.items[len(p.items)-1]
		p.items = p.items[:len(p.items)-1]
		return page
	}

	return &Page[K, V]{
		len:    1,
		key:    key,
		values: [PageValues]V{value},
	}
}
