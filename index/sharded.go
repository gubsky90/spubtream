package index

import (
	"cmp"
	"sort"
)

const PageValues = 255

type PagePool[K cmp.Ordered, V comparable] interface {
	Free(page *Page[K, V])
	Alloc(key K, value V) *Page[K, V]
}

type Sharded[K cmp.Ordered, V comparable] struct {
	Pool[K, V]
	shards []*Shard[K, V]
	unused []*Page[K, V]
	hash   func(K) int
}

type Page[K cmp.Ordered, V comparable] struct {
	len    uint8
	key    K
	values [PageValues]V
}

type Shard[K cmp.Ordered, V comparable] struct {
	pages []*Page[K, V]
}

func (shard *Shard[K, V]) add(key K, value V, pool PagePool[K, V]) {
	if len(shard.pages) == 0 {
		shard.pages = append(shard.pages, pool.Alloc(key, value))
		return
	}

	pageIdx := sort.Search(len(shard.pages), func(i int) bool {
		return key <= shard.pages[i].key
	})

	if pageIdx == len(shard.pages) {
		shard.pages = append(shard.pages, pool.Alloc(key, value))
		return
	}

	page := shard.pages[pageIdx]
	if page.key == key && page.len != PageValues {
		page.values[page.len] = value
		page.len++
		return
	}

	shard.pages = append(shard.pages[:pageIdx+1], shard.pages[pageIdx:]...)
	shard.pages[pageIdx] = pool.Alloc(key, value)
}

func (shard *Shard[K, V]) del(key K, value V, pool PagePool[K, V]) {
	var zero V
	pageIdx := sort.Search(len(shard.pages), func(i int) bool {
		return key <= shard.pages[i].key
	})
	for {
		page := shard.pages[pageIdx]
		if page.key != key {
			break
		}
		for i := uint8(0); i < page.len; i++ {
			if page.values[i] == value {
				page.len--
				page.values[i] = page.values[page.len]
				page.values[page.len] = zero

				if page.len == 0 {
					copy(shard.pages[pageIdx:], shard.pages[pageIdx+1:])
					shard.pages[len(shard.pages)-1] = nil
					shard.pages = shard.pages[:len(shard.pages)-1]
					pool.Free(page)
				}

				return
			}
		}
		pageIdx++
	}
}

func (shard *Shard[K, V]) scan(key K, fn func(V)) {
	pageIdx := sort.Search(len(shard.pages), func(i int) bool {
		return key <= shard.pages[i].key
	})
	for pageIdx < len(shard.pages) {
		page := shard.pages[pageIdx]
		if page.key != key {
			break
		}
		for i := uint8(0); i < page.len; i++ {
			fn(page.values[i])
		}
		pageIdx++
	}
}

func (shard *Shard[K, V]) extract(key K, fn func(V), pool PagePool[K, V]) {
	pageIdx := sort.Search(len(shard.pages), func(i int) bool {
		return key <= shard.pages[i].key
	})
	for pageIdx < len(shard.pages) {
		page := shard.pages[pageIdx]
		if page.key != key {
			break
		}
		for i := uint8(0); i < page.len; i++ {
			fn(page.values[i])
		}
		pool.Free(page)
		copy(shard.pages[pageIdx:], shard.pages[pageIdx+1:])
		shard.pages[len(shard.pages)-1] = nil
		shard.pages = shard.pages[:len(shard.pages)-1]
	}
}

func (sh *Sharded[K, V]) Add(key K, value V) {
	sh.shard(key).add(key, value, sh)
}

func (sh *Sharded[K, V]) Del(key K, value V) {
	sh.shard(key).del(key, value, sh)
}

func (sh *Sharded[K, V]) Scan(key K, fn func(V)) {
	sh.shard(key).scan(key, fn)
}

func (sh *Sharded[K, V]) Extract(key K, fn func(V)) {
	sh.shard(key).extract(key, fn, sh)
}

func (sh *Sharded[K, V]) Stats() string {
	return ""
}

func (sh *Sharded[K, V]) shard(key K) *Shard[K, V] {
	idx := sh.hash(key) % len(sh.shards)
	return sh.shards[idx]
}

func NewSharded[K cmp.Ordered, V comparable](size int, hash func(K) int) *Sharded[K, V] {
	shards := make([]*Shard[K, V], size)
	for i := 0; i < size; i++ {
		shards[i] = &Shard[K, V]{}
	}
	return &Sharded[K, V]{
		shards: shards,
		hash:   hash,
	}
}
