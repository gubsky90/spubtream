package index

import (
	"cmp"
	"sort"
)

type Sharded[K cmp.Ordered, V comparable] struct {
	shards []*Shard[K, V]
	unused []*Page[K, V]
	hash   func(K) int
}

type Page[K cmp.Ordered, V comparable] struct {
	offset uint8
	len    uint8
	keys   [256]K
	values [256]V
}

type Shard[K cmp.Ordered, V comparable] struct {
	pages []*Page[K, V]
}

func (shard *Shard[K, V]) search(key K) (page *Page[K, V], pageIndex, index int) {
	pageIndex = sort.Search(len(shard.pages), func(i int) bool {
		p := shard.pages[i]
		return key <= p.keys[p.offset+((p.len-p.offset)/2)]
	})

	page = shard.pages[pageIndex]

	index = sort.Search(int(page.len-page.offset), func(i int) bool {
		return key <= page.keys[int(page.offset)+i]
	})
	return
}

func (shard *Shard[K, V]) add(key K, value V) {
	if len(shard.pages) == 0 {
		shard.pages = append(shard.pages, &Page[K, V]{})
	}

	page := shard.pages[0]
	page.keys[page.len] = key
	page.values[page.len] = value
	page.len++
}

func (shard *Shard[K, V]) del(key K, value V) {

}

func (shard *Shard[K, V]) extract(key K, fn func(V)) {

}

func (sh *Sharded[K, V]) Add(key K, value V) {
	sh.shard(key).add(key, value)
}

func (sh *Sharded[K, V]) Del(key K, value V) {
	sh.shard(key).del(key, value)
}

func (sh *Sharded[K, V]) Extract(key K, fn func(V)) {
	sh.shard(key).extract(key, fn)
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
	return &Sharded[K, V]{
		shards: shards,
		hash:   hash,
	}
}
