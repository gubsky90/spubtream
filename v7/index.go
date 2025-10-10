package spubtream

import "sort"

type Index[K comparable] struct {
	data map[K][]int64
}

func (index *Index[K]) Next(key K, id int64) int64 {
	if ids := index.data[key]; len(ids) > 0 {
		idx := sort.Search(len(ids), func(i int) bool { return ids[i] > id })
		if idx < len(ids) {
			return ids[idx]
		}
	}
	return -1
}

func (index *Index[K]) Append(key K, id int64) {
	index.data[key] = append(index.data[key], id)
}

func (index *Index[K]) Drop(offset int64) {
	for key, ids := range index.data {
		if ids[0] > offset {
			continue
		}

		idx := sort.Search(len(ids), func(i int) bool { return ids[i] > offset })
		if idx == len(ids) {
			delete(index.data, key)
			// store.pool.Put(ids)
		} else if idx > 0 {
			index.data[key] = ids[:copy(ids, ids[idx:])]
		}
	}
}

func NewIndex[K comparable]() *Index[K] {
	return &Index[K]{
		data: make(map[K][]int64),
	}
}
