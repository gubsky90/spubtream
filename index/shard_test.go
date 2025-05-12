package index

import (
	"cmp"
	"fmt"
	"testing"
)

func Test_Shard_add(t *testing.T) {
	pool := &Pool[int, string]{}
	shard := &Shard[int, string]{}

	shard.add(1, "one", pool)
	shard.add(3, "three", pool)
	shard.add(1, "one_again", pool)
	shard.add(2, "two", pool)
	shard.add(2, "two_again", pool)
	shard.add(2, "two", pool)
	shard.add(2, "two", pool)
	shard.add(2, "two", pool)
	shard.add(2, "two", pool)
	shard.add(2, "two", pool)
	shard.add(2, "two", pool)
	shard.add(2, "two", pool)
	shard.add(4, "four", pool)

	shard.extract(2, func(value string) {
		fmt.Println("extract", value)
	}, pool)

	printShard(shard)
}

func Test_Sharded_Example(t *testing.T) {
	index := NewSharded[int, string](10, func(key int) int {
		return key
	})

	index.Add(1, "one")
	index.Add(2, "two")
	printIndex(index)

	index.Del(1, "one")
	printIndex(index)
}

func printIndex[K cmp.Ordered, V comparable](index *Sharded[K, V]) {
	for _, shard := range index.shards {
		printShard(shard)
	}
}

func printShard[K cmp.Ordered, V comparable](shard *Shard[K, V]) {
	for _, page := range shard.pages {
		fmt.Printf("(%v)[%d]{", page.key, page.len)
		for i := uint8(0); i < page.len; i++ {
			if i == 0 {
				fmt.Printf("%v", page.values[i])
			} else {
				fmt.Printf(",%v", page.values[i])
			}
		}
		fmt.Println("}")
	}
}
