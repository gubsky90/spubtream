package index

import (
	"cmp"
	"fmt"
	"testing"
)

func Test_Shard_add(t *testing.T) {
	shard := &Shard[int, string]{}

	shard.add(1, "one")
	shard.add(2, "two")
	shard.add(1, "one_again")

	printShard(shard)
}

func Test_Shard_search(t *testing.T) {
	shard := &Shard[int, string]{
		len: 6,
		pages: []*Page[int, string]{
			{
				len:    2,
				keys:   [256]int{1, 2},
				values: [256]string{"one", "two"},
			},
			{
				len:    4,
				offset: 2,
				keys:   [256]int{0, 0, 2, 3},
				values: [256]string{"", "", "two", "three"},
			},
			{
				len:    2,
				keys:   [256]int{3, 4},
				values: [256]string{"three", "four"},
			},
		},
	}

	printShard(shard)

	page, pageIndex, index := shard.search(3)
	fmt.Println(pageIndex, index, page)
}

func printShard[K cmp.Ordered, V comparable](shard *Shard[K, V]) {
	for _, page := range shard.pages {
		fmt.Printf("[%d:%d]{", page.offset, page.len)
		for i := page.offset; i < page.len; i++ {
			if i == page.offset {
				fmt.Printf("%v=%v", page.keys[i], page.values[i])
			} else {
				fmt.Printf(",%v=%v", page.keys[i], page.values[i])
			}
		}
		fmt.Println("}")
	}
}
