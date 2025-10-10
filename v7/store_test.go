package spubtream

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func Benchmark_Store_Next(b *testing.B) {
	s := NewStore[string, int]()

	keys := []string{"one", "two", "three"}

	for i := 0; i < 10; i++ {
		s.Append(i, keys, func(id int64) (used int32) {
			return
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Next(1000, keys)
	}
}

func Benchmark_Store_Append(b *testing.B) {
	s := NewStore[string, int]()

	keys := []string{"one", "two", "three"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Append(i, keys, func(id int64) (used int32) {
			return
		})
	}
}

func Test_Store(t *testing.T) {
	s := NewStore[string, int]()

	for i := 0; i < 5; i++ {
		s.Append(i, []string{"one"}, func(id int64) (used int32) {
			fmt.Println("Append", id)
			if i == 0 {
				return 1
			}
			return 0
		})
	}

	tags := []string{"one", "two"}

	fmt.Println("Next", s.Next(1000, tags))
	fmt.Println("Next", s.Next(1001, tags))

	fmt.Println("Next", s.Next(1002, tags))
	fmt.Println("Next", s.Next(1003, tags))
	fmt.Println("Next", s.Next(1004, tags))

	s.Append(5, []string{"one"}, func(id int64) (used int32) {
		fmt.Println("Append", id)
		return 0
	})

	printJSON(s)
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "\t")
	if err := enc.Encode(v); err != nil {
		panic(err)
	}
}

func (store *Store[K, M]) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"offset": store.offset,
		"used":   store.used,
		"items":  store.items,
		"index":  store.index,
	})
}
