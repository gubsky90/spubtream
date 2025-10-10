package spubtream

import (
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
)

func Test_Messages(t *testing.T) {

	m := NewMessages[string, string]()

	m.Add("msg1", 1, []string{"one"})
	m.Add("msg2", 1, []string{"two"})
	m.Add("msg3", 1, []string{"one", "two"})

	// fmt.Println(m.NextMessage(1002, []string{"one"}))

	printJSON(m)
}

func Benchmark_Messages_Add(b *testing.B) {
	m := NewMessages[string, string]()
	msg := "msg"
	tags := []string{"1", "2", "3", "4", "5", "6"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.Add(msg, 0, tags[:i%len(tags)])
	}
}

func Benchmark_Store_Next(b *testing.B) {
	s := NewMessages[string, int]()

	keys := []string{"one", "two", "three"}

	for i := 0; i < 10; i++ {
		s.Add(i, 0, keys)
	}

	var target atomic.Int64

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.NextMessage(1000, &target, keys)
	}
}

func Benchmark_Store_Append(b *testing.B) {
	s := NewMessages[string, int]()

	keys := []string{"one", "two", "three"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Add(i, 0, keys)
	}
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "\t")
	if err := enc.Encode(v); err != nil {
		panic(err)
	}
}

func (m *Messages[K, M]) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"offset":   m.offset,
		"messages": m.messages,
		"used":     m.used,
		"keys":     m.keys,
		// "pool":     m.pool,
	})
}
