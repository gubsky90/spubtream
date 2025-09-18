package spubtream

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"
)

func Test_Example_001(t *testing.T) {
	stream := NewStream[string, int]()
	stream.Sub(1, "one", "two")
	stream.Pub("msg1", "one")
	stream.Pub("msg2", "two")
	stream.Pub("msg3", "one", "two")
	var wg sync.WaitGroup
	wg.Add(3)
	stream.Start(func(id int, msg string) {
		defer wg.Done()
		fmt.Println(id, msg)
	})
	wg.Wait()
}

func TestName(t *testing.T) {
	type Client struct {
		id       int
		tags     []string
		expected int
		received int
	}

	clients := []*Client{
		{
			id:       1,
			tags:     []string{"one"},
			expected: 10,
		},
		{
			id:       2,
			tags:     []string{"two"},
			expected: 10,
		},
		{
			id:       3,
			tags:     []string{"one", "two"},
			expected: 20,
		},
	}

	stream := NewStream[string, *Client]()
	var wg sync.WaitGroup
	for _, client := range clients {
		client := client
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream.Sub(client, client.tags...)
		}()
	}
	wg.Wait()

	for tag, count := range map[string]int{
		"one": 10,
		"two": 10,
	} {
		tag := tag
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				stream.Pub(tag, tag)
			}()
		}
	}
	wg.Wait()

	wg.Add(40)
	stream.Start(func(client *Client, message string) {
		defer wg.Done()

		fmt.Println(client.id, message)

		if !slices.Contains(client.tags, message) {
			t.Fatal("unexpected message", client.id, message)
		}
		client.received++
	})
	wg.Wait()

	for _, client := range clients {
		if client.expected != client.received {
			t.Fatal("unexpected received", client.id, client.expected, client.received)
		}
	}
}

func BenchmarkPub(b *testing.B) {
	stream := NewStream[string, int]()
	for i := 0; i < 100000; i++ {
		stream.Sub(i, "tag")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream.Pub("payload", "tag")
	}
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "\t")
	if err := enc.Encode(v); err != nil {
		panic(err)
	}
}

func (stream *Stream[M, R]) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"messages":      stream.messages,
		"subscriptions": stream.subscriptions,
		"tags":          stream.tags,
	})
}

func (m *Messages[M]) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"offset":   m.offset,
		"messages": m.messages,
		"used":     m.used,
	})
}

func (sub *Subscription[R]) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"offset": sub.offset,
		"tags":   "...", // sub.tags,
		"next":   sub.next,
	})
}

func (root *TagRoot[R]) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"msgIDs":        root.msgIDs,
		"subscriptions": "...", // root.subscriptions,
	})
}

func (tr *TagReceiver[R]) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"next":         tr.next,
		"subscription": tr.subscription,
	})
}
