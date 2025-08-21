package v3

import (
	"encoding/json"
	"os"
	"testing"
)

func TestName(t *testing.T) {
	stream := NewStream[string, int]()
	stream.Sub(1, "tag")
	stream.Sub(2, "tag", "two")
	stream.Sub(3, "tag")

	stream.Pub("msg1", "tag")
	stream.Pub("msg2", "two")

	for i := 0; i < 10; i++ {
		stream.Read()
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
		"offset":        stream.offset,
		"messages":      stream.messages,
		"used":          stream.used,
		"subscriptions": stream.subscriptions,
		"tags":          stream.tags,
		"head":          stream.head,
		"tail":          stream.tail,
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
