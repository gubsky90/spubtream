package spubtream

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type TestMessage struct {
	ID      int
	Time    time.Time
	Tags    []string
	Payload int
}

func (t *TestMessage) MessageTags() []string {
	return t.Tags
}

func Test_Stream_PubSub(t *testing.T) {
	var receiveCalls int
	stream := New[*TestMessage](context.Background()).Stream()
	pos := stream.Newest()

	stream.Pub(&TestMessage{Tags: []string{"first"}})

	stream.SubFunc(func(_ context.Context, msg *TestMessage) error {
		receiveCalls++
		return nil
	}, pos, "first")
	stream.WaitWorkers()

	assertEq(t, receiveCalls, 1)
}

func Test_Stream_SubPub(t *testing.T) {
	var receiveCalls int
	stream := New[*TestMessage](context.Background()).Stream()
	pos := stream.Newest()

	stream.SubFunc(func(_ context.Context, msg *TestMessage) error {
		receiveCalls++
		return nil
	}, pos, "first")

	stream.Pub(&TestMessage{Tags: []string{"first"}})

	stream.WaitWorkers()

	assertEq(t, receiveCalls, 1)
}

func Test_Stream_ReceiveError(t *testing.T) {
	stream := New[*TestMessage](context.Background()).Stream()

	var receiveCalls int
	stream.SubFunc(func(_ context.Context, msg *TestMessage) error {
		receiveCalls++
		return errors.New("test")
	}, stream.Newest(), "first")

	stream.Pub(&TestMessage{Tags: []string{"first"}})
	stream.WaitWorkers()

	stream.Pub(&TestMessage{Tags: []string{"first"}})

	assertEq(t, receiveCalls, 1)
}

func Test_Stream_ReSub(t *testing.T) {
	stream := New[*TestMessage](context.Background()).Stream()

	var received []int
	sub := stream.SubFunc(func(_ context.Context, msg *TestMessage) error {
		received = append(received, msg.ID)
		return nil
	}, stream.Newest(), "first")

	stream.Pub(&TestMessage{ID: 1, Tags: []string{"first"}})
	stream.Pub(&TestMessage{ID: 2, Tags: []string{"second"}})
	stream.WaitWorkers()

	stream.ReSub(sub, []string{"second"}, []string{"first"})

	stream.Pub(&TestMessage{ID: 3, Tags: []string{"first"}})
	stream.Pub(&TestMessage{ID: 4, Tags: []string{"second"}})
	stream.WaitWorkers()

	assertEq(t, received, []int{1, 4})
}

func Test_Stream_UnSub(t *testing.T) {
	stream := New[*TestMessage](context.Background()).Stream()

	var received []int
	sub := stream.SubFunc(func(_ context.Context, msg *TestMessage) error {
		received = append(received, msg.ID)
		return nil
	}, stream.Newest(), "first", "second")

	stream.Pub(&TestMessage{ID: 1, Tags: []string{"first"}})
	stream.Pub(&TestMessage{ID: 2, Tags: []string{"second"}})
	stream.WaitWorkers()

	assertEq(t, received, []int{1, 2})
	received = nil
	stream.UnSub(sub)

	stream.Pub(&TestMessage{ID: 3, Tags: []string{"first"}})
	stream.Pub(&TestMessage{ID: 4, Tags: []string{"second"}})
	stream.WaitWorkers()

	assertEq(t, received, []int{})
}

func Benchmark_nextPos(b *testing.B) {
	s := New[*TestMessage](context.Background()).Stream()
	for i := 0; i < 100000; i++ {
		if i%20000 == 0 {
			s.Pub(&TestMessage{Tags: []string{"one", "two"}})
		} else {
			s.Pub(&TestMessage{Tags: []string{"one"}})
		}
	}

	two := Encode("two")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := 0
		for pos < len(s.messages) {
			pos, _ = s.nextPos([]int{two}, pos)
		}
	}
}

func Benchmark_Pub(b *testing.B) {
	stream := New[*TestMessage](context.Background()).Stream()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%20000 == 0 {
			stream.Pub(&TestMessage{Tags: []string{"one", "two"}})
		} else {
			stream.Pub(&TestMessage{Tags: []string{"one"}})
		}
	}
}

func assertEq(t *testing.T, act, exp any) {
	if sact, sexp := fmt.Sprint(act), fmt.Sprint(exp); sact != sexp {
		t.Fatalf("act: %s; exp: %s", sact, sexp)
	}
}
