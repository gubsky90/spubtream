package spubtream

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
	"unsafe"
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

func TestSizeOfSubscription(t *testing.T) {
	fmt.Println(unsafe.Sizeof(Subscription[*TestMessage]{}))
}

func Test_Stream_PubSub(t *testing.T) {
	var receiveCalls int
	stream := New[*TestMessage](context.Background()).Stream()
	pos := stream.Newest()

	_ = stream.Pub(context.TODO(), &TestMessage{Tags: []string{"first"}})

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

	_ = stream.Pub(context.TODO(), &TestMessage{Tags: []string{"first"}})

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

	_ = stream.Pub(context.TODO(), &TestMessage{Tags: []string{"first"}})
	stream.WaitWorkers()

	_ = stream.Pub(context.TODO(), &TestMessage{Tags: []string{"first"}})

	assertEq(t, receiveCalls, 1)
}

func Test_Stream_ReSub(t *testing.T) {
	stream := New[*TestMessage](context.Background()).Stream()

	var received []int
	sub := stream.SubFunc(func(_ context.Context, msg *TestMessage) error {
		received = append(received, msg.ID)
		return nil
	}, stream.Newest(), "first")

	_ = stream.Pub(context.TODO(), &TestMessage{ID: 1, Tags: []string{"first"}})
	_ = stream.Pub(context.TODO(), &TestMessage{ID: 2, Tags: []string{"second"}})
	stream.WaitWorkers()

	stream.ReSub(sub, []string{"second"}, []string{"first"})

	_ = stream.Pub(context.TODO(), &TestMessage{ID: 3, Tags: []string{"first"}})
	_ = stream.Pub(context.TODO(), &TestMessage{ID: 4, Tags: []string{"second"}})
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

	_ = stream.Pub(context.TODO(), &TestMessage{ID: 1, Tags: []string{"first"}})
	_ = stream.Pub(context.TODO(), &TestMessage{ID: 2, Tags: []string{"second"}})
	stream.WaitWorkers()

	assertEq(t, received, []int{1, 2})
	received = nil
	stream.UnSub(sub)

	_ = stream.Pub(context.TODO(), &TestMessage{ID: 3, Tags: []string{"first"}})
	_ = stream.Pub(context.TODO(), &TestMessage{ID: 4, Tags: []string{"second"}})
	stream.WaitWorkers()

	assertEq(t, received, []int{})
}

func Test_Stream_gc(t *testing.T) {
	ctx := context.Background()
	stream := New[*TestMessage](context.Background()).WithGCInterval(0).Stream()
	pos := stream.Newest()
	gcFn := func(messages []*TestMessage) int {
		return len(messages)
	}

	_ = stream.Pub(ctx, &TestMessage{ID: 1, Tags: []string{"one"}})
	_ = stream.Pub(ctx, &TestMessage{ID: 2, Tags: []string{"one"}})
	_ = stream.Pub(ctx, &TestMessage{ID: 3, Tags: []string{"one"}})

	ch := make(chan *TestMessage)
	stream.SubFunc(func(_ context.Context, msg *TestMessage) error {
		ch <- msg
		return nil
	}, pos, "one")

	assertEq(t, stream.Oldest().pos, 0)
	assertEq(t, (<-ch).ID, 1)
	time.Sleep(time.Millisecond)
	stream.gc(gcFn)

	assertEq(t, stream.Oldest().pos, 1)
	assertEq(t, (<-ch).ID, 2)
	time.Sleep(time.Millisecond)
	stream.gc(gcFn)

	assertEq(t, stream.Oldest().pos, 2)
	assertEq(t, (<-ch).ID, 3)
	time.Sleep(time.Millisecond)
	stream.gc(gcFn)
}

func Benchmark_nextPos(b *testing.B) {
	stream := New[*TestMessage](context.Background()).
		WithBufferSizeLimit(0).
		Stream()
	for i := 0; i < 100000; i++ {
		if i%20000 == 0 {
			_ = stream.Pub(context.TODO(), &TestMessage{Tags: []string{"one", "two"}})
		} else {
			_ = stream.Pub(context.TODO(), &TestMessage{Tags: []string{"one"}})
		}
	}

	two := Encode("two")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := 0
		for pos < len(stream.messages) {
			pos, _ = stream.nextPos([]int{two}, pos)
		}
	}
}

func Benchmark_Pub(b *testing.B) {
	stream := New[*TestMessage](context.Background()).
		WithBufferSizeLimit(0).
		Stream()

	ctx := context.TODO()
	msg1 := &TestMessage{Tags: []string{"one", "two"}}
	msg2 := &TestMessage{Tags: []string{"one"}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%20000 == 0 {
			_ = stream.Pub(ctx, msg1)
		} else {
			_ = stream.Pub(ctx, msg2)
		}
	}
}

func assertEq(t *testing.T, act, exp any) {
	if sact, sexp := fmt.Sprint(act), fmt.Sprint(exp); sact != sexp {
		t.Fatalf("act: %s; exp: %s", sact, sexp)
	}
}
