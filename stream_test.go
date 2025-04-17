package spubtream

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
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

func Test_Stream_Stress(t *testing.T) {
	stream := New[*TestMessage](context.Background()).
		WithGCInterval(time.Millisecond * 5000).
		WithWorkersLimit(1024).
		Stream()

	var received int64

	ts := time.Now()
	for i := 0; i < 1000000; i++ {
		stream.SubFunc(func(_ context.Context, msg *TestMessage) error {
			atomic.AddInt64(&received, 1)
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			return nil
		}, stream.Newest(),
			"all",
			fmt.Sprintf("role#%d", i%10),
			fmt.Sprintf("user#%d", i%100000),
		)
	}
	fmt.Println("Sub done", time.Since(ts))

	var tags []string
	//tags = append(tags, "all")
	//for i := 0; i < 10; i++ {
	//	tags = append(tags, fmt.Sprintf("role#%d", i))
	//}
	for i := 0; i < 100000; i++ {
		tags = append(tags, fmt.Sprintf("user#%d", i))
	}

	var pubs int64
	go func() {
		for {
			stream.Pub(&TestMessage{Tags: []string{"all", tags[int(atomic.AddInt64(&pubs, 1))%len(tags)]}})
		}
	}()

	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		fmt.Println(
			"pubs", atomic.SwapInt64(&pubs, 0),
			"received", atomic.SwapInt64(&received, 0),
			"workers", stream.workers,
		)
	}
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
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := 0
		for pos < len(s.messages) {
			pos, _ = s.nextPos([]string{"two"}, pos)
		}
	}
}

func assertEq(t *testing.T, act, exp any) {
	if sact, sexp := fmt.Sprint(act), fmt.Sprint(exp); sact != sexp {
		t.Fatalf("act: %s; exp: %s", sact, sexp)
	}
}
