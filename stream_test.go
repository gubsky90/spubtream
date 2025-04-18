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
		WithGCInterval(time.Millisecond * 1000).
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
			fmt.Sprintf("role#%d", i%10),
			"all",
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
			stream.Pub(&TestMessage{Tags: []string{tags[int(atomic.AddInt64(&pubs, 1))%len(tags)]}})
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

func Test_Stream_some(t *testing.T) {
	stream := New[*TestMessage](context.Background()).Stream()
	pos := stream.Newest()

	fmt.Println("Sub")
	stream.SubFunc(func(_ context.Context, msg *TestMessage) error {
		fmt.Println("[Receive]", msg.Tags)
		return nil
	}, pos, "all", "one", "two")

	for shardID, subs := range stream.idleSubs {
		if len(subs) == 0 {
			continue
		}
		fmt.Println(shardID, subs)
	}

	fmt.Println("Pub")
	stream.Pub(&TestMessage{Tags: []string{"one"}})
	stream.Pub(&TestMessage{Tags: []string{"two"}})

	for shardID, subs := range stream.idleSubs {
		if len(subs) == 0 {
			continue
		}
		fmt.Println(shardID, subs)
	}

	fmt.Println("WaitWorkers")
	stream.WaitWorkers()

	for shardID, subs := range stream.idleSubs {
		if len(subs) == 0 {
			continue
		}
		fmt.Println(shardID, subs)
	}
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

func Benchmark_Contains(b *testing.B) {
	shard := make([]*Subscription[*TestMessage], 1000000)
	for i := range shard {
		shard[i] = &Subscription[*TestMessage]{
			tags: []int{1, 2, 3},
		}
	}

	tag := 3

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sub := range shard {
			existsInOrdered(sub.tags, tag)
		}
	}
}

func assertEq(t *testing.T, act, exp any) {
	if sact, sexp := fmt.Sprint(act), fmt.Sprint(exp); sact != sexp {
		t.Fatalf("act: %s; exp: %s", sact, sexp)
	}
}

func Test_Map(t *testing.T) {
	idle := map[int][]*Subscription[*TestMessage]{}
	for i := 0; i < 1000000; i++ {
		tags := EncodeAll(
			"all",
			fmt.Sprintf("role#%d", i%10),
			fmt.Sprintf("user#%d", i%100000),
		)
		sub := &Subscription[*TestMessage]{
			tags: tags,
		}
		for _, tag := range tags {
			idle[tag] = append(idle[tag], sub)
		}
	}
	time.Sleep(time.Hour)
}

func Test_Slice(t *testing.T) {
	idle := []*Subscription[*TestMessage]{}
	for i := 0; i < 1000000; i++ {
		tags := EncodeAll(
			"all",
			fmt.Sprintf("role#%d", i%10),
			fmt.Sprintf("user#%d", i%100000),
		)
		sub := &Subscription[*TestMessage]{
			tags: tags,
		}
		idle = append(idle, sub)
	}
	time.Sleep(time.Hour)
}
