package way

import (
	"context"
	"fmt"
	"testing"
	"time"
	"unsafe"
)

type TestMessage struct {
	ID   int
	Tags []string
}

func (msg *TestMessage) MessageTags() []string {
	return msg.Tags
}

type TestReceiver struct {
	ID     int
	Silent bool
	Delay  time.Duration
}

func (r *TestReceiver) Receive(ctx context.Context, msg *TestMessage) error {
	if r.Silent {
		return nil
	}
	if r.Delay != 0 {
		time.Sleep(r.Delay)
	}
	fmt.Printf("[Receive][%d] %d %v\n", r.ID, msg.ID, msg.Tags)
	return nil
}

func TestSizeOfSubscription(t *testing.T) {
	fmt.Println(unsafe.Sizeof(Subscription[*TestMessage]{}))
}

func Test_Stream(t *testing.T) {
	ctx := context.Background()
	_ = ctx

	stream := NewStream[*TestMessage]()

	onStep = func() {
		printStream(stream)
		fmt.Println()
	}

	stream.Pub(ctx, &TestMessage{ID: 1, Tags: []string{"one"}})
	stream.Pub(ctx, &TestMessage{ID: 2, Tags: []string{"one"}})
	stream.Pub(ctx, &TestMessage{ID: 3, Tags: []string{"one"}})
	// stream.Pub(ctx, &TestMessage{ID: 2, Tags: []string{"one"}})
	// stream.Pub(ctx, &TestMessage{ID: 3, Tags: []string{"one"}})

	stream.Sub(&TestReceiver{ID: 1, Delay: time.Second}, -1, "one")

	// stream.WaitWorkers()
	// printStream(stream)

	//for i := 1; i <= 5; i++ {
	//	fmt.Printf("\n[STEP] %d\n", i)
	//	stream.step()
	//	printStream(stream)
	//}

	time.Sleep(time.Second * 5)
}

//func Benchmark_Stream_step(b *testing.B) {
//	ctx := context.Background()
//	stream := NewStream[*TestMessage]()
//
//	for i := 0; i < 1000000; i++ {
//		stream.Sub(&TestReceiver{ID: i, Silent: true}, 0,
//			fmt.Sprintf("user%d", i%100000),
//			fmt.Sprintf("role%d", i%1000),
//			"all",
//		)
//	}
//
//	for i := 0; i < 100000; i++ {
//		stream.Pub(ctx, &TestMessage{ID: i, Tags: []string{fmt.Sprintf("user%d", i)}})
//	}
//	for i := 0; i < 1000; i++ {
//		stream.Pub(ctx, &TestMessage{ID: i, Tags: []string{fmt.Sprintf("role%d", i)}})
//	}
//	for i := 0; i < 100; i++ {
//		stream.Pub(ctx, &TestMessage{ID: i, Tags: []string{"all"}})
//	}
//
//	b.ReportAllocs()
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		_, _, _ = stream.step()
//	}
//}

func printStream[T Message](stream *Stream[T]) {
	fmt.Printf("head: %s\n", stream.head)
	fmt.Printf("tail: %s\n", stream.tail)
	fmt.Printf("used: %v\n", stream.used)
	fmt.Printf("offset: %v\n", stream.offset)

	fmt.Printf("index\n")
	for tagID, item := range stream.index {
		fmt.Printf("\t%v\n", tagID)
		fmt.Printf("\t\tmsgIDs: %v\n", item.msgIDs)

		fmt.Printf("\t\tsubs\n")
		for _, sub := range item.subs {
			fmt.Printf("\t\t\t%s\n", sub)
		}
	}
}
