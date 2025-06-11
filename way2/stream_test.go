package way

import (
	"context"
	"fmt"
	"testing"
	"time"
	"unsafe"
)

func TestSizeOfSubscription(t *testing.T) {
	fmt.Println(unsafe.Sizeof(Subscription[*int]{}))
}

type Client struct {
	ID int
}

type Handler struct {
	stream *Stream[string, *Client]
}

func (h *Handler) OnMsg1(client *Client, msg string) {
	fmt.Println("[OnMsg1]", client, msg)
}

func Test_SizeOf_Subscription(t *testing.T) {
	var s Subscription[any]
	fmt.Println(unsafe.Sizeof(s))
}

func Test_Stream_offset(t *testing.T) {
	ctx := context.Background()
	stream := NewStream[string, *Client]()

	stream.Pub(ctx, "t1_1", "t1")
	stream.Pub(ctx, "t1_2", "t1")
	stream.Pub(ctx, "t1_3", "t1")

	stream.Pub(ctx, "t2_1", "t2")
	stream.Pub(ctx, "t2_2", "t2")
	stream.Pub(ctx, "t2_3", "t2")

	stream.Pub(ctx, "t3_1", "t3")
	stream.Pub(ctx, "t3_2", "t3")
	stream.Pub(ctx, "t3_3", "t3")

	stream.Sub(&Client{}, stream.Last, "t2")

}

func Test_Stream_count(t *testing.T) {
	ctx := context.Background()

	stream := NewStream[string, *Client]()

	stream.Start(func(client *Client, msg string) {
		client.ID++
	})

	var clients []*Client
	for i := 0; i < 10; i++ {
		client := &Client{}
		clients = append(clients, client)
		stream.Sub(client, stream.Last, fmt.Sprintf("a%d", i), fmt.Sprintf("b%d", i))
	}

	for i := 0; i < 1000; i++ {
		_ = stream.Pub(ctx, "text", fmt.Sprintf("a%d", i%10), fmt.Sprintf("b%d", i%2))
	}

	time.Sleep(time.Second * 2)

	for i, client := range clients {
		fmt.Println(i, "=", client.ID)
	}
}

func Test_Stream(t *testing.T) {
	ctx := context.Background()
	_ = ctx

	c1 := &Client{ID: 1}
	c2 := &Client{ID: 2}
	c3 := &Client{ID: 3}

	h := &Handler{
		stream: NewStream[string, *Client](),
	}

	h.stream.Start(h.OnMsg1)

	//onStep = func() {
	//	printStream(stream)
	//	fmt.Println()
	//}

	h.stream.Pub(ctx, "msg1", "one")
	h.stream.Pub(ctx, "msg2", "one")
	h.stream.Pub(ctx, "msg3", "one")

	h.stream.Sub(c1, h.stream.First, "one")
	h.stream.Sub(c2, h.stream.Last, "one")
	h.stream.Sub(c3, func(messages []string) (int, error) {
		fmt.Println(messages)
		return 0, nil
	}, "one")

	// h.stream.UnSub(c1)

	// stream.WaitWorkers()
	// printStream(stream)

	//for i := 1; i <= 5; i++ {
	//	fmt.Printf("\n[STEP] %d\n", i)
	//	stream.step()
	//	printStream(stream)
	//}

	time.Sleep(time.Second * 5)
}

func printStream[M any, R comparable](stream *Stream[M, R]) {
	fmt.Printf("head: %v\n", stream.head)
	fmt.Printf("tail: %v\n", stream.tail)
	fmt.Printf("used: %v\n", stream.used)
	fmt.Printf("offset: %v\n", stream.offset)

	fmt.Printf("index\n")
	for tagID, item := range stream.index {
		fmt.Printf("\t%v\n", tagID)
		fmt.Printf("\t\tmsgIDs: %v\n", item.msgIDs)

		fmt.Printf("\t\treceivers\n")
		for _, sub := range item.receivers {
			fmt.Printf("\t\t\t%v\n", sub)
		}
	}
}
