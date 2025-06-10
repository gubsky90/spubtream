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
}

func (h *Handler) OnMsg1(ctx context.Context, client *Client, msg string) error {
	fmt.Println("[OnMsg1]", client, msg)
	return nil
}

func Test_SizeOf_Subscription(t *testing.T) {
	var s Subscription[any]
	fmt.Println(unsafe.Sizeof(s))
}

func Test_Stream_count(t *testing.T) {
	ctx := context.Background()

	stream := NewStream(func(ctx context.Context, client *Client, msg string) error {
		client.ID++
		return nil
	})

	var clients []*Client
	for i := 0; i < 10; i++ {
		client := &Client{}
		clients = append(clients, client)
		stream.Sub(client, 0, fmt.Sprintf("a%d", i), fmt.Sprintf("b%d", i))
	}

	for i := 0; i < 1000; i++ {
		stream.Pub(ctx, "text", fmt.Sprintf("a%d", i%10), fmt.Sprintf("b%d", i%2))
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

	h := &Handler{}

	stream := NewStream(h.OnMsg1)

	//onStep = func() {
	//	printStream(stream)
	//	fmt.Println()
	//}

	stream.Sub(c1, -1, "one")
	stream.Sub(c2, -1, "one")

	stream.Pub(ctx, "msg1", "one")
	stream.Pub(ctx, "msg2", "one")
	stream.Pub(ctx, "msg3", "one")

	stream.UnSub(c1)

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
