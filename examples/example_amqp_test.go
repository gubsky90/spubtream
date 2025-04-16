package examples

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gubsky90/spubtream"
	amqp "github.com/rabbitmq/amqp091-go"
)

func Test_AMQP(t *testing.T) {
	url := "amqp://guest:guest@rabbit.stream.local:5672/"
	queue := "test"

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()

	if err := ch.Qos(1, 0, false); err != nil {
		t.Fatal(err)
	}

	delivery, err := ch.Consume(queue, "", false, false, false, false, amqp.Table{
		"x-stream-offset": "last",
	})
	if err != nil {
		t.Fatal(err)
	}

	stream := spubtream.New[*AMQPMessage](context.Background()).Stream()

	var received int64
	go func() {
		for {
			time.Sleep(time.Second)
			fmt.Println("received", atomic.SwapInt64(&received, 0))
		}
	}()

	for i := 0; i < 1000000; i++ {
		stream.Sub(spubtream.ReceiverFunc[*AMQPMessage](func(_ context.Context, msg *AMQPMessage) error {
			atomic.AddInt64(&received, 1)
			return nil
		}), stream.Newest(), []string{
			"all",
			fmt.Sprintf("user#%d", i%100000),
		})
	}

	for d := range delivery {
		_ = d.Ack(false)
		msg, err := AMQPMessageFromDelivery(d.Headers["x-stream-offset"].(int64), d)
		if err != nil {
			// do some
			continue
		}
		stream.Pub(msg)
	}
}
