package examples

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/gubsky90/spubtream"
	amqp "github.com/rabbitmq/amqp091-go"
)

type AMQPMessage struct {
	Offset  int64
	Tags    []string
	Payload []byte
}

func (msg *AMQPMessage) MessageTags() []string {
	return msg.Tags
}

func AMQPMessageFromDelivery(offset int64, d amqp.Delivery) (*AMQPMessage, error) {
	var msg struct {
		Tags []string `json:"tags"`
	}
	if err := json.Unmarshal(d.Body, &msg); err != nil {
		return nil, err
	}
	return &AMQPMessage{
		Offset:  offset,
		Tags:    msg.Tags,
		Payload: d.Body,
	}, nil
}

func Test_AMQP_History(t *testing.T) {
	url := "amqp://guest:guest@rabbit.stream.local:5672/"
	queue := "test"

	stream := spubtream.NewStream[*AMQPMessage]()

	var lastOffset int64
	if err := initStream(url, queue, func(offset int64, d amqp.Delivery) {
		msg, err := AMQPMessageFromDelivery(offset, d)
		if err != nil {
			// do some
			return
		}
		lastOffset = offset
		stream.Pub(msg)
	}); err != nil {
		t.Fatal(err)
	}

	// now we can accept subscribers
	{
		// pos := stream.Newest()

		pos, err := stream.After(func(msg *AMQPMessage) int {
			return int(msg.Offset - 2773)
		})
		if err != nil {
			t.Fatal(err)
		}

		stream.Sub(spubtream.ReceiverFunc[*AMQPMessage](func(msg *AMQPMessage) error {
			fmt.Printf(">>>%#v\n", msg)
			return nil
		}), pos, []string{"one"})
	}

	fmt.Println(lastOffset)

	go publish(url, queue)

	if err := consume(url, queue, lastOffset+1, func(offset int64, d amqp.Delivery) {
		msg, err := AMQPMessageFromDelivery(offset, d)
		if err != nil {
			// do some
			return
		}
		stream.Pub(msg)
	}); err != nil {
		t.Fatal(err)
	}

	//{
	//	for i := 0; i < 3; i++ {
	//		if err := ch.Publish("", "test", true, false, amqp.Publishing{Body: []byte("payload")}); err != nil {
	//			t.Fatal(err)
	//		}
	//		time.Sleep(time.Millisecond * 10)
	//	}
	//
	//	return
	//}

	time.Sleep(time.Hour)
}

func publish(url, stream string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	for {
		if err := ch.Publish("", stream, true, false, amqp.Publishing{
			Timestamp: time.Now(),
			Body:      []byte(`{"tags": ["one"]}`),
		}); err != nil {
			return err
		}
		time.Sleep(time.Second)
	}
}

func consume(url, stream string, offset int64, fn func(int64, amqp.Delivery)) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}

	delivery, err := ch.Consume(stream, "", false, false, false, false, amqp.Table{
		"x-stream-offset": offset,
	})
	if err != nil {
		return err
	}

	for d := range delivery {
		_ = d.Ack(false)
		fn(d.Headers["x-stream-offset"].(int64), d)
	}

	return nil
}

func initStream(url, stream string, history func(int64, amqp.Delivery)) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	queue, err := ch.QueueDeclare(stream, true, false, false, false, amqp.Table{
		"x-queue-type":                    "stream",
		"x-max-age":                       "60s",
		"x-stream-max-segment-size-bytes": 1024 * 10,
		"x-queue-leader-locator":          "least-leaders", // just for comp
	})
	if err != nil {
		return err
	}
	_ = queue

	//if queue.Messages == 0 {
	//	// publish initial message
	//	if err := ch.Publish("", stream, true, false, amqp.Publishing{
	//		Timestamp: time.Now(),
	//	}); err != nil {
	//		return err
	//	}
	//}

	lastOffset, err := getLastXStreamOffset(conn, stream)
	if err != nil {
		return err
	}

	slog.Info("getLastXStreamOffset", "lastOffset", lastOffset)

	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}
	delivery, err := ch.Consume(stream, "", false, false, false, false, amqp.Table{
		"x-stream-offset": "first",
	})
	if err != nil {
		return err
	}

	start := time.Now()
	for d := range delivery {
		_ = d.Ack(false)
		offset := d.Headers["x-stream-offset"].(int64)
		if start.Sub(d.Timestamp) < time.Minute {
			history(offset, d)
		}
		if offset == lastOffset {
			break
		}
	}

	return nil
}

func getLastXStreamOffset(conn *amqp.Connection, stream string) (int64, error) {
	ch, err := conn.Channel()
	if err != nil {
		return 0, err
	}
	defer ch.Close()

	if err := ch.Qos(1, 0, false); err != nil {
		return 0, err
	}

	delivery, err := ch.Consume(stream, "", false, false, false, false, amqp.Table{
		"x-stream-offset": "last",
	})
	if err != nil {
		return 0, err
	}

	d := <-delivery

	return d.Headers["x-stream-offset"].(int64), nil
}
