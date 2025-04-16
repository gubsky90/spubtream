package examples

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/gubsky90/spubtream"
	"github.com/redis/go-redis/v9"
)

type RedisMessage struct {
	Tags    []string
	Payload []byte
}

func (msg *RedisMessage) MessageTags() []string {
	return msg.Tags
}

func RedisMessageFromXMessage(msg redis.XMessage) (*RedisMessage, error) {
	var tmp struct {
		Tags []string `json:"tags"`
	}
	payload := []byte(msg.Values["payload"].(string))
	if err := json.Unmarshal(payload, &tmp); err != nil {
		return nil, err
	}
	return &RedisMessage{
		Tags:    tmp.Tags,
		Payload: payload,
	}, nil
}

func Test_Redis(t *testing.T) {
	redisStreamName := "test"

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "redis.stream.local:6379"})

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	fmt.Println(client.XLen(ctx, redisStreamName).Result())

	//id, err := client.XAdd(ctx, &redis.XAddArgs{
	//	Stream: redisStreamName,
	//	Values: []string{"payload", `{"tags": ["one"], "data": 123}`},
	//}).Result()
	//fmt.Println(id, err)

	// 1744728122852-0

	stream := spubtream.NewStream[*RedisMessage]()
	stream.Sub(spubtream.ReceiverFunc[*RedisMessage](func(msg *RedisMessage) error {
		fmt.Println(">>>", string(msg.Payload))
		return nil
	}), stream.Newest(), []string{"one"})

	// -----------------
	// Read history
	// -----------------

	from := fmt.Sprintf("%d-0", time.Now().Add(-time.Minute*50).UnixMilli())
	// use client.XRangeN()
	msgs, err := client.XRange(ctx, redisStreamName, from, "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, xmsg := range msgs {
		msg, _ := RedisMessageFromXMessage(xmsg)
		stream.Pub(msg)
	}

	// -----------------
	// Consume
	// -----------------

	_ = consumeRedis(ctx, client, redisStreamName, "0", func(xmsg redis.XMessage) {
		msg, _ := RedisMessageFromXMessage(xmsg)
		stream.Pub(msg)
	})

	time.Sleep(time.Minute)
}

func consumeRedis(ctx context.Context, client *redis.Client, stream, from string, fn func(redis.XMessage)) error {
	for {
		cmd := client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{stream},
			ID:      from,
		})
		if err := cmd.Err(); err != nil {
			return err
		}
		for _, msg := range cmd.Val()[0].Messages {
			fn(msg)
			from = msg.ID
		}
	}
}
