package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func consume(ctx context.Context, wg *sync.WaitGroup, url, queue string, fn func(amqp.Delivery)) (err error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			conn.Close()
		}
	}()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			ch.Close()
		}
	}()

	if err := ch.Qos(2, 0, false); err != nil {
		return err
	}

	deliveries, err := ch.ConsumeWithContext(ctx, queue, "test", false, false, false, false, nil)
	if err != nil {
		return err
	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		slog.Info("start consume")
		for delivery := range deliveries {
			if err := delivery.Ack(false); err != nil {
				slog.Error("Ack", "err", err)
				break
			}
			fn(delivery)
		}

		slog.Info("stop consume")

		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}

			if err := consume(ctx, wg, url, queue, fn); err != nil {
				slog.Warn("reconnect failed", "err", err)
			}

			break
		}
	}()

	return nil
}
