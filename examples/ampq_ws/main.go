package main

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/gubsky90/spubtream"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Client struct {
	conn net.Conn
}

func (c *Client) Receive(_ context.Context, message *Message) error {
	return wsutil.WriteServerText(c.conn, message.Payload)
}

type Message struct {
	Tags    []string
	Payload []byte
}

func (m *Message) MessageTags() []string {
	return m.Tags
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	defer wg.Wait()

	stream := spubtream.New[*Message](ctx).Stream()

	if err := consume(ctx, &wg, "amqp://guest:guest@127.0.0.1:5672/", "ws.notifications", func(delivery amqp.Delivery) {
		var body struct {
			Tags []string `json:"tags"`
		}
		if err := json.Unmarshal(delivery.Body, &body); err != nil {
			slog.Error("json.Unmarshal", "body", string(delivery.Body), "err", err)
			return
		}
		msg := &Message{
			Tags:    body.Tags,
			Payload: delivery.Body,
		}
		slog.Info("Pub", "msg", string(delivery.Body))
		stream.Pub(msg)
	}); err != nil {
		log.Fatal(err)
	}

	server := &http.Server{
		Addr: ":9010",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, _, _, err := ws.UpgradeHTTP(r, w)
			if err != nil {
				return
			}

			tags, err := auth(conn)
			if err != nil {
				slog.Debug("auth failed", "err", err)
				_ = conn.Close()
				return
			}

			stream.Sub(&Client{
				conn: conn,
			}, stream.Newest(), tags)
		}),
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

func auth(conn net.Conn) ([]string, error) {
	msg, _, err := wsutil.ReadClientData(conn)
	if err != nil {
		return nil, err
	}

	var authMsg struct {
		Token string `json:"token"` // sub tags here (tag1,tag2,...)
	}
	if err := json.Unmarshal(msg, &authMsg); err != nil {
		return nil, err
	}

	return strings.Split(authMsg.Token, ","), nil
}

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
