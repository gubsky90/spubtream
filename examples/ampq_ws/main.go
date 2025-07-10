package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/gobwas/ws/wsutil"
	"github.com/gubsky90/spubtream/v2"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()

	stream := spubtream.NewStream[[]byte, net.Conn]()
	stream.StartDynamicPool(func(msg []byte, conn net.Conn) {
		_ = wsutil.WriteServerText(conn, msg)
	})

	//if err := consume(ctx, &wg, "amqp://guest:guest@127.0.0.1:5672/", "ws.notifications", func(delivery amqp.Delivery) {
	//	var body struct {
	//		Tags []string `json:"tags"`
	//	}
	//	if err := json.Unmarshal(delivery.Body, &body); err != nil {
	//		slog.Error("json.Unmarshal", "body", string(delivery.Body), "err", err)
	//		return
	//	}
	//	slog.Info("Pub", "msg", string(delivery.Body))
	//	_ = stream.Pub(ctx, delivery.Body, body.Tags...)
	//}); err != nil {
	//	log.Fatal(err)
	//}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		bytes, _ := os.ReadFile("index.html")
		w.Write(bytes)
	})
	mux.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Tags    []string `json:"tags"`
			Payload string   `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = stream.Pub(r.Context(), []byte(body.Payload), body.Tags...)
	})
	mux.Handle("/ws", WS(stream))

	if err := ListenAndServe(ctx, &wg, ":9010", mux); err != nil {
		return err
	}

	return nil
}
