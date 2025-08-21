package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/gubsky90/spubtream/v2"
	// "github.com/golang-jwt/jwt/v5"
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
	// mux.Handle("/ws", WS(stream))

	if err := ListenAndServe(ctx, &wg, ":9010", mux); err != nil {
		return err
	}

	wsl, err := net.Listen("tcp", ":9091")
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := wsl.Accept()
			if err != nil {
				slog.Warn("Accept", "err", err)
				time.Sleep(time.Second)
				continue
			}
			go handleWS(stream, conn)
		}
	}()

	<-ctx.Done()
	return nil
}

func handleWS(stream *Stream, conn net.Conn) {
	defer conn.Close()

	slog.Info("handleWS", "addr", conn.RemoteAddr())

	if _, err := (ws.Upgrader{
		OnHost: func(host []byte) error {
			fmt.Println("OnHost", string(host))
			return nil
		},
		OnHeader: func(key, value []byte) error {
			fmt.Println("OnHeader", string(key), string(value))
			return nil
		},
		OnRequest: func(uri []byte) error {
			fmt.Println("OnRequest", string(uri))
			return nil
		},
	}).Upgrade(conn); err != nil {
		slog.Error("Upgrade", "err", err)
		return
	}

	tags, err := auth(conn)
	if err != nil {
		slog.Debug("auth", "err", err)
		_ = wsutil.WriteServerMessage(conn, ws.OpClose, ws.NewCloseFrameBody(ws.StatusPolicyViolation, "not auth"))
		return
	}

	fmt.Println(tags)

	_ = stream.Sub(conn, stream.Last, tags...)
	defer stream.UnSub(conn)

	for {
		// _ = conn.SetReadDeadline(time.Now().Add(time.Second * 2))
		data, opCode, err := wsutil.ReadClientData(conn)
		fmt.Println(string(data), opCode, err)
		fmt.Printf("%#v\n", opCode)
		fmt.Printf("%#v\n", err)
		if err != nil {
			return
		}
	}
}
