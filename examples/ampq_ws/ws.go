package main

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/gubsky90/spubtream/v2"
)

type Stream = spubtream.Stream[[]byte, net.Conn]

func WS(stream *Stream) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, _, _, err := ws.UpgradeHTTP(r, w)
		if err != nil {
			return
		}

		go func() {
			defer conn.Close()

			tags, err := auth(conn)
			if err != nil {
				slog.Debug("auth failed", "err", err)
				return
			}

			_ = stream.Sub(conn, stream.Last, tags...)
			defer stream.UnSub(conn)

			for {
				_, _, err := wsutil.ReadClientData(conn)
				if err != nil {
					return
				}
			}
		}()
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
