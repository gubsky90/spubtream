package examples

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gubsky90/spubtream"
)

type TCPMessage struct {
	Tags []string `json:"tags"`
}

func (msg *TCPMessage) MessageTags() []string {
	return msg.Tags
}

type TCPClient struct {
	conn   net.Conn
	stream *spubtream.Stream[*TCPMessage]
}

func (c *TCPClient) Receive(_ context.Context, msg *TCPMessage) error {
	return json.NewEncoder(c.conn).Encode(msg)
}

func (c *TCPClient) Reader() {
	sub := c.stream.Sub(c, c.stream.Newest())

	scanner := bufio.NewScanner(c.conn)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println(">>>", line)
		switch {
		case strings.HasPrefix(line, "sub "):
			add := strings.Fields(strings.TrimPrefix(line, "sub "))
			c.stream.ReSub(sub, add, nil)
		case strings.HasPrefix(line, "unsub "):
			remove := strings.Fields(strings.TrimPrefix(line, "unsub "))
			c.stream.ReSub(sub, nil, remove)
		default:
			_, _ = c.conn.Write([]byte("bad cmd\n"))
		}
	}
	fmt.Println("closed", scanner.Err())
}

func Test_Example_TCP(t *testing.T) {
	ctx := context.Background()
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	stream := spubtream.New[*TCPMessage](context.Background()).Stream()

	go func() {
		for {
			time.Sleep(time.Second)
			_ = stream.Pub(ctx, &TCPMessage{Tags: []string{"one"}})
			_ = stream.Pub(ctx, &TCPMessage{Tags: []string{"two"}})
			_ = stream.Pub(ctx, &TCPMessage{Tags: []string{"three"}})
		}
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			t.Fatal(err)
		}
		client := &TCPClient{
			conn:   conn,
			stream: stream,
		}
		go client.Reader()
	}
}
