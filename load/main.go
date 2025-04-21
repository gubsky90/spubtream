package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gubsky90/spubtream"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	_ "net/http/pprof"
)

type TestMessage struct {
	ID      int
	Time    time.Time
	Tags    []string
	Payload int
}

func (t *TestMessage) MessageTags() []string {
	return t.Tags
}

func main() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":9100", nil))
	}()

	stream := spubtream.New[*TestMessage](context.Background()).
		WithGCInterval(time.Millisecond * 1000).
		WithWorkersLimit(1024 * 10).
		WithBufferSizeLimit(10000).
		Stream()

	var received int64

	ts := time.Now()
	for i := 0; i < 1000000; i++ {
		stream.SubFunc(func(_ context.Context, msg *TestMessage) error {
			atomic.AddInt64(&received, 1)
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			return nil
		}, stream.Newest(),
			"all",
			fmt.Sprintf("role#%d", i%10),
			fmt.Sprintf("user#%d", i%100000),
			// fmt.Sprintf("conn#%d", i),
		)
	}
	fmt.Println("Sub done", time.Since(ts))

	var tags []string
	tags = append(tags, "all")
	for i := 0; i < 10; i++ {
		tags = append(tags, fmt.Sprintf("role#%d", i))
	}
	for i := 0; i < 100000; i++ {
		tags = append(tags, fmt.Sprintf("user#%d", i))
	}
	//for i := 0; i < 1000000; i++ {
	//	tags = append(tags, fmt.Sprintf("conn#%d", i))
	//}

	messages := make([]*TestMessage, len(tags))
	for i, tag := range tags {
		messages[i] = &TestMessage{Tags: []string{tag}}
	}

	var pubs int64
	go func() {
		p := 0
		ctx := context.Background()
		for {
			p++
			atomic.AddInt64(&pubs, 1)
			_ = stream.Pub(ctx, messages[p%len(messages)])
			if p%len(messages) == 0 {
				time.Sleep(time.Second * 30)
			}
		}
	}()

	for i := 0; i < 10000; i++ {
		time.Sleep(time.Second)
		fmt.Println(
			"pubs", atomic.SwapInt64(&pubs, 0),
			"received", atomic.SwapInt64(&received, 0),
		)
	}
}
