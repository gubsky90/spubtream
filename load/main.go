package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gubsky90/spubtream/way"
	"github.com/prometheus/client_golang/prometheus"
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

func metrics(fn func() way.Stats) {
	messages := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "spubtream_messages",
	})
	subscriptions := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "spubtream_subscriptions",
	})
	published := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "spubtream_published",
	})
	received := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "spubtream_received",
	})

	prometheus.DefaultRegisterer.MustRegister(
		messages,
		subscriptions,
		published,
		received,
	)

	for {
		time.Sleep(time.Second)
		stats := fn()
		messages.Set(float64(stats.Messages))
		subscriptions.Set(float64(stats.Subscriptions))
		published.Set(float64(stats.Published))
		received.Set(float64(stats.Received))
	}
}

func main() {

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":9100", nil))
	}()

	//stream := spubtream.New[*TestMessage](context.Background()).
	//	WithGCInterval(time.Millisecond * 1000).
	//	WithWorkersLimit(1024).
	//	WithBufferSizeLimit(10000).
	//	Stream()

	stream := way.NewStream[*TestMessage]()
	go metrics(stream.Stats)

	ts := time.Now()
	for i := 0; i < 1000000; i++ {
		stream.SubFunc(func(_ context.Context, msg *TestMessage) error {
			// time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
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

	go func() {
		p := 0
		ctx := context.Background()
		for {
			p++
			_ = stream.Pub(ctx, messages[p%len(messages)])
			//if p%len(messages) == 0 {
			//	time.Sleep(time.Second * 10)
			//}
		}
	}()

	time.Sleep(time.Hour)
}
