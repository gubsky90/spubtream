package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	_ "net/http/pprof"

	"github.com/gubsky90/spubtream/v6"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

type Consumer struct {
}

type Client struct {
	ID int
}

func (c *Consumer) OnMessage(client any, msg *TestMessage) {
	// time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
}

func main() {
	//runtime.SetBlockProfileRate(1)
	//runtime.SetMutexProfileFraction(1)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":9100", nil))
	}()

	consumer := &Consumer{}

	stream := spubtream.NewStream[any, *TestMessage]()
	stream.Start(consumer.OnMessage)
	go metrics(stream.Stats)

	ts := time.Now()
	for i := 0; i < 1000000; i++ {

		stream.Sub(&Client{},
			"all",
			fmt.Sprintf("role#%d", i%10),
			fmt.Sprintf("user#%d", i%100000),
			fmt.Sprintf("conn#%d", i),
		)
	}
	fmt.Println("Sub done", time.Since(ts))

	// time.Sleep(time.Hour)

	var tags []string
	// tags = append(tags, "all")
	//for i := 0; i < 10; i++ {
	//	tags = append(tags, fmt.Sprintf("role#%d", i))
	//}
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

	//messages := []*TestMessage{
	//	{Tags: []string{"role#0", "role#1", "role#2"}},
	//	{Tags: []string{"role#2", "role#3", "role#4"}},
	//	{Tags: []string{"role#5", "role#6", "role#7"}},
	//	{Tags: []string{"role#8", "role#9"}},
	//}

	go func() {
		p := 0
		for {
			p++
			msg := messages[p%len(messages)]
			// time.Sleep(time.Millisecond * 100)
			stream.Pub(msg, msg.Tags...)
			//time.Sleep(time.Second * 10)
			//if p%1000 == 0 {
			//	time.Sleep(time.Second * 10)
			//}
		}
	}()

	time.Sleep(time.Hour)
}

func metrics(fn func() spubtream.Stats) {
	var prev spubtream.Stats
	for {
		time.Sleep(time.Second * 2)
		next := fn()

		fmt.Println(
			"Messages", next.Messages,
			"Received", (next.Received-prev.Received)/2,
			"Published", (next.Published-prev.Published)/2,
		)

		prev = next
	}
}

func _metrics(fn func() spubtream.Stats) {
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
	selected := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "spubtream_selected",
	})

	prometheus.DefaultRegisterer.MustRegister(
		messages,
		subscriptions,
		published,
		received,
		selected,
	)

	for {
		time.Sleep(time.Second)
		stats := fn()

		fmt.Printf("%#v\n", stats)

		messages.Set(float64(stats.Messages))
		subscriptions.Set(float64(stats.Subscriptions))
		published.Set(float64(stats.Published))
		received.Set(float64(stats.Received))
	}
}
