package spubtream

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

var receiveCount int64

type TestMessage struct {
	ID      int
	Time    time.Time
	Tags    []string
	Payload int
}

func (t *TestMessage) MessageTags() []string {
	return t.Tags
}

type TestClient struct {
	ID    int
	Delay time.Duration
}

func (c *TestClient) Receive(_ context.Context, msg *TestMessage) error {
	fmt.Printf("[Receive][%d] %v\n", c.ID, msg)
	atomic.AddInt64(&receiveCount, 1)
	if c.Delay != 0 {
		time.Sleep(c.Delay)
	}

	return nil
}

func TestName(t *testing.T) {
	s := New[*TestMessage](context.Background()).Stream()

	fmt.Println("sub start")
	for i := 0; i < 100000; i++ {
		s.Sub(&TestClient{ID: i}, s.Newest(), []string{fmt.Sprintf("tag#%d", i%100000), "all"})
	}
	fmt.Println("sub done")

	time.Sleep(time.Second)

	for {
		s.Pub(&TestMessage{Tags: []string{"tag#1"}})
		s.Pub(&TestMessage{Tags: []string{"tag#2"}})
		s.Pub(&TestMessage{Tags: []string{"tag#3"}})

		// start := time.Now()
		s.wg.Wait()
		// fmt.Println("Done", receiveCount, time.Since(start))
		receiveCount = 0
		time.Sleep(time.Millisecond * 10)
	}

}

func Test_Example(t *testing.T) {
	s := New[*TestMessage](context.Background()).Stream()

	s.Sub(&TestClient{ID: 1}, s.Newest(), []string{"one"})
	s.Sub(&TestClient{ID: 2}, s.Newest(), []string{"two", "all"})

	s.Pub(&TestMessage{Tags: []string{"one", "all"}})
	s.Pub(&TestMessage{Tags: []string{"one"}})

	s.wg.Wait()

	fmt.Println(receiveCount)
}

func Test_Stream_SubAfter(t *testing.T) {
	stream := New[*TestMessage](context.Background()).Stream()

	pos := stream.Newest()
	fmt.Println(pos)

	stream.Pub(&TestMessage{ID: 1})
	stream.Pub(&TestMessage{ID: 2})
	stream.Pub(&TestMessage{ID: 3})
	stream.Pub(&TestMessage{ID: 4})
	stream.Pub(&TestMessage{ID: 5})

	pos, err := stream.After(func(msg *TestMessage) int {
		return msg.ID - 0
	})
	if err != nil {
		t.Fatal(err)
	}

	stream.Sub(&TestClient{}, pos, nil)
}

func Test_SizeOf(t *testing.T) {

	type Subscription[T Message] struct {
		receiver Receiver[T]
		tags     []string
		pos      int32
		status   uint8
	}

	fmt.Println(unsafe.Sizeof(Subscription[*TestMessage]{}))
}

func Test_Stream_gc(t *testing.T) {
	stream := New[*TestMessage](context.Background()).Stream()
	stream.offset = 10

	stream.Sub(&TestClient{
		ID:    1,
		Delay: time.Millisecond,
	}, stream.Newest(), []string{"one"})

	stream.Sub(&TestClient{
		ID:    2,
		Delay: time.Second * 2,
	}, stream.Newest(), []string{"one"})

	stream.Sub(&TestClient{
		ID:    3,
		Delay: time.Millisecond,
	}, stream.Newest(), []string{"two"})

	for i := 0; i < 5; i++ {
		stream.Pub(&TestMessage{ID: i + 1, Tags: []string{"one"}})
		time.Sleep(time.Second)
	}

	time.Sleep(time.Hour)
	// last 3s
	//stream.gc(func(messages []*TestMessage) int {
	//	now := time.Now()
	//	return sort.Search(len(messages), func(i int) bool {
	//		return int((3*time.Second)-now.Sub(messages[i].Time)) >= 0
	//	})
	//})
}

func Benchmark_nextPos(b *testing.B) {
	s := New[*TestMessage](context.Background()).Stream()
	for i := 0; i < 100000; i++ {
		if i%20000 == 0 {
			s.Pub(&TestMessage{Tags: []string{"one", "two"}})
		} else {
			s.Pub(&TestMessage{Tags: []string{"one"}})
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pos := 0
		for pos < len(s.messages) {
			pos, _ = s.nextPos([]string{"two"}, pos)
		}
	}
}
