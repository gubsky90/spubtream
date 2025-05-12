package spubtream

import (
	"fmt"
	"log/slog"
	"testing"
)

func printQ(msg string, q Q[int]) {
	var items []int
	q.Scan(func(i int) {
		items = append(items, i)
	})
	slog.Info(msg,
		"Len", q.Len(),
		"Empty", q.Empty(),
		"Scan", items,
		"items", q.items,
	)
}

func Test_Q_Enq(t *testing.T) {
	var q Q[int]
	for i := 0; i < 10; i++ {
		q.Enq(i)
		fmt.Println("Enq", q.Stats())
	}
	for i := 0; i < 10; i++ {
		fmt.Println("Deq", q.Deq(), q.Stats())
	}
	for i := 0; i < 10; i++ {
		q.Enq(i)
		fmt.Println("Enq", q.Stats())
	}
}

func Test_Q(t *testing.T) {
	var q Q[int]
	printQ("empty", q)

	q.Enq(1)
	printQ("enq 1", q)

	q.Enq(2)
	q.Enq(3)
	printQ("enq 2,3", q)

	printQ(fmt.Sprintf("deq %d", q.Deq()), q)
	printQ(fmt.Sprintf("deq %d", q.Deq()), q)

	q.Enq(4)
	q.Enq(5)
	printQ("enq 4, 5", q)

	printQ(fmt.Sprintf("deq %d", q.Deq()), q)
	printQ(fmt.Sprintf("deq %d", q.Deq()), q)
}

func Benchmark_Q(b *testing.B) {
	var q Q[int]
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		q.Enq(i)
		q.Enq(i)
		q.Deq()
		q.Deq()
	}
}
