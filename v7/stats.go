package spubtream

import "sync/atomic"

type Stats struct {
	Messages      int64
	Subscriptions int64
	Published     int64
	Received      int64
}

func (stream *Stream[M, R, T]) Stats() Stats {
	return Stats{
		Messages:      atomic.LoadInt64(&stream.stats.Messages),
		Subscriptions: atomic.LoadInt64(&stream.stats.Subscriptions),
		Published:     atomic.LoadInt64(&stream.stats.Published),
		Received:      atomic.LoadInt64(&stream.stats.Received),
	}
}
