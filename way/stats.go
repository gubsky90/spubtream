package way

type Stats struct {
	Messages      int
	Subscriptions int
	Published     int
	Received      int
}

func (stream *Stream[T]) Stats() Stats {
	req := make(chan Stats)
	stream.requestStats <- req
	return <-req
}
