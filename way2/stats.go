package way

type Stats struct {
	Messages      int
	Subscriptions int
	Published     int
	Received      int
	Selected      int
}

func (stream *Stream[M, R]) Stats() Stats {
	req := make(chan Stats)
	stream.requestStats <- req
	return <-req
}
