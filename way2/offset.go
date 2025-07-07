package way

type Positioner[M any] func(messages []M) (int, error)

func (stream *Stream[M, R]) Last(messages []M) (int, error) {
	return len(messages), nil
}

func (stream *Stream[M, R]) First([]M) (int, error) {
	return -1, nil
}

func (stream *Stream[M, R]) Hold(pos Positioner[M]) (Positioner[M], func(), error) {
	res := make(chan HoldResult[M])
	stream.hold <- Hold[M]{
		pos: pos,
		res: res,
	}
	r := <-res
	if r.err != nil {
		return nil, nil, r.err
	}
	return func([]M) (int, error) { return r.offset, nil }, func() { stream.release <- r.offset }, nil
}
