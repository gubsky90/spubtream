package way

type Positioner[M any] func(messages []M) (int, error)

func (stream *Stream[M, R]) Last(messages []M) (int, error) {
	return len(messages), nil
}

func (stream *Stream[M, R]) First([]M) (int, error) {
	return -1, nil
}

func (stream *Stream[M, R]) Hold(pos Positioner[M]) (Positioner[M], func(), error) {
	stream.lock <- struct{}{}
	defer func() {
		stream.unlock <- false
	}()

	offset, err := pos(stream.messages)
	if err != nil {
		return nil, nil, err
	}

	stream.used[offset]++

	release := func() {
		stream.lock <- struct{}{}
		stream.used[offset]--
		stream.unlock <- false
	}

	return func([]M) (int, error) { return offset, nil }, release, nil
}
