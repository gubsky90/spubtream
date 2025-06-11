package way

type Positioner[M any] func(messages []M) (int, error)

func (stream *Stream[M, R]) Last(messages []M) (int, error) {
	return len(messages), nil
}

func (stream *Stream[M, R]) First([]M) (int, error) {
	return -1, nil
}
