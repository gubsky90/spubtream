package way

import (
	"context"
	"fmt"
)

type Position = int

type ReceiverFunc[T Message] func(ctx context.Context, message T) error

func (fn ReceiverFunc[T]) Receive(ctx context.Context, message T) error {
	return fn(ctx, message)
}

func (stream *Stream[T]) SubFunc(receiver ReceiverFunc[T], pos Position, tags ...string) *Subscription[T] {
	return stream.Sub(receiver, pos, tags...)
}

func (stream *Stream[T]) Newest() Position {
	return 0
}

func (sub *Subscription[T]) String() string {
	var next string
	if sub.next == nil {
		next = "<nil>"
	} else {
		next = fmt.Sprint(sub.next.receiver)
	}
	return fmt.Sprintf("%v (offset: %d; next: %s)", sub.receiver, sub.offset, next)
}
