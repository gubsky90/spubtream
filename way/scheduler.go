package way

import "context"

func (stream *Stream[T]) processWorker(ctx context.Context) {
	defer stream.wg.Done()
	defer close(stream.process)
	for {
		stream.mx.Lock()
		task, ok := stream.selectTask()
		stream.mx.Unlock()

		if ok {
			select {
			case <-ctx.Done():
				return
			case stream.process <- task:
				continue
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-stream.signal:
			continue
		}
	}
}

func (stream *Stream[T]) doneWorker() {
	defer stream.wg.Done()
	for task := range stream.done {
		// handle task.err

		stream.mx.Lock()
		if task.sub.tagIDs == nil { // unsubscribed
			task.sub.receiver = nil
			task.sub.next = nil
		} else {
			stream.reQ(task.sub)
		}
		stream.mx.Unlock()
	}
}
