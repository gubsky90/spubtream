package way

const WORKERS = 128

type ConsumerFunc[M any, R comparable] func(receiver R, msg M)

func (stream *Stream[M, R]) Start(consumerFunc ConsumerFunc[M, R]) {
	for i := 0; i < WORKERS; i++ {
		go func() {
			for task := range stream.process {
				consumerFunc(task.receiver, task.msg)
				stream.done <- task
			}
		}()
	}
}

func (stream *Stream[M, R]) Close() {

}
