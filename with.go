package spubtream

import (
	"sort"
	"time"
)

func (s *Stream[T]) WithWaitForLaggards(wait bool) *Stream[T] {

	return s
}

func (s *Stream[T]) WithWorkersLimit(limit int) *Stream[T] {
	s.workersLimit = limit
	return s
}

func (s *Stream[T]) WithTTL(ttl time.Duration, fn func(T) time.Time) *Stream[T] {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			time.Sleep(time.Second)
			s.gc(func(messages []T) int {
				now := time.Now()
				return sort.Search(len(messages), func(i int) bool {
					return int(ttl-now.Sub(fn(messages[i]))) >= 0
				})
			})
		}
	}()
	return s
}

func (s *Stream[T]) WithLimit(limit int) *Stream[T] {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			time.Sleep(time.Second)
			s.gc(func(messages []T) int {
				return len(messages) - limit
			})
		}
	}()
	return s
}
