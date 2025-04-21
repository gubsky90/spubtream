package spubtream

import (
	"context"
	"sort"
	"time"
)

type Config[T Message] struct {
	stream     *Stream[T]
	gcInterval time.Duration
	gcFunc     func([]T) int
}

func (cfg *Config[T]) WithWorkersLimit(limit int) *Config[T] {
	cfg.stream.workersLimit = limit
	return cfg
}

func (cfg *Config[T]) WithWaitForLaggards(wait bool) *Config[T] {
	cfg.stream.waitForLaggards = wait
	return cfg
}

func (cfg *Config[T]) WithTTL(ttl time.Duration, fn func(T) time.Time) *Config[T] {
	cfg.gcFunc = func(messages []T) int {
		now := time.Now()
		return sort.Search(len(messages), func(i int) bool {
			return int(ttl-now.Sub(fn(messages[i]))) >= 0
		})
	}
	return cfg
}

func (cfg *Config[T]) WithLimit(limit int) *Config[T] {
	cfg.gcFunc = func(messages []T) int {
		return len(messages) - limit
	}
	return cfg
}

func (cfg *Config[T]) WithGCInterval(interval time.Duration) *Config[T] {
	cfg.gcInterval = interval
	return cfg
}

func (cfg *Config[T]) WithBufferSizeLimit(limit int) *Config[T] {
	cfg.stream.bufferSizeLimit = limit
	return cfg
}

func (cfg *Config[T]) Stream() *Stream[T] {
	stream := cfg.stream
	if cfg.gcFunc != nil && cfg.gcInterval > 0 {
		stream.commonWG.Add(1)
		go func() {
			defer stream.commonWG.Done()
			for {
				time.Sleep(cfg.gcInterval)
				stream.gc(cfg.gcFunc)
			}
		}()
	}
	return cfg.stream
}

func New[T Message](ctx context.Context) *Config[T] {
	return &Config[T]{
		gcInterval: 10 * time.Second,
		gcFunc: func(messages []T) int {
			return len(messages)
		},
		stream: &Stream[T]{
			ctx:       ctx,
			tags:      map[int][]int{},
			inProcess: map[*Subscription[T]]struct{}{},
			idleSubs:  map[int][]*Subscription[T]{},

			workersLimit:    128,
			waitForLaggards: true,
			bufferSizeLimit: 10000,
		},
	}
}
