package spubtream

import (
	"fmt"
	"log/slog"
	"sort"
	"sync/atomic"
	"time"
)

func (stream *Stream[M, R]) gc(name string, minDrop int) {
	if len(stream.messages) == 0 {
		return
	}

	var unused int
	for _, count := range stream.used {
		if count > 0 {
			break
		}
		unused++
	}

	drop := min(unused, len(stream.messages))
	if drop < minDrop {
		return
	}

	dropOffset := stream.offset + drop

	stream.index.rangeItems(func(tagID int, item *IndexItem[R]) {
		if len(item.msgIDs) == 0 {
			return
		}
		if item.msgIDs[0] > dropOffset {
			return
		}
		i := sort.SearchInts(item.msgIDs, dropOffset)
		item.msgIDs = item.msgIDs[:copy(item.msgIDs, item.msgIDs[i:])]
	})

	n := copy(stream.messages, stream.messages[drop:])
	_ = copy(stream.used, stream.used[drop:])
	clear(stream.messages[n:])
	stream.messages = stream.messages[:n]
	stream.used = stream.used[:n]

	stream.offset += drop

	slog.Info("[GC]",
		"name", name,
		"Received", stream.stats.Received,
		"receivers", len(stream.receivers),
		"messages", fmt.Sprintf("[%d:%d]", len(stream.messages), cap(stream.messages)),
		"used", fmt.Sprintf("[%d:%d]", len(stream.used), cap(stream.used)),
		"offset", stream.offset,
		"drop", drop,
		"unused", unused,
		// "waitForLaggards", s.waitForLaggards,

		//"inProcess", len(s.inProcess),
		//"readyq", s.readyq.Stats(),
		//"tags", infoMapSlice(s.tags),
		//"idleSubs", s.idleSubs.Stats(),
		// "idleSubs", infoMapSlice(s.idleSubs),
	)
}

func (stream *Stream[M, R]) chanWorker() {
	var process chan Task[M, R]
	var readyTask Task[M, R]

	selectTask := func() {
		var ok bool
		if readyTask, ok = stream.selectTask(); ok {
			atomic.AddInt64(&stream.stats.Selected, 1)
			process = stream.process
		} else {
			process = nil
		}
	}

	gc := time.NewTicker(time.Second)
	defer gc.Stop()

	messagesLimit := 100000
	pub := stream.pub

	for {
		select {
		case <-gc.C:
			stream.gc("timer", 5000)
			if len(stream.messages) < messagesLimit {
				pub = stream.pub
			}

		case <-stream.lock:
			if <-stream.unlock && process == nil {
				selectTask()
			}

		case msg := <-pub:
			atomic.AddInt64(&stream.stats.Published, 1)
			if stream.handlePub(msg.msg, msg.tags) && process == nil {
				selectTask()
			}
			if len(stream.messages) == messagesLimit {
				stream.gc("pub", 500)
			}
			if len(stream.messages) == messagesLimit {
				pub = nil
			}

		case task := <-stream.done:
			atomic.AddInt64(&stream.stats.Received, 1)

			// stream.used[task.sub.offset-stream.offset]--

			if task.sub.unsubscribed() {
				//*task.sub = Subscription[R]{}
				//task = Task[M, R]{}
			} else if stream.reQ(task.receiver, task.sub) && process == nil {
				selectTask()
			}

		case process <- readyTask:
			selectTask()
		}

		atomic.StoreInt64(&stream.stats.Messages, int64(len(stream.messages)))
	}
}
