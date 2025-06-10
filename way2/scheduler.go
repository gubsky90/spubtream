package way

import (
	"fmt"
	"log/slog"
	"sort"
	"time"
)

func (stream *Stream[M, R]) gc(fn func(messages []M) int) {
	if len(stream.messages) == 0 {
		return
	}

	var unused int

	//if stream.waitForLaggards {
	if true {
		for _, count := range stream.used {
			if count > 0 {
				break
			}
			unused++
		}
	} else {
		unused = len(stream.messages)
	}

	drop := min(unused, fn(stream.messages))
	if drop < 512 {
		return
	}

	dropOffset := stream.offset + drop
	for tagID, indexItem := range stream.index {
		if len(indexItem.msgIDs) == 0 {
			continue
		}
		if indexItem.msgIDs[0] > dropOffset {
			continue
		}
		i := sort.SearchInts(indexItem.msgIDs, dropOffset)
		//if i == len(items) {
		//	delete(s.tags, tagID)
		//	// s.tags[tagID] = items[:0]
		//	continue
		//}
		indexItem.msgIDs = indexItem.msgIDs[:copy(indexItem.msgIDs, indexItem.msgIDs[i:])]
		stream.index[tagID] = indexItem
	}

	n := copy(stream.messages, stream.messages[drop:])
	_ = copy(stream.used, stream.used[drop:])
	clear(stream.messages[n:])
	stream.messages = stream.messages[:n]
	stream.used = stream.used[:n]

	stream.offset += drop

	slog.Info("[GC]",
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
			stream.stats.Selected++
			process = stream.process
		} else {
			process = nil
		}
	}

	gc := time.NewTicker(time.Second)
	defer gc.Stop()

	messagesLimit := 10000
	pub := stream.pub

	for {
		select {
		case req := <-stream.requestStats:
			req <- stream.stats
		case <-gc.C:
			stream.gc(func(messages []M) int {
				return len(messages)
			})

			if len(stream.messages) < messagesLimit {
				pub = stream.pub
			}

		case resub := <-stream.resub:
			stream.handleReSub(resub.receiver, resub.add, resub.remove)
		case receiver := <-stream.unsub:
			stream.handleUnSub(receiver)
		case sub := <-stream.sub:
			stream.stats.Subscriptions++
			if stream.handleSub(sub) && process == nil {
				selectTask()
			}
		case msg := <-pub:
			stream.stats.Published++
			if stream.handlePub(msg.msg, msg.tags) && process == nil {
				selectTask()
			}
			if len(stream.messages) == messagesLimit {
				pub = nil
			}
		case task := <-stream.done:
			// handle task.err

			stream.stats.Received++

			stream.used[task.sub.offset-stream.offset]--

			if task.sub.tagIDs == nil { // unsubscribed
				//*task.sub = Subscription[R]{}
				//task = Task[M, R]{}
			} else if stream.reQ(task.receiver, task.sub) && process == nil {
				selectTask()
			}
		case process <- readyTask:
			selectTask()
		}

		stream.stats.Messages = len(stream.messages)

		if onStep != nil {
			onStep()
		}
	}
}

var onStep func()
