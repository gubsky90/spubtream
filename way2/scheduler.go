package way

import (
	"fmt"
	"log/slog"
	"sort"
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
		"name", name,
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
			stream.gc("timer", 5000)
			if len(stream.messages) < messagesLimit {
				pub = stream.pub
			}
		case resub := <-stream.resub:
			stream.handleReSub(resub.receiver, resub.add, resub.remove)
		case receiver := <-stream.unsub:
			stream.handleUnSub(receiver)
		case sub := <-stream.sub:
			offset, err := sub.pos(stream.messages)
			sub.done <- err
			if err == nil {
				stream.stats.Subscriptions++
				if stream.handleSub(sub.receiver, offset, sub.tagIDs) && process == nil {
					selectTask()
				}
			}
		case msg := <-pub:
			stream.stats.Published++
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
			// handle task.err

			stream.stats.Received++

			// stream.used[task.sub.offset-stream.offset]--

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
