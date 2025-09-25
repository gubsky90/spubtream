package spubtream

import (
	"fmt"
	"slices"
	"sync/atomic"
)

func (stream *Stream[R, M]) Pub(msg M, tags ...string) {
	atomic.AddInt64(&stream.stats.Published, 1)
	if len(tags) == 0 {
		return
	}

	tags = slices.Compact(tags)

	stream.mx.Lock()
	defer stream.mx.Unlock()
	defer func() {
		stream.tmpKeys = stream.tmpKeys[:0]
	}()

	for _, tag := range tags {
		if key := stream.tags[tag]; key != nil && key.head != nil {
			stream.tmpKeys = append(stream.tmpKeys, key)
		}
	}

	if len(stream.tmpKeys) == 0 {
		return
	}

	var used int
	msgID, l := stream.messages.Add(msg, 1)

	atomic.StoreInt64(&stream.stats.Messages, l)

	for _, key := range stream.tmpKeys {
		stream.keyMsgLocks[key.lockSlot].Lock()
		key.msgIDs = append(key.msgIDs, msgID)
		stream.keyMsgLocks[key.lockSlot].Unlock()

		for cur := key.head; cur != nil; cur = cur.next {
			if cur.sub.offset.CompareAndSwap(-1, msgID) {
				used++
				stream.in <- cur.sub
			}
		}
	}

	stream.messages.Used(msgID, used-1)
}

func (stream *Stream[R, M]) cleanup() {
	dropOffset := stream.messages.Offset()
	fmt.Println("cleanup", dropOffset)

	stream.mx.Lock()
	for tag, key := range stream.tags {
		stream.keyMsgLocks[key.lockSlot].Lock()
		if key.head == nil {

			delete(stream.tags, tag)

		} else if len(key.msgIDs) > 0 && key.msgIDs[0] <= dropOffset {
			i, _ := slices.BinarySearch(key.msgIDs, dropOffset) // TODO: check if dropOffset not found
			key.msgIDs = key.msgIDs[:copy(key.msgIDs, key.msgIDs[i:])]
		}
		stream.keyMsgLocks[key.lockSlot].Unlock()
	}
	stream.mx.Unlock()
}
