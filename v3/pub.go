package spubtream

import (
	"slices"
	"sync/atomic"
)

func (stream *Stream[R, M]) Pub(msg M, tags ...string) {
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
	msgID := stream.messages.Add(msg, 1)
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
	atomic.AddInt64(&stream.stats.Published, 1)
	stream.messages.Used(msgID, used-1)
}

//func (stream *Stream[R, M]) cleanup() {
//	stream.mx.Lock()
//	defer stream.mx.Unlock()
//
//	drop, dropOffset := stream.messages.GetDrop()
//	if drop < 5000 {
//		return
//	}
//
//	fmt.Println("cleanup", drop, dropOffset)
//
//	for tag, key := range stream.tags {
//		if !stream.keyHasSub(key) {
//			delete(stream.tags, tag)
//			continue
//		}
//
//		stream.keyMsgLocks[key.lockSlot].Lock()
//		if len(key.msgIDs) > 0 && key.msgIDs[0] <= dropOffset {
//			i, _ := slices.BinarySearch(key.msgIDs, dropOffset) // TODO: check if dropOffset not found
//			key.msgIDs = key.msgIDs[:copy(key.msgIDs, key.msgIDs[i:])]
//		}
//		stream.keyMsgLocks[key.lockSlot].Unlock()
//	}
//
//	stream.messages.Drop(drop)
//}
