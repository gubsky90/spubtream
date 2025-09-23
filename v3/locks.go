package spubtream

import (
	"sync/atomic"
)

type Counter uint32

func (c *Counter) Next() uint8 {
	return uint8(atomic.AddUint32((*uint32)(c), 1))
}
