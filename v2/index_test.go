package spubtream

import (
	"fmt"
	"testing"
)

func Test_Index(t *testing.T) {
	index := NewIndex[string]()

	index.addReceiver(1, "one")
	index.addReceiver(1, "two")

	index.rangeReceivers(1, func(v string) {
		fmt.Println(v)
	})

	index.deleteReceiver(1, "one")
	index.deleteReceiver(1, "two")

	fmt.Println()
	index.rangeReceivers(1, func(v string) {
		fmt.Println(v)
	})
}
