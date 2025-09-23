package spubtream

import "testing"

func Test_Messages(t *testing.T) {
	msgs := &Messages[string]{}
	printJSON(msgs)

	msgs.Add("1", 1)
	printJSON(msgs)

	msgs.Add("2", 0)
	printJSON(msgs)

	msgs.Add("3", 0)
	printJSON(msgs)

	msgs.Add("4", 0)
	printJSON(msgs)

	msgs.Used(0, -1)

	msgs.Add("5", 0)
	printJSON(msgs)
}
