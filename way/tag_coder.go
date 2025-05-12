package way

import "sync"

var (
	tagcodermx   sync.Mutex
	tagcoderdata = map[string]int{}
)

func Encode(tag string) int {
	tagcodermx.Lock()
	defer tagcodermx.Unlock()
	return encode(tag)
}

func encode(tag string) int {
	n, ok := tagcoderdata[tag]
	if !ok {
		n = len(tagcoderdata)
		tagcoderdata[tag] = n
	}
	return n
}

func EncodeAll(tags ...string) []int {
	if len(tags) == 0 {
		return nil
	}

	tagcodermx.Lock()
	defer tagcodermx.Unlock()
	ns := make([]int, len(tags))
	for i, tag := range tags {
		ns[i] = encode(tag)
	}
	return ns
}
