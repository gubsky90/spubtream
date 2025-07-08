package way

type Pub[M any] struct {
	msg  M
	tags []string
}
