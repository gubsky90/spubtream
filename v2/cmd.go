package spubtream

type Pub[M any] struct {
	msg  M
	tags []string
}
