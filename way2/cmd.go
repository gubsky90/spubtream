package way

type ReSub[R comparable] struct {
	receiver R
	add      []string
	remove   []string
}

type Sub[M any, R comparable] struct {
	done     chan error
	pos      Positioner[M]
	tagIDs   []int
	receiver R
}

type Pub[M any] struct {
	msg  M
	tags []string
}

type Hold[M any] struct {
	pos Positioner[M]
	res chan HoldResult[M]
}

type HoldResult[M any] struct {
	offset int
	err    error
}
