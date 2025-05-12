package way

type State int

const (
	StateIdle State = iota
	StateProcess
)

func (state State) String() string {
	switch state {
	case StateIdle:
		return "Idle"
	case StateProcess:
		return "Process"
	default:
		panic("unexpected")
	}
}
