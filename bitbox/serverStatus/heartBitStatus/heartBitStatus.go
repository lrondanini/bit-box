package heartBitStatus

import "fmt"

type HeartBitStatus int

const (
	None HeartBitStatus = iota
	Alive
	Leaving
	Left
	Failed
)

func (s HeartBitStatus) String() string {
	switch s {
	case None:
		return "none"
	case Alive:
		return "alive"
	case Leaving:
		return "leaving"
	case Left:
		return "left"
	case Failed:
		return "failed"
	default:
		panic(fmt.Sprintf("unknown MemberStatus: %d", s))
	}
}
