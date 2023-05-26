package serverStatus

import "fmt"

type ServerStatus uint8

/*
Used to monitor node's status - see heartbit for the statuses used in the cluster's heartbeat
*/
const (
	Joining ServerStatus = iota
	Decommissioning
	Alive
	Starting
)

func (s ServerStatus) String() string {
	switch s {
	case Joining:
		return "Joining"
	case Decommissioning:
		return "Decommissioning"
	case Alive:
		return "Alive"
	case Starting:
		return "Starting"
	default:
		panic(fmt.Sprintf("unknown ServerStatus: %d", s))
	}
}
