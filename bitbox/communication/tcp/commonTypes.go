package tcp

import "github.com/lrondanini/bit-box/bitbox/cluster/actions"

type MessageType rune

const (
	Request  MessageType = 'Q'
	Response MessageType = 'A'
)

type Frame struct {
	FromNodeId  string //id = ip:port
	MessageType MessageType
	Action      actions.Action
	Error       bool
	Body        string
}

type MessageFromCluster struct {
	Frame          Frame
	ReplyToChannel chan Frame
}
