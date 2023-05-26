package utils

import (
	"fmt"
	"sync"

	"github.com/lrondanini/bit-box/bitbox/actions"
	"github.com/lrondanini/bit-box/bitbox/communication"
	"github.com/lrondanini/bit-box/bitbox/communication/tcp"
)

func CreateAckFrame(nodeId string) *tcp.Frame {
	sb, _ := communication.SerialiazeBody("ack")
	return &tcp.Frame{
		FromNodeId:     nodeId,
		MessageType:    tcp.Response,
		StreamId:       0,
		StreamPosition: 0,
		Action:         actions.NoAction,
		Error:          false,
		Body:           sb,
	}
}

type Cluster struct {
	NodeId      string
	CommManager *communication.CommunicationManager
	ackFrame    *tcp.Frame
	mu          sync.Mutex
}

func InitCluster(nodeId string, nodeIp string, nodePort string) *Cluster {

	c := Cluster{
		NodeId:   nodeId,
		ackFrame: CreateAckFrame(nodeId),
	}
	c.mu.Lock()
	commManager := communication.StartCommManager(nodeId, nodeIp, nodePort)
	c.mu.Unlock()

	c.CommManager = commManager

	return &c
}

func (c *Cluster) StartListening() {
	go func() {
		for {
			msg, ok := <-c.CommManager.ReceiverChannel /// msg is of type tcp.MessageFromCluster
			if ok {
				c.HandleEvent(&msg)
			} else {
				//channel closed
				return
			}

		}
	}()
}

func (c *Cluster) HandleEvent(msg *tcp.MessageFromCluster) {
	f := msg.Frame
	switch f.Action {
	case actions.AbortPartitionTableChanges:
		c.onAbortPartitionTableChanges(&f, msg.ReplyToChannel)
	}
}

func (c *Cluster) Shutdown() {
	c.CommManager.Shutdown()
}

func (c *Cluster) newErrorFrame(action actions.Action, errorMessage string) *tcp.Frame {
	sb, _ := communication.SerialiazeBody(errorMessage)
	return &tcp.Frame{
		FromNodeId:     c.NodeId,
		MessageType:    tcp.Response,
		StreamId:       0,
		StreamPosition: 0,
		Action:         action,
		Error:          true,
		Body:           sb,
	}
}

// func (c *Cluster) newFrame(action actions.Action, body string) *tcp.Frame {
// 	sb, _ := communication.SerialiazeBody(body)
// 	return &tcp.Frame{
// 		FromNodeId:     c.nodeId,
// 		MessageType:    tcp.Response,
// 		StreamId:       0,
// 		StreamPosition: 0,
// 		Action:         action,
// 		Error:          false,
// 		Body:           sb,
// 	}
// }

func (c *Cluster) onAbortPartitionTableChanges(f *tcp.Frame, replyToChannel chan tcp.Frame) {

	var message string

	err := communication.DeserializeBody(f.Body, &message)

	if err != nil {
		replyToChannel <- *c.newErrorFrame(actions.NoAction, "Could not parse request: "+err.Error())
	} else {
		fmt.Println(message)
	}

	replyToChannel <- *c.ackFrame
}
