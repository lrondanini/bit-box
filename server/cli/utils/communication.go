// Copyright 2023 lucarondanini
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"fmt"
	"sync"

	"github.com/lrondanini/bit-box/bitbox/cluster/actions"
	"github.com/lrondanini/bit-box/bitbox/communication"
	"github.com/lrondanini/bit-box/bitbox/communication/tcp"
)

func CreateAckFrame(nodeId string) *tcp.Frame {
	sb, _ := communication.SerialiazeBody("ack")
	return &tcp.Frame{
		FromNodeId:  nodeId,
		MessageType: tcp.Response,
		Action:      actions.NoAction,
		Error:       false,
		Body:        sb,
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
		FromNodeId:  c.NodeId,
		MessageType: tcp.Response,
		Action:      action,
		Error:       true,
		Body:        sb,
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
