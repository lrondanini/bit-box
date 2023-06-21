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

package tcp

import (
	"encoding/gob"
	"net"

	"github.com/lrondanini/bit-box/bitbox/cluster/actions"
)

type TcpClientsManager struct {
	currentNodeId string
}

func InitTcpClientsManager(nodeId string) *TcpClientsManager {
	return &TcpClientsManager{
		currentNodeId: nodeId,
	}
}

func (c *TcpClientsManager) SendMessage(toNodeId string, messageType MessageType, action actions.Action, body interface{}) (*Frame, error) {

	serialiazedBody, e := EncodeBody(body)
	if e != nil {
		return nil, e
	}

	m := &Frame{
		FromNodeId:  c.currentNodeId,
		MessageType: messageType,
		Action:      action,
		Body:        serialiazedBody,
	}

	tcpAddr, tcpAddrErr := net.ResolveTCPAddr("tcp", toNodeId)
	if tcpAddrErr != nil {
		return nil, tcpAddrErr
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}

	reply := &Frame{}

	encoder := gob.NewEncoder(conn)
	err = encoder.Encode(m)
	if err != nil {
		return nil, err
	} else {
		dec := gob.NewDecoder(conn)
		err = dec.Decode(reply)
		if err != nil {
			return nil, err
		}
	}

	return reply, nil
}
