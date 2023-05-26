package tcp

import (
	"encoding/gob"
	"net"

	"github.com/lrondanini/bit-box/bitbox/actions"
)

type TcpClientsManager struct {
	currentNodeId string
}

func InitTcpClientsManager(nodeId string) *TcpClientsManager {
	return &TcpClientsManager{
		currentNodeId: nodeId,
	}
}

func (c *TcpClientsManager) SendMessage(toNodeId string, messageType MessageType, streamId uint8, streamPosition uint32, action actions.Action, body interface{}) (*Frame, error) {

	serialiazedBody, e := EncodeBody(body)
	if e != nil {
		return nil, e
	}

	m := &Frame{
		FromNodeId:     c.currentNodeId,
		MessageType:    messageType,
		StreamId:       streamId,
		StreamPosition: streamPosition,
		Action:         action,
		Body:           serialiazedBody,
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
