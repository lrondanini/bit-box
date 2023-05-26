package server

import (
	"strconv"

	"github.com/lrondanini/bit-box/bitbox/partitioner"
	"github.com/lrondanini/bit-box/bitbox/serverStatus"
	"github.com/lrondanini/bit-box/bitbox/serverStatus/heartBitStatus"
)

type Server struct {
	NodeId           string                        `json:"nodeId"`
	NodeIp           string                        `json:"nodeIp"`
	NodePort         string                        `json:"nodePort"`
	NodeHeartbitPort string                        `json:"nodeHeartbitPort"`
	Status           serverStatus.ServerStatus     `json:"Status"`
	HeartbitStatus   heartBitStatus.HeartBitStatus `json:"heartbitStatus"`
	PartitionTable   string                        `json:"partitionTable"`
	Memory           string                        `json:"memory"` //USED/AVAILABLE/TOTAL
	CPU              string                        `json:"cpu"`
	NumbOfVNodes     int                           `json:"numbOfVNodes"` //number of vnodes assigned to this server
}

func InitServerList(pt *partitioner.PartitionTable) map[string]Server {
	res := make(map[string]Server)

	for _, v := range pt.VNodes {
		res[v.NodeId] = Server{
			NodeId:           v.NodeId,
			NodeIp:           v.NodeIp,
			NodePort:         v.NodePort,
			NodeHeartbitPort: "",
			Status:           serverStatus.Joining,
			HeartbitStatus:   heartBitStatus.None,
			PartitionTable:   strconv.FormatInt(pt.Timestamp, 10),
			Memory:           "",
			CPU:              "",
			NumbOfVNodes:     0,
		}
	}

	return res
}
