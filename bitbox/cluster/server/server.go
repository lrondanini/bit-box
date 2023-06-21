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

package server

import (
	"github.com/lrondanini/bit-box/bitbox/cluster/partitioner"
	"github.com/lrondanini/bit-box/bitbox/cluster/server/serverStatus"
	"github.com/lrondanini/bit-box/bitbox/cluster/server/serverStatus/heartBitStatus"
)

type Server struct {
	NodeId                  string                        `json:"nodeId"`
	NodeIp                  string                        `json:"nodeIp"`
	NodePort                string                        `json:"nodePort"`
	NodeHeartbitPort        string                        `json:"nodeHeartbitPort"`
	Status                  serverStatus.ServerStatus     `json:"Status"`
	HeartbitStatus          heartBitStatus.HeartBitStatus `json:"heartbitStatus"`
	PartitionTableTimestamp int64                         `json:"partitionTable"`
	Memory                  string                        `json:"memory"` //USED/AVAILABLE/TOTAL
	CPU                     string                        `json:"cpu"`
	NumbOfVNodes            int                           `json:"numbOfVNodes"` //number of vnodes assigned to this server
}

func InitServerList(pt *partitioner.PartitionTable) map[string]Server {
	res := make(map[string]Server)

	for _, v := range pt.VNodes {
		res[v.NodeId] = Server{
			NodeId:                  v.NodeId,
			NodeIp:                  v.NodeIp,
			NodePort:                v.NodePort,
			NodeHeartbitPort:        "",
			Status:                  serverStatus.Joining,
			HeartbitStatus:          heartBitStatus.None,
			PartitionTableTimestamp: pt.Timestamp,
			Memory:                  "",
			CPU:                     "",
			NumbOfVNodes:            0,
		}
	}

	return res
}
