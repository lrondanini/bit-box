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

package partitioner

type VNode struct {
	StartToken       uint64   `json:"startToken"` //included in this vnode ring
	EndToken         uint64   `json:"endToken"`   //included in this vnode ring
	NodeId           string   `json:"nodeId"`
	NodeIp           string   `json:"nodeIp"`
	NodePort         string   `json:"nodePort"`
	ReplicatedTo     []string `json:"replicatedTo"`
	VNodeID          string   `json:"vNodeId"`
	PrevNodeID       string   `json:"prevVNodeId"` //the node that previously owned this vnode
	PrevReplicatedTo []string `json:"prevReplicatedTo"`
}

func InitVNode(startToken uint64, endToken uint64, prevNodeID string, prevReplicatedTo []string, vNodeID string, nodeId string, nodeIp string, nodePort string, replicatedTo []string) *VNode {
	return &VNode{
		StartToken:       startToken,
		EndToken:         endToken,
		NodeId:           nodeId,
		NodeIp:           nodeIp,
		NodePort:         nodePort,
		ReplicatedTo:     replicatedTo,
		VNodeID:          vNodeID,
		PrevNodeID:       prevNodeID,
		PrevReplicatedTo: prevReplicatedTo,
	}
}
