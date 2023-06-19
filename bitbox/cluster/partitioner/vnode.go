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