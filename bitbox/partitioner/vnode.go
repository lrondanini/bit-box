package partitioner

type VNode struct {
	StartToken uint64 `json:"startToken"` //included in this vnode ring
	EndToken   uint64 `json:"endToken"`   //included in this vnode ring
	NodeId     string `json:"nodeId"`
	NodeIp     string `json:"nodeIp"`
	NodePort   string `json:"nodePort"`
}

func InitVNode(startToken uint64, endToken uint64, nodeId string, nodeIp string, nodePort string) *VNode {
	return &VNode{
		StartToken: startToken,
		EndToken:   endToken,
		NodeId:     nodeId,
		NodeIp:     nodeIp,
		NodePort:   nodePort,
	}
}
