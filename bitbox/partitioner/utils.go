package partitioner

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"
)

const REPLICATION_FACTOR = 3

func GenerateNewPartitionTable(assignNumbVNodes int, nodeId string, nodeIp string, nodePort string) *[]VNode {
	var vnodes []VNode

	const max uint64 = math.MaxUint64 //18446744073709551615

	var multiplier = max / uint64(assignNumbVNodes)

	var i int = 1
	var prev uint64 = 0
	for ; i <= assignNumbVNodes; i++ {
		var maxToken = prev + multiplier
		if maxToken < multiplier {
			maxToken = max
		}
		vnodes = append(vnodes, *InitVNode(prev, maxToken, nodeId, nodeIp, nodePort, []string{}))
		prev = maxToken + 1
	}

	return &vnodes
}

func GetRandom(max int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	min := 0
	return (r.Intn(max-min) + min)
}

func UpdateReplication(nodesInCluster *[]string, nodeId string, currentNodeReplication *[]string) {
	// fmt.Println(nodeId, "#############")
	// fmt.Println(*currentNodeReplication)
	var numberOfReplicas = REPLICATION_FACTOR - 1

	var numberOfNodesInCluster = len(*nodesInCluster)

	if numberOfNodesInCluster <= numberOfReplicas {
		numberOfReplicas = numberOfNodesInCluster - 1
	}

	if numberOfReplicas < 0 {
		numberOfReplicas = 0
	}

	var tmp = make(map[string]bool)

	for _, v := range *currentNodeReplication {
		tmp[v] = true
	}

	for {
		if len(*currentNodeReplication) < numberOfReplicas {
			for _, v := range *nodesInCluster {
				// fmt.Println(v)
				if v != nodeId && !tmp[v] && len(*currentNodeReplication) < numberOfReplicas {
					*currentNodeReplication = append(*currentNodeReplication, v)
					tmp[v] = true
				}
			}
		} else {
			break
		}
	}

	// fmt.Println(*currentNodeReplication)
	// fmt.Println(nodeId, "#############")
	// fmt.Println()
}

type entry struct {
	val int
	key string
}

// TO ORDER MAPS: //////////////////
type entries []entry

func (s entries) Len() int           { return len(s) }
func (s entries) Less(i, j int) bool { return s[i].val < s[j].val }
func (s entries) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func OrderInReverse(m *map[string]int) []string {
	var es entries

	for k, v := range *m {
		es = append(es, entry{val: v, key: k})
	}

	sort.Sort(es)

	var onlyKeys = []string{}
	for _, e := range es {
		onlyKeys = append(onlyKeys, e.key)
		//fmt.Printf("%q : %d\n", e.key, e.val)
	}
	return onlyKeys
}

//////////////////

func AddNode(oldPartionTable *[]VNode, assignNumbVNodes int, newNodeId string, newNodeIp string, newNodePort string) (*[]VNode, error) {

	//calcualte stats used to generate the PT
	var nodesInClusterReplicationLoad = make(map[string]int)
	//newNodeId is on top with zero replicas
	nodesInClusterReplicationLoad[newNodeId] = 0
	tmp := make(map[string]bool)
	for _, v := range *oldPartionTable {
		if !tmp[v.NodeId] {
			nodesInClusterReplicationLoad[v.NodeId] = 0
			tmp[v.NodeId] = true
		}
	}
	for _, v := range *oldPartionTable {
		for _, n := range v.ReplicatedTo {
			nodesInClusterReplicationLoad[n]++
		}
	}

	var nodesInClusterInReplciationOrder = OrderInReverse(&nodesInClusterReplicationLoad)
	numberOfVnodesInTheCluster := len(*oldPartionTable)
	//////////////

	//randomly assign assignNumbVNodes to the new node
	var vnodesPositions = make(map[int]bool)
	for i := 0; i < assignNumbVNodes; i++ {
		pos := GetRandom(numberOfVnodesInTheCluster)

		for {
			if vnodesPositions[pos] {
				pos = GetRandom(numberOfVnodesInTheCluster)
			} else {
				break
			}
		}
		vnodesPositions[pos] = true
	}
	//////////////

	var newPartitionTable []VNode

	var numberOfReplicas = REPLICATION_FACTOR - 1
	if numberOfReplicas < 0 {
		numberOfReplicas = 0
	}
	//split selected vnodes
	for k, v := range *oldPartionTable {
		if v.NodeId == newNodeId {
			return nil, errors.New("partition table already contains a node with id: " + newNodeId)
		}
		if vnodesPositions[k] {
			UpdateReplication(&nodesInClusterInReplciationOrder, v.NodeId, &v.ReplicatedTo)
			var replicatedTo = []string{}
			UpdateReplication(&nodesInClusterInReplciationOrder, newNodeId, &replicatedTo)
			et := v.StartToken + ((v.EndToken - v.StartToken) / 2)
			v1 := *InitVNode(v.StartToken, et, newNodeId, newNodeIp, newNodePort, replicatedTo)
			v2 := *InitVNode(et+1, v.EndToken, v.NodeId, v.NodeIp, v.NodePort, v.ReplicatedTo)
			newPartitionTable = append(newPartitionTable, v1)
			newPartitionTable = append(newPartitionTable, v2)
		} else {
			if len(v.ReplicatedTo) < numberOfReplicas {
				UpdateReplication(&nodesInClusterInReplciationOrder, v.NodeId, &v.ReplicatedTo)
			}
			newPartitionTable = append(newPartitionTable, v)
		}

	}

	return &newPartitionTable, nil
}

// uses the fact that originally a node was inserted splitting the next node in the ring
// this may not be exact, if the next node gets splitted again by another node but it should be ok
// note also that a node gets inserted only one time => no 2 CONSECUTIVE vnodes should be ever assigned to the same node
func RemoveNode(oldPartionTable *[]VNode, oldNodeId string) *[]VNode {

	//calcualte stats used to generate the PT (without the node to be removed)
	var nodesInClusterReplicationLoad = make(map[string]int)
	tmp := make(map[string]bool)
	for _, v := range *oldPartionTable {
		if oldNodeId != v.NodeId {
			if !tmp[v.NodeId] {
				nodesInClusterReplicationLoad[v.NodeId] = 0
				tmp[v.NodeId] = true
			}
		}
	}
	for _, v := range *oldPartionTable {
		if oldNodeId != v.NodeId {
			for _, n := range v.ReplicatedTo {
				if n != oldNodeId {
					nodesInClusterReplicationLoad[n]++
				}
			}
		}

	}

	var nodesInClusterInReplciationOrder = OrderInReverse(&nodesInClusterReplicationLoad)
	//////////////

	var mm = make(map[string]int)

	for _, v := range *oldPartionTable {
		mm[v.NodeId]++
	}

	numberOfNodesInTheCluster := len(mm) + 1

	if numberOfNodesInTheCluster == 1 {
		//doesnt mean anything to remove a node from a cluster with only one node
		return oldPartionTable
	}

	var newPartitionTable []VNode

	performJoin := false
	var nodeToDelete VNode
	for _, v := range *oldPartionTable {
		if v.NodeId == oldNodeId {
			performJoin = true
			nodeToDelete = v
		} else {
			if performJoin {
				v.StartToken = nodeToDelete.StartToken
				performJoin = false
			}
			var tmp []string
			for _, n := range v.ReplicatedTo {
				if n != oldNodeId {
					tmp = append(tmp, n)
				}
			}
			UpdateReplication(&nodesInClusterInReplciationOrder, v.NodeId, &tmp)
			v.ReplicatedTo = tmp
			newPartitionTable = append(newPartitionTable, v)
		}
	}

	return &newPartitionTable
}

// ************** DEBUG FUNCTIONS *************************************************
func PrintDistribution(vnodes []VNode) {
	var mm = make(map[string]int)

	for _, v := range vnodes {
		mm[v.NodeId]++
	}

	max := 0
	min := 0
	fmt.Println("nodeId", "#vnodes")
	fmt.Println("------------------------------------------")
	for k, e := range mm {
		if e > max {
			max = e
		}
		if min == 0 || e < min {
			min = e
		}
		fmt.Println(k, "    ", e)
	}

	fmt.Println()
	fmt.Println("min", min)
	fmt.Println("max", max)
	fmt.Println()
}

func PrintVnodes(vnodes []VNode) {
	for _, v := range vnodes {
		fmt.Println(v.StartToken, v.EndToken, v.NodeId, v.ReplicatedTo)
	}
	fmt.Println()
}

func PrintReplicastDistribution(vnodes []VNode) {
	var mm = make(map[string]int)

	for _, v := range vnodes {
		for _, n := range v.ReplicatedTo {
			mm[n]++
		}
	}

	for k, e := range mm {
		fmt.Println(k, "    ", e)
	}

	fmt.Println()
}

//********************************************************************************
