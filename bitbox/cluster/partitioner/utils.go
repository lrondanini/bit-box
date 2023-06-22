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

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/rs/xid"
	"golang.org/x/exp/slices"
)

// do not change this without extensive testing
const REPLICATION_FACTOR = 3

func GenerateNewPartitionTable(assignNumbVNodes int, nodeId string, nodeIp string, nodePort string) *[]VNode {
	var vnodes []VNode

	const max uint64 = math.MaxUint64 //18446744073709551615 //199 //

	var multiplier = max / uint64(assignNumbVNodes)

	var i int = 1
	var prev uint64 = 0
	for ; i <= assignNumbVNodes; i++ {
		var maxToken = prev + multiplier
		if maxToken < multiplier {
			maxToken = max
		}

		vnodes = append(vnodes, *InitVNode(prev, maxToken, "", []string{}, xid.New().String(), nodeId, nodeIp, nodePort, []string{nodeId}))
		prev = maxToken + 1
	}

	return &vnodes
}

func GetRandom(max int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	min := 0
	return (r.Intn(max-min) + min)
}

func UpdateReplication(nodesInClusterReplicationLoad *map[string]int, nodeId string, currentNodeReplication *[]string) {

	var nodesInCluster = OrderInReverse(nodesInClusterReplicationLoad)

	var numberOfReplicas = REPLICATION_FACTOR - 1

	var numberOfNodesInCluster = len(nodesInCluster)

	if numberOfNodesInCluster <= numberOfReplicas {
		for _, v := range nodesInCluster {
			if !slices.Contains(*currentNodeReplication, v) {
				*currentNodeReplication = append(*currentNodeReplication, v)
			}
		}
	} else {
		var tmp []string

		var dups = make(map[string]bool)
		for _, v := range *currentNodeReplication {
			if v != nodeId && !dups[v] {
				tmp = append(tmp, v)
			}

			dups[v] = true
		}

		for {
			if len(tmp) < numberOfReplicas {
				for _, v := range nodesInCluster {
					if v != nodeId && len(tmp) < numberOfReplicas && !dups[v] {
						(*nodesInClusterReplicationLoad)[v]++
						tmp = append(tmp, v)
					}
				}
			} else {
				break
			}
		}

		*currentNodeReplication = tmp
	}

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
	}

	return onlyKeys
}

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
			prevReplicatedTo := v.ReplicatedTo
			UpdateReplication(&nodesInClusterReplicationLoad, v.NodeId, &v.ReplicatedTo)
			var replicatedTo = []string{}
			UpdateReplication(&nodesInClusterReplicationLoad, newNodeId, &replicatedTo)
			et := v.StartToken + ((v.EndToken - v.StartToken) / 2)
			v1 := *InitVNode(v.StartToken, et, v.NodeId, prevReplicatedTo, xid.New().String(), newNodeId, newNodeIp, newNodePort, replicatedTo)
			v2 := *InitVNode(et+1, v.EndToken, v.NodeId, prevReplicatedTo, v.VNodeID, v.NodeId, v.NodeIp, v.NodePort, v.ReplicatedTo)
			newPartitionTable = append(newPartitionTable, v1)
			newPartitionTable = append(newPartitionTable, v2)
		} else {
			prevReplicatedTo := v.ReplicatedTo
			UpdateReplication(&nodesInClusterReplicationLoad, v.NodeId, &v.ReplicatedTo)
			v.PrevReplicatedTo = prevReplicatedTo
			v.PrevNodeID = v.NodeId
			newPartitionTable = append(newPartitionTable, v)
		}

	}

	//rebalance replicas
	rebalanceReplicas(newNodeId, assignNumbVNodes, &newPartitionTable)

	return &newPartitionTable, nil
}

func orderAsc(nodes map[string]int) []string {
	keys := []string{}

	for key := range nodes {
		keys = append(keys, key)
	}

	sort.Slice(keys, func(i, j int) bool {
		return nodes[keys[i]] > nodes[keys[j]]
	})

	return keys
}

func getNodeToStealreplicaFrom(mm map[string]int) string {
	orderedNodes := orderAsc(mm)
	n := 0
	stealReplicaFrom := ""
	done := false
	for !done {
		stealReplicaFrom = orderedNodes[n]
		currentRepOnNode := mm[stealReplicaFrom]
		if currentRepOnNode == 1 {
			n++
			if n > len(orderedNodes) {
				stealReplicaFrom = ""
				done = true
			}
		} else {
			done = true
		}
	}

	return stealReplicaFrom
}

func rebalanceReplicas(newNodeId string, assignNumbVNodes int, newPartitionTable *[]VNode) {

	var numberOfReplicasForNewNode = (REPLICATION_FACTOR - 1) * assignNumbVNodes

	var mm = make(map[string]int)

	for _, v := range *newPartitionTable {
		for _, n := range v.ReplicatedTo {
			mm[n]++
		}
	}

	// for k, e := range mm {
	// 	fmt.Println(k, "    ", e)
	// }

	if mm[newNodeId] < numberOfReplicasForNewNode {
		cr := mm[newNodeId]
		stealReplicaFrom := getNodeToStealreplicaFrom(mm)

		for cr < numberOfReplicasForNewNode && stealReplicaFrom != "" {
			skipped := 0
		LOOP:
			for k, v := range *newPartitionTable {
				if v.NodeId != newNodeId {
					if slices.Contains(v.ReplicatedTo, stealReplicaFrom) && !slices.Contains(v.ReplicatedTo, newNodeId) {

						tmp := []string{}
						for _, n := range v.ReplicatedTo {
							if n == stealReplicaFrom {
								tmp = append(tmp, newNodeId)
							} else {
								tmp = append(tmp, n)
							}
						}
						(*newPartitionTable)[k].ReplicatedTo = tmp
						mm[stealReplicaFrom]--
						cr++
						stealReplicaFrom = getNodeToStealreplicaFrom(mm)
						break LOOP
					} else {
						skipped++
					}
				} else {
					skipped++
				}
			}

			if skipped == len(*newPartitionTable) {
				//out, not enough nodes to steal from
				break
			}
		}

	}

}

func RemoveNode(oldPartionTable *[]VNode, oldNodeId string) *[]VNode {

	//calculate stats used to generate the PT (without the node to be removed)
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
			UpdateReplication(&nodesInClusterReplicationLoad, v.NodeId, &tmp)
			prevReplicatedTo := v.ReplicatedTo
			v.ReplicatedTo = tmp
			if v.PrevNodeID == oldNodeId {
				v.PrevNodeID = v.NodeId
			}
			tmp = []string{}
			for _, n := range prevReplicatedTo {
				if n != oldNodeId {
					tmp = append(tmp, n)
				}
			}
			v.PrevReplicatedTo = tmp
			newPartitionTable = append(newPartitionTable, v)
		}
	}

	return &newPartitionTable
}

// ************** FUNCTIONS USED DURING DEV *************************************************
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

func VNodeToString(v VNode) string {
	return "ST:\t" + strconv.FormatUint(v.StartToken, 10) + "-\t ET:\t" + strconv.FormatUint(v.EndToken, 10) + "-\t VN:\t" + v.VNodeID + "-\t PVN:\t" + v.PrevNodeID + "-\t PRT:\t" + strings.Join(v.PrevReplicatedTo, ",") + "-\t N:\t" + v.NodeId + "-\t RT:\t" + strings.Join(v.ReplicatedTo, ",") + "-"
}

func PrintVnodes(vnodes []VNode) {

	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', tabwriter.AlignRight)

	s := "StartToken\t * \tEndToken\t * \tVNodeID\t * \tPrevNodeID\t * \tPrevReplicatedTo\t * \tNodeId\t * \tReplicatedTo"
	//fmt.Println(v.StartToken, v.EndToken, v.VNodeID, v.PrevNodeID, v.PrevReplicatedTo, v.NodeId, v.ReplicatedTo)
	fmt.Fprintln(w, s)

	for _, v := range vnodes {
		s := strconv.FormatUint(v.StartToken, 10) + "\t * \t" + strconv.FormatUint(v.EndToken, 10) + "\t * \t" + v.VNodeID + "\t * \t" + v.PrevNodeID + "\t * \t" + strings.Join(v.PrevReplicatedTo, ",") + "\t * \t" + v.NodeId + "\t * \t" + strings.Join(v.ReplicatedTo, ",")
		//fmt.Println(v.StartToken, v.EndToken, v.VNodeID, v.PrevNodeID, v.PrevReplicatedTo, v.NodeId, v.ReplicatedTo)
		fmt.Fprintln(w, s)
	}

	w.Flush()

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
