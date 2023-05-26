package partitioner

import (
	"fmt"
)

const NUMBER_VNODES_PER_NODE = 8

func GeneratePartitionTable(numberOfNodesInTheCluster int, nodeId string, nodeIp string, nodePort string) *[]VNode {
	var numberOfVNodes = NUMBER_VNODES_PER_NODE * numberOfNodesInTheCluster

	var vnodes []VNode

	const max uint64 = 18446744073709551615

	var multiplier = max / uint64(numberOfVNodes)

	var i int = 1
	var prev uint64 = 0
	for ; i <= numberOfVNodes; i++ {
		var maxToken = prev + multiplier
		if maxToken < multiplier {
			maxToken = max
		}
		vnodes = append(vnodes, *InitVNode(prev, maxToken, nodeId, nodeIp, nodePort))
		prev = maxToken + 1
	}

	return &vnodes
}

// ** calculates the position of new node in the cluster
func calculatePosition(lenArray int, numberOfPosition int) []int {
	chunkSize := lenArray / numberOfPosition

	var positions []int
	for i := 0; i < lenArray; i += chunkSize {
		end := i + chunkSize
		if end > lenArray {
			end = lenArray
		}
		if len(positions) < numberOfPosition {
			positions = append(positions, end-1)
		}
	}

	return positions
}

func AddNode(oldPartionTable *[]VNode, newNodeId string, newNodeIp string, newNodePort string) *[]VNode {

	var mm = make(map[string]int)

	for _, v := range *oldPartionTable {
		mm[v.NodeId]++
	}

	numberOfNodesInTheCluster := len(mm) + 1

	var numberOfVNodesForNewNode = NUMBER_VNODES_PER_NODE

	newPartitionTable := *GeneratePartitionTable(numberOfNodesInTheCluster, "", "", "")

	positions := calculatePosition(len(newPartitionTable), numberOfVNodesForNewNode)

	for _, v := range positions {
		newPartitionTable[v].NodeId = newNodeId
		newPartitionTable[v].NodeIp = newNodeIp
		newPartitionTable[v].NodePort = newNodePort
	}

	//recalculate the partition table for the other nodes trying to move the minimum number of vndoes
	var n = 0
	for i, v := range newPartitionTable {
		if v.NodeId != newNodeId {
			for n < len(*oldPartionTable) {
				old := (*oldPartionTable)[n]
				if v.EndToken <= old.EndToken {
					//stay
					newPartitionTable[i].NodeId = old.NodeId
					newPartitionTable[i].NodeIp = old.NodeIp
					newPartitionTable[i].NodePort = old.NodePort
					n++
					break
				} else {
					n++
				}
			}
		}

	}

	return &newPartitionTable
}

func RemoveNode(oldPartionTable *[]VNode, oldNodeId string) *[]VNode {

	var mm = make(map[string]int)

	for _, v := range *oldPartionTable {
		mm[v.NodeId]++
	}

	numberOfNodesInTheCluster := len(mm) - 1

	newPartitionTable := *GeneratePartitionTable(numberOfNodesInTheCluster, "", "", "")

	var n = 0
	var prevNodeId string
	for i := range newPartitionTable {
		old := (*oldPartionTable)[n]
		if old.NodeId == oldNodeId || prevNodeId == old.NodeId {
			n++
			old = (*oldPartionTable)[n]
		}
		newPartitionTable[i].NodeId = old.NodeId
		newPartitionTable[i].NodeIp = old.NodeIp
		newPartitionTable[i].NodePort = old.NodePort

		prevNodeId = old.NodeId
		n++
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
		fmt.Println(v.StartToken, v.EndToken, v.NodeId)
	}
	fmt.Println()
}

//********************************************************************************
