package partitioner

import (
	"fmt"
	"testing"
)

func TestGenerateNewPartitionTable(t *testing.T) {

	NUMBER_OF_VNODES := 8
	vnodes := GenerateNewPartitionTable(NUMBER_OF_VNODES, "node-id", "0.0.0.0", "1111")

	if len(*vnodes) != NUMBER_OF_VNODES {
		t.Errorf("Generated %d vnodes but expected %d vnodes", len(*vnodes), NUMBER_OF_VNODES)
	}

	var uniqueKeys = make(map[string]bool)
	found := 0
	for _, v := range *vnodes {
		if uniqueKeys[v.VNodeID] {
			found++
		} else {
			uniqueKeys[v.VNodeID] = true
		}
	}

	if found > 0 {
		t.Errorf("Found %d VNODES with duplicated keys", found)
	}

	t.Log("PASS")
}

func TestConsistency(t *testing.T) {

	var vnodes *[]VNode
	var vnodesBefore *[]VNode
	var err error

	NUMBER_OF_VNODES := 4
	oldPartionTable := GenerateNewPartitionTable(NUMBER_OF_VNODES, "node-0", "", "")

	vnodesBefore, err = AddNode(oldPartionTable, NUMBER_OF_VNODES, "node-1", "", "")
	if err != nil {
		t.Errorf(err.Error())
	}

	vnodesBefore, _ = AddNode(vnodesBefore, NUMBER_OF_VNODES, "node-2", "", "")

	PrintVnodes(*vnodesBefore)
	fmt.Println()

	vnodes, err = AddNode(vnodesBefore, NUMBER_OF_VNODES, "node-3", "", "")
	if err != nil {
		t.Errorf(err.Error())
	}

	PrintVnodes(*vnodes)
	fmt.Println()

	for _, o := range *vnodesBefore {
		splitTo := []VNode{}
		for _, n := range *vnodes {
			if (o.EndToken < n.StartToken) || (o.StartToken > n.EndToken) {
				//no overlap, nothing to do
				continue
			} else {
				if o.StartToken == n.StartToken {
					splitTo = append(splitTo, n)
				} else if o.StartToken < n.EndToken {
					splitTo = append(splitTo, n)
				}
			}
		}

		var e uint64 = 0
		for _, v := range splitTo {
			if v.EndToken > e {
				e = v.EndToken
			}
		}

		if e != o.EndToken {
			fmt.Println("ERROR ", o.VNodeID, " ", o.StartToken, " ", o.EndToken, " ")
		}
	}

	t.Log("PASS")
}

func TestPrintForDebug(t *testing.T) {
	pt0 := GenerateNewPartitionTable(4, "node-0", "", "")
	PrintVnodes(*pt0)
	fmt.Println()

	fmt.Println("---------------- ADDED node-1 ----------------------------------")
	pt1, _ := AddNode(pt0, 4, "node-1", "", "")
	PrintVnodes(*pt1)
	PrintReplicastDistribution(*pt1)

	fmt.Println("---------------- ADDED node-2 ----------------------------------")
	pt2, _ := AddNode(pt1, 4, "node-2", "", "")
	PrintVnodes(*pt2)
	PrintReplicastDistribution(*pt2)

	fmt.Println("---------------- ADDED node-3 ----------------------------------")
	pt3, _ := AddNode(pt2, 4, "node-3", "", "")
	PrintVnodes(*pt3)
	PrintReplicastDistribution(*pt3)

	fmt.Println("---------------- ADDED node-4 ----------------------------------")
	pt4, _ := AddNode(pt3, 4, "node-4", "", "")
	PrintVnodes(*pt4)
	PrintReplicastDistribution(*pt4)

	fmt.Println("---------------- ADDED node-5 ----------------------------------")
	pt5, _ := AddNode(pt4, 4, "node-5", "", "")
	PrintVnodes(*pt5)
	PrintReplicastDistribution(*pt5)

	fmt.Println("----------------------------REMOVE node-1-------------------------------------------------")
	pt6 := RemoveNode(pt5, "node-1")
	PrintVnodes(*pt6)
	PrintReplicastDistribution(*pt6)

	fmt.Println("----------------------------REMOVE node-2-------------------------------------------------")
	pt7 := RemoveNode(pt6, "node-2")
	PrintVnodes(*pt7)
	PrintReplicastDistribution(*pt7)

	fmt.Println("----------------------------REMOVE node-5-------------------------------------------------")
	pt8 := RemoveNode(pt7, "node-5")
	PrintVnodes(*pt8)
	PrintReplicastDistribution(*pt8)

	fmt.Println("----------------------------REMOVE node-3-------------------------------------------------")
	pt9 := RemoveNode(pt8, "node-3")
	PrintVnodes(*pt9)
	PrintReplicastDistribution(*pt9)

	fmt.Println("----------------------------REMOVE node-4-------------------------------------------------")
	pt10 := RemoveNode(pt9, "node-4")
	PrintVnodes(*pt10)
	PrintReplicastDistribution(*pt10)
}

func TestSyncData(t *testing.T) {
	vnodes := GenerateNewPartitionTable(4, "node-0", "", "")
	PrintVnodes(*vnodes)
	fmt.Println()

	pt0 := InitPartitionTable(*vnodes, 0)

	fmt.Println("---------------- ADDED node-1 ----------------------------------")
	fmt.Println()
	vnodes, _ = AddNode(vnodes, 4, "node-1", "", "")
	PrintVnodes(*vnodes)

	pt1 := InitEmptyPartitionTable()

	fmt.Println()
	pt0.CalculateMissingData("node-0", InitPartitionTable(*vnodes, 0))
	pt1.CalculateMissingData("node-1", InitPartitionTable(*vnodes, 0))

	fmt.Println("---------------- ADDED node-2 ----------------------------------")
	fmt.Println()
	vnodes, _ = AddNode(vnodes, 4, "node-2", "", "")
	PrintVnodes(*vnodes)

	pt2 := InitEmptyPartitionTable()

	fmt.Println()
	pt0.CalculateMissingData("node-0", InitPartitionTable(*vnodes, 0))
	pt1.CalculateMissingData("node-1", InitPartitionTable(*vnodes, 0))
	pt2.CalculateMissingData("node-2", InitPartitionTable(*vnodes, 0))

	fmt.Println("---------------- ADDED node-3 ----------------------------------")
	fmt.Println()
	vnodes, _ = AddNode(vnodes, 4, "node-3", "", "")
	PrintVnodes(*vnodes)

	pt3 := InitEmptyPartitionTable()

	fmt.Println()
	pt0.CalculateMissingData("node-0", InitPartitionTable(*vnodes, 0))
	pt1.CalculateMissingData("node-1", InitPartitionTable(*vnodes, 0))
	pt2.CalculateMissingData("node-2", InitPartitionTable(*vnodes, 0))
	pt3.CalculateMissingData("node-3", InitPartitionTable(*vnodes, 0))

	fmt.Println("---------------- ADDED node-4 ----------------------------------")
	fmt.Println()
	vnodes, _ = AddNode(vnodes, 4, "node-4", "", "")
	PrintVnodes(*vnodes)

	pt4 := InitEmptyPartitionTable()

	fmt.Println()
	pt0.CalculateMissingData("node-0", InitPartitionTable(*vnodes, 0))
	pt1.CalculateMissingData("node-1", InitPartitionTable(*vnodes, 0))
	pt2.CalculateMissingData("node-2", InitPartitionTable(*vnodes, 0))
	pt3.CalculateMissingData("node-3", InitPartitionTable(*vnodes, 0))
	pt4.CalculateMissingData("node-4", InitPartitionTable(*vnodes, 0))

	fmt.Println("---------------- REMOVE node-2 ----------------------------------")
	fmt.Println()
	vnodes = RemoveNode(vnodes, "node-2")
	PrintVnodes(*vnodes)
	pt0.CalculateMissingData("node-0", InitPartitionTable(*vnodes, 0))
	pt1.CalculateMissingData("node-1", InitPartitionTable(*vnodes, 0))
	pt3.CalculateMissingData("node-3", InitPartitionTable(*vnodes, 0))
	pt4.CalculateMissingData("node-4", InitPartitionTable(*vnodes, 0))

	fmt.Println("---------------- REMOVE node-1 ----------------------------------")
	fmt.Println()
	vnodes = RemoveNode(vnodes, "node-1")
	PrintVnodes(*vnodes)
	pt0.CalculateMissingData("node-0", InitPartitionTable(*vnodes, 0))
	pt3.CalculateMissingData("node-3", InitPartitionTable(*vnodes, 0))
	pt4.CalculateMissingData("node-4", InitPartitionTable(*vnodes, 0))

	fmt.Println("---------------- REMOVE node-0 ----------------------------------")
	fmt.Println()
	vnodes = RemoveNode(vnodes, "node-0")
	PrintVnodes(*vnodes)
	pt3.CalculateMissingData("node-3", InitPartitionTable(*vnodes, 0))
	pt4.CalculateMissingData("node-4", InitPartitionTable(*vnodes, 0))

	fmt.Println("---------------- REMOVE node-3 ----------------------------------")
	fmt.Println()
	vnodes = RemoveNode(vnodes, "node-3")
	PrintVnodes(*vnodes)
	pt4.CalculateMissingData("node-4", InitPartitionTable(*vnodes, 0))

	fmt.Println("---------------- ADDED node-5 ----------------------------------")
	fmt.Println()
	vnodes, _ = AddNode(vnodes, 4, "node-5", "", "")
	PrintVnodes(*vnodes)

	pt5 := InitEmptyPartitionTable()

	fmt.Println()
	pt4.CalculateMissingData("node-4", InitPartitionTable(*vnodes, 0))
	pt5.CalculateMissingData("node-5", InitPartitionTable(*vnodes, 0))

}
