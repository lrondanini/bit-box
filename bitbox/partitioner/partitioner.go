package partitioner

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/lrondanini/bit-box/bitbox/partitioner/murmur3"
	"github.com/lrondanini/bit-box/bitbox/storage"
	"golang.org/x/exp/slices"

	"github.com/google/uuid"
)

const PT_COLLECTION_NAME = "partitionTable"

func GetHash(s string) uint64 {
	return murmur3.GetHash64(s)
}

func GenerateUUID() string {
	return uuid.New().String()
}

type dataSyncAction struct {
	startToken uint64
	endToken   uint64
	status     string
	fromNodeId []string
	toNodeId   string
	toReplicas []string
	actionType string
	progress   uint64
	stepSize   uint64
	delete     bool //can delete, nodes wont hold the data as a replication node
}

type PartitionTable struct {
	VNodes              []VNode `json:"vNodes"`
	Timestamp           int64   `json:"timstamp"`
	dataSyncActionQueue []dataSyncAction
}

func InitEmptyPartitionTable() *PartitionTable {
	return &PartitionTable{}
}

func InitPartitionTable(nodes []VNode, timstamp int64) *PartitionTable {
	return &PartitionTable{
		VNodes:    nodes,
		Timestamp: timstamp,
	}
}

func LoadFromDb(systemDb *storage.Collection) (*PartitionTable, error) {
	res := PartitionTable{}
	err := systemDb.Get(PT_COLLECTION_NAME, &res)

	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, err
		}
		return nil, err
	}
	return &res, nil
}

func (pt *PartitionTable) SaveToDb(systemDb *storage.Collection) error {
	return systemDb.Set(PT_COLLECTION_NAME, pt)
}

func (pt *PartitionTable) Update2(nodeId string, newPartitionTable *PartitionTable) error {

	if len(pt.VNodes) == 0 {
		//new node, just find the partitions assigned to it
		for _, v := range newPartitionTable.VNodes {
			if v.NodeId == nodeId {
				tvn := dataSyncAction{
					startToken: v.StartToken,
					endToken:   v.EndToken,
					status:     "streaming",
					fromNodeId: []string{v.PrevNodeID},
					toNodeId:   "",
					actionType: "new",
					progress:   0,
					stepSize:   1,
				}
				pt.dataSyncActionQueue = append(pt.dataSyncActionQueue, tvn)
			} else if slices.Contains(v.ReplicatedTo, nodeId) {
				streamFrom := v.PrevReplicatedTo
				if len(streamFrom) == 0 {
					if v.PrevNodeID == "" {
						//	happens for the second node added to the cluster
						streamFrom = []string{v.NodeId}
					} else {
						streamFrom = []string{v.PrevNodeID}
					}

				}
				tvn := dataSyncAction{
					startToken: v.StartToken,
					endToken:   v.EndToken,
					status:     "streaming",
					fromNodeId: streamFrom,
					toNodeId:   "",
					actionType: "new-replica",
					progress:   0,
					stepSize:   1,
				}
				pt.dataSyncActionQueue = append(pt.dataSyncActionQueue, tvn)
			}
		}
	} else {
		for _, v := range newPartitionTable.VNodes {
			for _, cv := range pt.VNodes {
				if (cv.StartToken < v.StartToken && cv.EndToken < v.EndToken) || (cv.StartToken > v.EndToken) {
					//no overlap, nothing to do
					continue
				} else if cv.NodeId == nodeId || slices.Contains(cv.ReplicatedTo, nodeId) {
					//one way or another, this data is in this node

					lcvn := cv.EndToken - cv.StartToken
					lv := v.EndToken - v.StartToken

					if lcvn > lv {
						//shrinked, node was added
						actionType := "trim"
						to := ""
						toReplicas := []string{}
						delete := true

						if v.NodeId != nodeId {

							if cv.NodeId == nodeId {
								to = v.NodeId
							}

							for _, r := range v.ReplicatedTo {
								if r == nodeId {
									delete = false
								}
								if r != nodeId && r != v.NodeId && !slices.Contains(cv.ReplicatedTo, r) && cv.NodeId != r {
									toReplicas = append(toReplicas, r)
								}
							}
						} else {
							for _, r := range v.ReplicatedTo {
								if r == nodeId {
									delete = false
								}
								if r != nodeId && !slices.Contains(cv.ReplicatedTo, r) {
									toReplicas = append(toReplicas, r)
								}
							}

						}

						if v.NodeId == nodeId {
							delete = false
						}

						if to != "" || len(toReplicas) > 0 || delete {
							var start uint64 = 0
							var end uint64 = 0
							if cv.StartToken >= v.StartToken {
								start = cv.StartToken
								if cv.EndToken <= v.EndToken {
									end = cv.EndToken
								} else if cv.EndToken > v.EndToken {
									end = v.EndToken
								}
							} else {
								start = v.StartToken
								if cv.EndToken <= v.EndToken {
									end = cv.EndToken
								} else if cv.EndToken > v.EndToken {
									end = v.EndToken
								}
							}

							tvn := dataSyncAction{
								startToken: start,
								endToken:   end,
								status:     "streaming",
								fromNodeId: []string{},
								toNodeId:   to,
								toReplicas: toReplicas,
								actionType: actionType,
								progress:   0,
								stepSize:   1,
								delete:     delete,
							}
							pt.dataSyncActionQueue = append(pt.dataSyncActionQueue, tvn)
						}
					} else {
						//partition grew (node was removed)
						actionType := "expand"
						to := ""
						toReplicas := []string{}
						delete := false

						if v.NodeId != nodeId {

							if cv.NodeId == nodeId {
								to = v.NodeId
							}

							for _, r := range v.ReplicatedTo {
								if r == nodeId {
									delete = false
								}
								if r != nodeId && r != v.NodeId && !slices.Contains(cv.ReplicatedTo, r) && cv.NodeId != r {
									toReplicas = append(toReplicas, r)
								}
							}
						} else {
							for _, r := range v.ReplicatedTo {
								if r == nodeId {
									delete = false
								}
								if r != nodeId && !slices.Contains(cv.ReplicatedTo, r) {
									toReplicas = append(toReplicas, r)
								}
							}

						}

						if to != "" || len(toReplicas) > 0 {
							var start uint64 = 0
							var end uint64 = 0
							if cv.StartToken >= v.StartToken {
								start = cv.StartToken
								if cv.EndToken <= v.EndToken {
									end = cv.EndToken
								} else if cv.EndToken > v.EndToken {
									end = v.EndToken
								}
							} else {
								start = v.StartToken
								if cv.EndToken <= v.EndToken {
									end = cv.EndToken
								} else if cv.EndToken > v.EndToken {
									end = v.EndToken
								}
							}

							tvn := dataSyncAction{
								startToken: start,
								endToken:   end,
								status:     "streaming",
								fromNodeId: []string{},
								toNodeId:   to,
								toReplicas: toReplicas,
								actionType: actionType,
								progress:   0,
								stepSize:   1,
								delete:     delete,
							}
							pt.dataSyncActionQueue = append(pt.dataSyncActionQueue, tvn)
						}
					}
				}
			}
		}
	}

	// current := make(map[string]VNode)
	// existing := make(map[string]bool)

	// for _, v := range pt.VNodes {
	// 	if v.NodeId == nodeId {
	// 		current[v.VNodeID] = v
	// 		existing[v.VNodeID] = true
	// 	}
	// }

	// //untouchedVnodes := make([]VNode, 0)
	// for _, v := range newPartitionTable.VNodes {
	// 	if v.NodeId == nodeId {
	// 		if existing[v.VNodeID] {
	// 			cvn := current[v.VNodeID]
	// 			lcvn := cvn.EndToken - cvn.StartToken
	// 			lv := v.EndToken - v.StartToken

	// 			if lcvn > lv {
	// 				//partition shrinked
	// 				var start uint64 = 0
	// 				if cvn.StartToken > v.StartToken {
	// 					start = v.StartToken
	// 				} else if cvn.StartToken <= v.StartToken {
	// 					start = cvn.StartToken
	// 				}
	// 				var end uint64 = v.StartToken - 1
	// 				v.StartToken = start

	// 				v.EndToken = end

	// 				to := ""
	// 				for _, v1 := range newPartitionTable.VNodes {
	// 					if v1.StartToken == start && v1.EndToken == end {
	// 						to = v1.NodeId
	// 					}
	// 				}
	// 				v.NodeId = to
	// 				delete := true
	// 				if slices.Contains(v.ReplicatedTo, nodeId) || len(v.ReplicatedTo) < 2 {
	// 					delete = false
	// 				}
	// 				tvn := dataSyncAction{
	// 					startToken: start,
	// 					endToken:   end,
	// 					status:     "streaming",
	// 					fromNodeId: []string{},
	// 					toNodeId:   to,
	// 					actionType: "trim",
	// 					progress:   0,
	// 					stepSize:   (end - start) / 2,
	// 					delete:     delete,
	// 				}
	// 				pt.dataSyncActionQueue = append(pt.dataSyncActionQueue, tvn)
	// 			} else if lcvn < lv {
	// 				//partition enlarged
	// 				var start uint64 = v.StartToken
	// 				var end uint64 = cvn.StartToken - 1
	// 				v.StartToken = start
	// 				v.EndToken = end

	// 				//find who got this range before:
	// 				from := []string{}
	// 				for _, v1 := range pt.VNodes {
	// 					if v1.StartToken == start && v1.EndToken == end {
	// 						from = v1.ReplicatedTo
	// 					}
	// 				}

	// 				v.PrevReplicatedTo = from

	// 				tvn := dataSyncAction{
	// 					startToken: start,
	// 					endToken:   end,
	// 					status:     "streaming",
	// 					fromNodeId: from,
	// 					toNodeId:   "",
	// 					actionType: "expand",
	// 					progress:   0,
	// 					stepSize:   (end - start) / 2,
	// 				}

	// 				pt.dataSyncActionQueue = append(pt.dataSyncActionQueue, tvn)
	// 			} else {
	// 				//partition not changed
	// 				//untouchedVnodes = append(untouchedVnodes, v)
	// 			}
	// 		} else {
	// 			if v.PrevNodeID != "" && v.PrevNodeID != nodeId {
	// 				tvn := dataSyncAction{
	// 					startToken: v.StartToken,
	// 					endToken:   v.EndToken,
	// 					status:     "streaming",
	// 					fromNodeId: []string{v.PrevNodeID},
	// 					toNodeId:   "",
	// 					actionType: "new",
	// 					progress:   0,
	// 					stepSize:   (v.EndToken - v.StartToken) / 2,
	// 				}
	// 				pt.dataSyncActionQueue = append(pt.dataSyncActionQueue, tvn)
	// 			}

	// 		}
	// 	}
	// }

	fmt.Println(nodeId + " ********************************** " + nodeId)
	printDataSyncActionQueue(pt.dataSyncActionQueue)
	fmt.Println()
	fmt.Println()

	//empty for cleaner debug
	pt.dataSyncActionQueue = []dataSyncAction{}

	pt.VNodes = newPartitionTable.VNodes

	return nil
}

type DataSyncTask struct {
	StartToken            uint64
	EndToken              uint64
	Status                DataSyncTaskStatus
	FromNodeId            string
	ToNodeId              string
	VNodeId               string
	Action                DataSyncAction
	Progress              uint64
	StepSize              uint64
	PartionTableTimestamp uint64
}

type DataSyncAction uint8

const (
	SendData DataSyncAction = iota
	SendDataForReplication
	DeleteData
)

func (s DataSyncAction) String() string {
	switch s {
	case SendData:
		return "SendData"
	case SendDataForReplication:
		return "SendDataForReplication"
	case DeleteData:
		return "DeleteData"
	default:
		panic(fmt.Sprintf("unknown DataSyncActionOld: %d", s))
	}
}

type DataSyncTaskStatus uint8

const (
	NewTask DataSyncTaskStatus = iota
	Streaming
	Completed
)

func (s DataSyncTaskStatus) String() string {
	switch s {
	case NewTask:
		return "NewTask"
	case Streaming:
		return "Streaming"
	case Completed:
		return "Completed"
	default:
		panic(fmt.Sprintf("unknown DataSyncActionOld: %d", s))
	}
}

/*
func (pt *PartitionTable) CalculateMissingData(nodeId string, newPartitionTable *PartitionTable) error {

		var tasks []DataSyncTask

		current := make(map[string]VNode)
		existing := make(map[string]bool)

		for _, v := range pt.VNodes {
			if v.NodeId == nodeId {
				current[v.VNodeID] = v
				existing[v.VNodeID] = true
			}
		}

		for _, v := range newPartitionTable.VNodes {
			if v.NodeId == nodeId {
				if existing[v.VNodeID] {
					cvn := current[v.VNodeID]
					lcvn := cvn.EndToken - cvn.StartToken
					lv := v.EndToken - v.StartToken

					if lcvn > lv {
						//partition shrinked
						var start uint64 = 0
						if cvn.StartToken > v.StartToken {
							start = v.StartToken
						} else if cvn.StartToken <= v.StartToken {
							start = cvn.StartToken
						}
						var end uint64 = v.StartToken - 1
						v.StartToken = start

						v.EndToken = end

						to := ""
						for _, v1 := range newPartitionTable.VNodes {
							if v1.StartToken == start && v1.EndToken == end {
								to = v1.NodeId
							}
						}
						v.NodeId = to

						t := DataSyncTask{
							StartToken: start,
							EndToken:   end,
							Status:     NewTask,
							FromNodeId: nodeId,
							ToNodeId:   to,
							VNodeId:    v.VNodeID,
							Action:     SendData,
							Progress:   0,
							StepSize:   (end - start) / 2,
						}

						tasks = append(tasks, t)
					} else if lcvn < lv {
						//partition enlarged
						var start uint64 = v.StartToken
						var end uint64 = cvn.StartToken - 1
						v.StartToken = start
						v.EndToken = end

						//find who got this range before:
						from := []string{}
						for _, v1 := range pt.VNodes {
							if v1.StartToken == start && v1.EndToken == end {
								from = v1.ReplicatedTo
							}
						}

						v.PrevReplicatedTo = from

						tvn := dataSyncAction{
							startToken: start,
							endToken:   end,
							status:     "streaming",
							fromNodeId: from,
							toNodeId:   "",
							actionType: "expand",
							progress:   0,
							stepSize:   (end - start) / 2,
						}

						pt.dataSyncActionQueue = append(pt.dataSyncActionQueue, tvn)
					} else {
						//partition not changed
						//untouchedVnodes = append(untouchedVnodes, v)
					}
				} else {
					//new partition
					if v.PrevNodeID != "" && v.PrevNodeID != nodeId {
						tvn := dataSyncAction{
							startToken: v.StartToken,
							endToken:   v.EndToken,
							status:     "streaming",
							fromNodeId: []string{v.PrevNodeID},
							toNodeId:   "",
							actionType: "new",
							progress:   0,
							stepSize:   (v.EndToken - v.StartToken) / 2,
						}
						pt.dataSyncActionQueue = append(pt.dataSyncActionQueue, tvn)
					}

				}
			}
		}
		fmt.Println(nodeId + " ********************************** " + nodeId)
		printDataSyncTasks(tasks)
		fmt.Println()
		fmt.Println()
		return nil
	}
*/

// help function for CalculateMissingData
func calcFetchDataTaskDetails(vnode VNode, oldVnode VNode, nodeId string, idNodetoBeRemoved string) DataSyncTask {
	askToNodeId := ""
	action := SendData
	if vnode.NodeId == nodeId {
		if oldVnode.NodeId != idNodetoBeRemoved {
			askToNodeId = oldVnode.NodeId
		} else {
			for _, ato := range oldVnode.ReplicatedTo {
				if ato != idNodetoBeRemoved {
					askToNodeId = ato
					break
				}
			}
		}
	} else if slices.Contains(vnode.ReplicatedTo, nodeId) {
		action = SendDataForReplication
		for _, ato := range oldVnode.ReplicatedTo {
			if ato != idNodetoBeRemoved {
				askToNodeId = ato
			}
		}
		if askToNodeId == "" {
			askToNodeId = oldVnode.NodeId
		}
	}

	t := DataSyncTask{
		StartToken: oldVnode.StartToken,
		EndToken:   oldVnode.EndToken,
		Status:     NewTask,
		FromNodeId: nodeId,
		ToNodeId:   askToNodeId,
		VNodeId:    vnode.VNodeID,
		Action:     action,
		Progress:   0,
		StepSize:   0,
	}

	return t
}
func (pt *PartitionTable) CalculateMissingData(nodeId string, newPartitionTable *PartitionTable) error {

	fmt.Println(nodeId + " ********************************** " + nodeId)

	var tasks []DataSyncTask

	if len(newPartitionTable.VNodes) < len(pt.VNodes) {
		//node removed
		nodesAccountedFor := []string{}
		for _, v := range newPartitionTable.VNodes {
			if v.NodeId == nodeId || slices.Contains(v.ReplicatedTo, nodeId) {
				var firstVNode VNode
				var secondVNode VNode
				idRemovedNode := ""
				for _, cvn := range pt.VNodes {
					if cvn.StartToken == v.StartToken && cvn.EndToken < v.EndToken {
						//cvn was deleted and merged into next
						firstVNode = cvn
						idRemovedNode = firstVNode.NodeId
					} else if cvn.EndToken == v.EndToken {
						//next node into which cvn was merged
						secondVNode = cvn
						break
					}
				}

				if idRemovedNode != "" {
					nodesAccountedFor = append(nodesAccountedFor, firstVNode.VNodeID)
					nodesAccountedFor = append(nodesAccountedFor, secondVNode.VNodeID)
					if firstVNode.NodeId != nodeId && !slices.Contains(firstVNode.ReplicatedTo, nodeId) {
						//fetch this data
						t := calcFetchDataTaskDetails(v, firstVNode, nodeId, idRemovedNode)
						t.PartionTableTimestamp = uint64(newPartitionTable.Timestamp)
						tasks = append(tasks, t)
					}
					if secondVNode.NodeId != nodeId && !slices.Contains(secondVNode.ReplicatedTo, nodeId) {
						//fetch this data
						t := calcFetchDataTaskDetails(v, secondVNode, nodeId, idRemovedNode)
						t.PartionTableTimestamp = uint64(newPartitionTable.Timestamp)
						tasks = append(tasks, t)
					}

				}
			}
		}

		//anything to delete?
		for _, cvn := range pt.VNodes {
			if (cvn.NodeId == nodeId || slices.Contains(cvn.ReplicatedTo, nodeId)) && !slices.Contains(nodesAccountedFor, cvn.VNodeID) {
				for _, v := range newPartitionTable.VNodes {
					if cvn.StartToken >= v.StartToken && cvn.EndToken <= v.EndToken {
						if v.NodeId != nodeId && !slices.Contains(v.ReplicatedTo, nodeId) {
							t := DataSyncTask{
								StartToken:            cvn.StartToken,
								EndToken:              cvn.EndToken,
								Status:                NewTask,
								FromNodeId:            nodeId,
								ToNodeId:              nodeId,
								VNodeId:               cvn.VNodeID,
								Action:                DeleteData,
								Progress:              0,
								StepSize:              0,
								PartionTableTimestamp: uint64(newPartitionTable.Timestamp),
							}
							tasks = append(tasks, t)
						}
					}
				}
			}
		}
	} else {
		//node added
		for _, v := range newPartitionTable.VNodes {
			if v.NodeId == nodeId || slices.Contains(v.ReplicatedTo, nodeId) {
				isForReplication := false
				askToNodeId := ""
				skip := false
				if v.NodeId == nodeId {
					askToNodeId = v.PrevNodeID
					if v.PrevNodeID == nodeId {
						skip = true
					}
				} else {
					if slices.Contains(v.PrevReplicatedTo, nodeId) || v.PrevNodeID == nodeId {
						skip = true
					} else {
						isForReplication = true
						//if N1 is replicated at position N => ask data from PrevReplicatedTo[N]
						k := 0
						found := false
						for i, rn := range v.ReplicatedTo {
							if rn == nodeId {
								k = i
								found = true
							}
						}
						if found && len(v.PrevReplicatedTo) > k {
							askToNodeId = v.PrevReplicatedTo[k]
							if askToNodeId == nodeId {
								//find the first that is not nodeId
								found = false
								for _, rn := range v.PrevReplicatedTo {
									if rn != nodeId {
										askToNodeId = rn
										found = true
									}
								}
								if !found {
									askToNodeId = v.PrevNodeID
								}
							}
						} else {
							askToNodeId = v.PrevNodeID
						}
					}
				}

				if !skip {
					t := DataSyncTask{
						StartToken:            v.StartToken,
						EndToken:              v.EndToken,
						Status:                NewTask,
						FromNodeId:            nodeId,
						ToNodeId:              askToNodeId,
						VNodeId:               v.VNodeID,
						Action:                SendData,
						Progress:              0,
						StepSize:              0,
						PartionTableTimestamp: uint64(newPartitionTable.Timestamp),
					}
					if isForReplication {
						t.Action = SendDataForReplication
					}
					tasks = append(tasks, t)
				}
			} else if v.PrevNodeID == nodeId || slices.Contains(v.PrevReplicatedTo, nodeId) {
				t := DataSyncTask{
					StartToken:            v.StartToken,
					EndToken:              v.EndToken,
					Status:                NewTask,
					FromNodeId:            nodeId,
					ToNodeId:              nodeId,
					VNodeId:               v.VNodeID,
					Action:                DeleteData,
					Progress:              0,
					StepSize:              0,
					PartionTableTimestamp: uint64(newPartitionTable.Timestamp),
				}
				tasks = append(tasks, t)
			}

		}
	}

	printDataSyncTasks(tasks)
	fmt.Println()
	fmt.Println()

	pt.VNodes = newPartitionTable.VNodes

	return nil
}

func (pt *PartitionTable) Update(nodeId string, newPartitionTable *PartitionTable) error {

	current := make(map[string]VNode)
	existing := make(map[string]bool)

	for _, v := range pt.VNodes {
		if v.NodeId == nodeId {
			current[v.VNodeID] = v
			existing[v.VNodeID] = true
		}
	}

	//untouchedVnodes := make([]VNode, 0)
	for _, v := range newPartitionTable.VNodes {
		if v.NodeId == nodeId {
			if existing[v.VNodeID] {
				cvn := current[v.VNodeID]
				lcvn := cvn.EndToken - cvn.StartToken
				lv := v.EndToken - v.StartToken

				if lcvn > lv {
					//partition shrinked
					var start uint64 = 0
					if cvn.StartToken > v.StartToken {
						start = v.StartToken
					} else if cvn.StartToken <= v.StartToken {
						start = cvn.StartToken
					}
					var end uint64 = v.StartToken - 1
					v.StartToken = start

					v.EndToken = end

					to := ""
					for _, v1 := range newPartitionTable.VNodes {
						if v1.StartToken == start && v1.EndToken == end {
							to = v1.NodeId
						}
					}
					v.NodeId = to

					delete := true
					if slices.Contains(v.ReplicatedTo, nodeId) || len(v.ReplicatedTo) < 2 {
						delete = false
					}

					tvn := dataSyncAction{
						startToken: start,
						endToken:   end,
						status:     "streaming",
						fromNodeId: []string{},
						toNodeId:   to,
						actionType: "trim",
						progress:   0,
						stepSize:   (end - start) / 2,
						delete:     delete,
					}
					pt.dataSyncActionQueue = append(pt.dataSyncActionQueue, tvn)
				} else if lcvn < lv {
					//partition enlarged
					var start uint64 = v.StartToken
					var end uint64 = cvn.StartToken - 1
					v.StartToken = start
					v.EndToken = end

					//find who got this range before:
					from := []string{}
					for _, v1 := range pt.VNodes {
						if v1.StartToken == start && v1.EndToken == end {
							from = v1.ReplicatedTo
						}
					}

					v.PrevReplicatedTo = from

					tvn := dataSyncAction{
						startToken: start,
						endToken:   end,
						status:     "streaming",
						fromNodeId: from,
						toNodeId:   "",
						actionType: "expand",
						progress:   0,
						stepSize:   (end - start) / 2,
					}

					pt.dataSyncActionQueue = append(pt.dataSyncActionQueue, tvn)
				} else {
					//partition not changed
					//untouchedVnodes = append(untouchedVnodes, v)
				}
			} else {
				//new partition
				if v.PrevNodeID != "" && v.PrevNodeID != nodeId {
					tvn := dataSyncAction{
						startToken: v.StartToken,
						endToken:   v.EndToken,
						status:     "streaming",
						fromNodeId: []string{v.PrevNodeID},
						toNodeId:   "",
						actionType: "new",
						progress:   0,
						stepSize:   (v.EndToken - v.StartToken) / 2,
					}
					pt.dataSyncActionQueue = append(pt.dataSyncActionQueue, tvn)
				}

			}
		}
	}

	fmt.Println(nodeId + " ********************************** " + nodeId)
	printDataSyncActionQueue(pt.dataSyncActionQueue)
	fmt.Println()
	fmt.Println()

	pt.VNodes = newPartitionTable.VNodes

	return nil
}

func printDataSyncActionQueue(vnodes []dataSyncAction) {
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', tabwriter.AlignRight)

	s := "StartToken\t * \tEndToken\t * \tStatus\t * \tType\t * \tFrom\t * \tTo\t * \tProgress\t * \tDelete\t"
	fmt.Fprintln(w, s)

	for _, v := range vnodes {
		s := strconv.FormatUint(v.startToken, 10) + "\t * \t" + strconv.FormatUint(v.endToken, 10) + "\t * \t" + v.status + "\t * \t" + v.actionType + "\t * \t" + strings.Join(v.fromNodeId, ",") + "\t * \t" + v.toNodeId + " + " + strings.Join(v.toReplicas, ",") + "\t * \t" + strconv.FormatUint(v.progress, 10) + "\t * \t" + strconv.FormatBool(v.delete)
		fmt.Fprintln(w, s)
	}
	w.Flush()
}

func printDataSyncTasks(tasks []DataSyncTask) {
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', tabwriter.AlignRight)

	s := "StartToken\t * \tEndToken\t * \tStatus\t * \tType\t * \tFrom\t * \tTo\t * \tVNodeID\t * \tProgress"
	fmt.Fprintln(w, s)

	for _, v := range tasks {
		s := strconv.FormatUint(v.StartToken, 10) + "\t * \t" + strconv.FormatUint(v.EndToken, 10) + "\t * \t" + v.Status.String() + "\t * \t" + v.Action.String() + "\t * \t" + v.FromNodeId + "\t * \t" + v.ToNodeId + "\t * \t" + v.VNodeId + "\t * \t" + strconv.FormatUint(v.Progress, 10)
		fmt.Fprintln(w, s)
	}
	w.Flush()
}
