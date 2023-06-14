package partitioner

import (
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"

	"github.com/rs/xid"
	"golang.org/x/exp/slices"
)

type DataSyncTask struct {
	ID                    string
	StartToken            uint64
	EndToken              uint64
	Status                DataSyncTaskStatus
	FromNodeId            string
	ToNodeId              string
	VNodeId               string
	Action                DataSyncAction
	Progress              uint64 //to save which data have been already sent
	ProgressCollection    string //to save at which collection the progress is
	PartionTableTimestamp uint64
	Error                 error
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
	SuspendedForError
	CompletedMessageNotSent
	Completed
)

func (s DataSyncTaskStatus) String() string {
	switch s {
	case NewTask:
		return "NewTask"
	case Streaming:
		return "Streaming"
	case SuspendedForError:
		return "SuspendedForError"
	case Completed:
		return "Completed"
	default:
		panic(fmt.Sprintf("unknown DataSyncActionOld: %d", s))
	}
}

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
		ID:                 xid.New().String(),
		StartToken:         oldVnode.StartToken,
		EndToken:           oldVnode.EndToken,
		Status:             NewTask,
		FromNodeId:         nodeId,
		ToNodeId:           askToNodeId,
		VNodeId:            vnode.VNodeID,
		Action:             action,
		Progress:           0,
		ProgressCollection: "",
	}

	return t
}

func (pt *PartitionTable) CalculateDataSyncTasks(nodeId string, newPartitionTable *PartitionTable) []DataSyncTask {
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
								ID:                    xid.New().String(),
								StartToken:            cvn.StartToken,
								EndToken:              cvn.EndToken,
								Status:                NewTask,
								FromNodeId:            nodeId,
								ToNodeId:              nodeId,
								VNodeId:               cvn.VNodeID,
								Action:                DeleteData,
								Progress:              0,
								ProgressCollection:    "",
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
					if v.PrevNodeID == nodeId || v.PrevNodeID == "" {
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
						ID:                    xid.New().String(),
						StartToken:            v.StartToken,
						EndToken:              v.EndToken,
						Status:                NewTask,
						FromNodeId:            nodeId,
						ToNodeId:              askToNodeId,
						VNodeId:               v.VNodeID,
						Action:                SendData,
						Progress:              0,
						ProgressCollection:    "",
						PartionTableTimestamp: uint64(newPartitionTable.Timestamp),
					}
					if isForReplication {
						t.Action = SendDataForReplication
					}
					tasks = append(tasks, t)
				}
			} else if v.PrevNodeID == nodeId || slices.Contains(v.PrevReplicatedTo, nodeId) {
				t := DataSyncTask{
					ID:                    xid.New().String(),
					StartToken:            v.StartToken,
					EndToken:              v.EndToken,
					Status:                NewTask,
					FromNodeId:            nodeId,
					ToNodeId:              nodeId,
					VNodeId:               v.VNodeID,
					Action:                DeleteData,
					Progress:              0,
					ProgressCollection:    "",
					PartionTableTimestamp: uint64(newPartitionTable.Timestamp),
				}
				tasks = append(tasks, t)
			}

		}
	}

	return tasks
}

func (pt *PartitionTable) calculateDataSyncTasksForTesting(nodeId string, newPartitionTable *PartitionTable) error {

	fmt.Println(nodeId + " ********************************** " + nodeId)

	tasks := pt.CalculateDataSyncTasks(nodeId, newPartitionTable)

	PrintDataSyncTasks(tasks)
	fmt.Println()
	fmt.Println()

	pt.VNodes = newPartitionTable.VNodes

	return nil
}

func PrintDataSyncTasks(tasks []DataSyncTask) {
	fmt.Println()
	w := tabwriter.NewWriter(os.Stdout, 1, 1, 1, ' ', tabwriter.AlignRight)

	s := "ID\t * \tStartToken\t * \tEndToken\t * \tStatus\t * \tType\t * \tFrom\t * \tTo\t * \tVNodeID\t * \tProgress\t * \tPT-Timestamp"
	fmt.Fprintln(w, s)

	for _, v := range tasks {
		s := v.ID + "\t * \t" + strconv.FormatUint(v.StartToken, 10) + "\t * \t" + strconv.FormatUint(v.EndToken, 10) + "\t * \t" + v.Status.String() + "\t * \t" + v.Action.String() + "\t * \t" + v.FromNodeId + "\t * \t" + v.ToNodeId + "\t * \t" + v.VNodeId + "\t * \t" + strconv.FormatUint(v.Progress, 10) + "\t * \t" + strconv.FormatUint(v.PartionTableTimestamp, 10)
		fmt.Fprintln(w, s)
	}
	w.Flush()
}
