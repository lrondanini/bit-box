package partitioner

// import (
// 	"fmt"
// 	"log"
// 	"os"
// 	"strconv"
// 	"strings"
// 	"sync"

// 	"golang.org/x/exp/slices"
// )

// type commManager struct {
// 	channels map[string]chan msg
// }

// func initCommManager() *commManager {
// 	return &commManager{
// 		channels: make(map[string]chan msg),
// 	}
// }

// func (cm *commManager) addChannel(nodeId string, channel chan msg) {
// 	cm.channels[nodeId] = channel
// }

// func (cm *commManager) send(nodeId string, m msg) {
// 	cm.channels[nodeId] <- m
// }

// type fakeNode struct {
// 	nodeId       string
// 	partionTable PartitionTable
// 	commChannel  chan msg
// 	commManager  *commManager
// 	syncDataStop chan bool
// 	synching     bool
// 	syncMutex    sync.Mutex
// 	data         map[uint64]string
// }

// type msg struct {
// 	action     string
// 	startToken uint64
// 	endToken   uint64
// 	fromNodeId string
// 	progress   uint64
// 	isLast     bool
// 	data       map[uint64]string
// }

// func initFakeNode(nodeId string, cm *commManager) *fakeNode {
// 	d := make(map[uint64]string)
// 	if nodeId == "node-0" {
// 		// d[2305843009213693951] = "2305843009213693951"
// 		// d[3458764513820540927] = "3458764513820540927"
// 		// d[4611686018427387903] = "4611686018427387903"
// 		// d[5764607523034234879] = "5764607523034234879"
// 		// d[6917529027641081855] = "6917529027641081855"
// 		// d[9223372036854775807] = "9223372036854775807"
// 		// d[10376293541461622783] = "10376293541461622783"
// 		// d[11529215046068469759] = "11529215046068469759"
// 		// d[13835058055282163711] = "13835058055282163711"
// 		// d[16140901064495857663] = "16140901064495857663"
// 		// d[18446744073709551615] = "18446744073709551615"

// 		d[10] = "10"
// 		d[20] = "20"
// 		d[22] = "22"
// 		d[30] = "30"
// 		d[40] = "40"
// 		d[43] = "43"
// 		d[50] = "50"
// 		d[55] = "55"
// 		d[60] = "60"
// 		d[67] = "67"
// 		d[70] = "70"
// 		d[75] = "75"
// 		d[80] = "80"
// 		d[82] = "82"
// 		d[90] = "90"
// 		d[94] = "94"
// 		d[96] = "96"
// 	}
// 	return &fakeNode{
// 		nodeId:       nodeId,
// 		partionTable: *InitEmptyPartitionTable(),
// 		commChannel:  make(chan msg),
// 		commManager:  cm,
// 		syncDataStop: make(chan bool),
// 		synching:     false,
// 		data:         d,
// 	}
// }

// func (fn *fakeNode) start() {
// 	for {
// 		m := <-fn.commChannel

// 		if m.action == "new" {
// 			for k, sa := range fn.partionTable.dataSyncActionQueue {
// 				if sa.actionType == "new" || sa.actionType == "expand" {
// 					if sa.startToken <= m.startToken && sa.endToken >= m.endToken {
// 						fn.partionTable.dataSyncActionQueue[k].status = "streaming"
// 						fn.partionTable.dataSyncActionQueue[k].progress = m.progress

// 						d := m.data
// 						for k, v := range d {
// 							fn.data[k] = v
// 						}

// 						if m.isLast {
// 							fn.partionTable.dataSyncActionQueue[k].status = "done"
// 							fn.appendLogToFile("DONE - received data data from " + m.fromNodeId + " [" + strconv.FormatUint(m.progress, 10) + "] (" + strconv.FormatUint(m.startToken, 10) + " - " + strconv.FormatUint(m.endToken, 10) + ")")
// 						} else {
// 							fn.appendLogToFile("received data data from " + m.fromNodeId + " [" + strconv.FormatUint(m.progress, 10) + "] (" + strconv.FormatUint(m.startToken, 10) + " - " + strconv.FormatUint(m.endToken, 10) + ")")
// 						}
// 					}
// 				}
// 			}
// 		} else if m.action == "sendNodesRequest" {
// 			go fn.sendDatatoNode(m.fromNodeId, m.startToken, m.endToken)

// 		}

// 		// allDone := true
// 		// for _, sa := range fn.partionTable.dataSyncActionQueue {
// 		// 	if sa.status != "done" {
// 		// 		allDone = false
// 		// 	}
// 		// }

// 		// if allDone {
// 		// 	fmt.Println(fn.nodeId + " - SYNC RECEIVED")
// 		// 	fn.printData()
// 		// 	fmt.Println("----------")
// 		// }

// 	}
// }

// func (fn *fakeNode) getData(from uint64, to uint64, delete bool) map[uint64]string {

// 	tmp := make(map[uint64]string)
// 	d := make(map[uint64]string)

// 	for k, v := range fn.data {
// 		if k >= from && k <= to {
// 			d[k] = v
// 		} else if delete {
// 			tmp[k] = v
// 		}
// 	}

// 	if delete {
// 		fn.data = tmp
// 	}
// 	return d
// }

// func (fn *fakeNode) getAllData() map[uint64]string {
// 	return fn.data
// }

// func (fn *fakeNode) printData() {

// 	for k, v := range fn.data {
// 		ok := false
// 		for _, vn := range fn.partionTable.VNodes {
// 			if vn.NodeId == fn.nodeId && vn.StartToken <= k && vn.EndToken >= k {
// 				ok = true
// 				break
// 			}
// 		}

// 		if ok {
// 			fmt.Println(strconv.FormatUint(k, 10) + " = " + v)
// 		} else {
// 			fmt.Println("WRONG PLACE: ", strconv.FormatUint(k, 10)+" = "+v)
// 		}

// 	}

// }

// func (fn *fakeNode) updatePartitionTable(newPT *PartitionTable) {
// 	fn.syncMutex.Lock()
// 	if fn.synching {
// 		close(fn.syncDataStop)
// 	}
// 	fn.synching = true
// 	fn.syncMutex.Unlock()

// 	fn.partionTable.Update(fn.nodeId, newPT)
// 	fn.syncDataStop = make(chan bool)
// 	go fn.syncDataWithCluster(fn.syncDataStop)
// }

// func (fn *fakeNode) syncDataWithCluster(stopChan chan bool) {

// 	done := false

// 	for !done {
// 		allDone := true

// 		fn.appendLogToFile("SYNC STARTED: " + strconv.Itoa(len(fn.partionTable.dataSyncActionQueue)))
// 		for k, sa := range fn.partionTable.dataSyncActionQueue {
// 			s := strconv.FormatUint(sa.startToken, 10) + "\t * \t" + strconv.FormatUint(sa.endToken, 10) + "\t * \t" + sa.status + "\t * \t" + sa.actionType + "\t * \t" + strings.Join(sa.fromNodeId, ",") + "\t * \t" + sa.toNodeId + "\t * \t" + strconv.FormatUint(sa.progress, 10) + "\t"
// 			fn.appendLogToFile("=>  " + s)
// 			select {
// 			case <-stopChan:
// 				fn.appendLogToFile("SYNC INTERRUPTED - DONE")
// 				return
// 			default:
// 				if sa.status != "done" {
// 					if sa.actionType == "trim" {
// 						t := (sa.endToken - sa.startToken) / sa.stepSize
// 						if sa.progress <= t {
// 							allDone = false
// 							fromToken := sa.startToken + (sa.progress * sa.stepSize)
// 							toToken := fromToken + sa.stepSize
// 							if toToken > sa.endToken {
// 								toToken = sa.endToken
// 							}
// 							fn.partionTable.dataSyncActionQueue[k].progress = sa.progress + 1
// 							isLast := false
// 							if fn.partionTable.dataSyncActionQueue[k].progress > t {
// 								isLast = true
// 							}
// 							fn.appendLogToFile("SEND TO: " + sa.toNodeId)

// 							fn.commManager.send(sa.toNodeId, msg{
// 								action:     "new",
// 								startToken: fromToken,
// 								endToken:   toToken,
// 								fromNodeId: fn.nodeId,
// 								progress:   sa.progress,
// 								isLast:     isLast,
// 								data:       fn.getData(fromToken, toToken, sa.delete),
// 							})
// 							fn.appendLogToFile("SEND TO: " + sa.toNodeId + " DONE")

// 							if isLast {
// 								fn.partionTable.dataSyncActionQueue[k].status = "done"
// 								fn.appendLogToFile("DONE: " + fn.nodeId + " sending data to " + sa.toNodeId + " [" + strconv.FormatUint(sa.progress, 10) + "] (" + strconv.FormatUint(sa.startToken, 10) + " - " + strconv.FormatUint(sa.endToken, 10) + ")")

// 							} else {
// 								fn.appendLogToFile(fn.nodeId + " sending data to " + sa.toNodeId + " [" + strconv.FormatUint(sa.progress, 10) + "] (" + strconv.FormatUint(fromToken, 10) + " - " + strconv.FormatUint(toToken, 10) + ")")
// 							}
// 						}
// 					} else if sa.actionType == "expand" {
// 						allDone = false
// 						if slices.Contains(sa.fromNodeId, fn.nodeId) {
// 							fn.partionTable.dataSyncActionQueue[k].status = "done"
// 						} else {
// 							fn.commManager.send(sa.toNodeId, msg{
// 								action:     "sendNodesRequest",
// 								startToken: sa.startToken,
// 								endToken:   sa.endToken,
// 								fromNodeId: fn.nodeId,
// 							})

// 						}
// 					}
// 				}

// 			}
// 			//fn.commManager.send(sa, msg{action: "update"})
// 		}
// 		fn.appendLogToFile("SYNC LOOP DONE")
// 		done = allDone
// 	}

// 	fn.syncMutex.Lock()
// 	fn.synching = false
// 	fn.syncMutex.Unlock()
// }

// func (fn *fakeNode) sendDatatoNode(toNodeId string, from uint64, to uint64) {
// 	fn.commManager.send(toNodeId, msg{
// 		action:     "new",
// 		startToken: from,
// 		endToken:   to,
// 		fromNodeId: fn.nodeId,
// 		progress:   0,
// 		isLast:     true,
// 		data:       fn.getData(from, to, false),
// 	})
// }

// func (fn *fakeNode) appendLogToFile(str string) {
// 	//fn.syncMutex.Lock()
// 	f, err := os.OpenFile(fn.nodeId+".log",
// 		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		log.Println(err)
// 	}
// 	defer func() {
// 		f.Close()
// 		//fn.syncMutex.Unlock()
// 	}()
// 	if _, err := f.WriteString(str + "\n"); err != nil {
// 		log.Println(err)
// 	}
// }
