package cluster

import (
	"strconv"
	"sync"

	"github.com/lrondanini/bit-box/bitbox/cluster/partitioner"
	"github.com/lrondanini/bit-box/bitbox/cluster/stream"
	"github.com/lrondanini/bit-box/bitbox/communication/types"
	"github.com/lrondanini/bit-box/bitbox/storage"
)

const JOBS_KEY_NAME = "syncJobs"           //tasks related to new PTs
const STREAMING_KEY_NAME = "streamingJobs" //sending data to other nodes

type DataSyncJob struct {
	PartitionTable       partitioner.PartitionTable
	SynchTasks           []partitioner.DataSyncTask
	WaitingToStartDelete bool
	GoDelete             bool
}

type DataSyncManager struct {
	clusterManager     *ClusterManager
	jobsQueue          []DataSyncJob
	streamingQueue     []partitioner.DataSyncTask
	syncMutex          sync.Mutex
	streamingTasksChan chan partitioner.DataSyncTask
}

func InitDataSyncManager(clusterManager *ClusterManager) (*DataSyncManager, error) {
	jobsQueue := []DataSyncJob{}

	err := clusterManager.systemDb.Get(JOBS_KEY_NAME, &jobsQueue)
	if err != nil {
		if err != storage.ErrKeyNotFound {
			return nil, err
		}
	}

	streamingQueue := []partitioner.DataSyncTask{}
	err = clusterManager.systemDb.Get(STREAMING_KEY_NAME, &streamingQueue)
	if err != nil {
		if err != storage.ErrKeyNotFound {
			return nil, err
		}
	}

	// fmt.Println("SYNCH TASKS:")
	// for _, job := range jobsQueue {
	// 	partitioner.PrintDataSyncTasks(job.SynchTasks)
	// }

	// partitioner.PrintDataSyncTasks(streamingQueue)

	return &DataSyncManager{
		clusterManager:     clusterManager,
		jobsQueue:          jobsQueue,
		streamingQueue:     streamingQueue,
		streamingTasksChan: make(chan partitioner.DataSyncTask, 1000),
	}, nil
}

// false if node is waiting for data
func (dsm *DataSyncManager) CanSetPartitionTable() bool {
	dsm.syncMutex.Lock()
	defer dsm.syncMutex.Unlock()
	canSet := true

	for _, job := range dsm.jobsQueue {
		for _, task := range job.SynchTasks {
			if task.Action == partitioner.SendData || task.Action == partitioner.SendDataForReplication {
				canSet = false
			}
		}
	}

	return canSet
}

func (dsm *DataSyncManager) IsWaitingForData(hash uint64) (bool, string) {
	dsm.syncMutex.Lock()
	defer dsm.syncMutex.Unlock()
	for _, job := range dsm.jobsQueue {
		for _, task := range job.SynchTasks {
			if task.Action == partitioner.SendData || task.Action == partitioner.SendDataForReplication {
				if task.StartToken <= hash && hash <= task.EndToken {
					return true, task.ToNodeId
				}
			}
		}
	}

	return false, ""
}

func (dsm *DataSyncManager) GetStatus() types.DataSyncStatusResponse {
	jobsQueue := []types.DataSyncJob{}
	dsm.syncMutex.Lock()
	for _, job := range dsm.jobsQueue {
		j := types.DataSyncJob{
			PartitionTableTimestamp: job.PartitionTable.Timestamp,
			WaitingToStartDelete:    job.WaitingToStartDelete,
			GoDelete:                job.GoDelete,
		}
		j.SynchTasks = append(j.SynchTasks, job.SynchTasks...)
		jobsQueue = append(jobsQueue, j)
	}
	dsm.syncMutex.Unlock()
	return types.DataSyncStatusResponse{
		JobsQueue:      jobsQueue,
		StreamingQueue: dsm.streamingQueue,
	}
}

func (dsm *DataSyncManager) InitUpdatePartitionTableProcess(pt *partitioner.PartitionTable) error {
	newJob := DataSyncJob{
		PartitionTable: *pt,
		SynchTasks:     dsm.clusterManager.partitionTable.CalculateDataSyncTasks(dsm.clusterManager.currentNode.GetId(), pt),
	}

	if len(newJob.SynchTasks) == 0 {
		dsm.clusterManager.currentNode.UpdateHeartbitPartitionTable(pt.Timestamp)
	} else {
		hasFetchDataTasks := false
		for _, task := range newJob.SynchTasks {
			if task.Action == partitioner.SendData || task.Action == partitioner.SendDataForReplication {
				hasFetchDataTasks = true
			}
		}
		if !hasFetchDataTasks {
			newJob.WaitingToStartDelete = true
		}

		dsm.syncMutex.Lock()
		dsm.jobsQueue = append(dsm.jobsQueue, newJob)
		dsm.clusterManager.systemDb.Set(JOBS_KEY_NAME, dsm.jobsQueue)
		dsm.syncMutex.Unlock()

		if !hasFetchDataTasks {
			dsm.clusterManager.currentNode.UpdateHeartbitPartitionTable(pt.Timestamp) //verify anyway, this node could be the last one to update its PT
			dsm.VerifyClusterSyncWihtPartionTable()
		}
	}

	return nil
}

// Processes one task (the first) from the queue, when task is completed calls ProcessNextGetDataTask again
func (dsm *DataSyncManager) ProcessNextGetDataTask() {
	dsm.syncMutex.Lock()

	fetchDataTasks := []partitioner.DataSyncTask{}

	for _, job := range dsm.jobsQueue {
		for _, task := range job.SynchTasks {
			if task.Action == partitioner.SendData || task.Action == partitioner.SendDataForReplication {
				fetchDataTasks = append(fetchDataTasks, task)
			}
		}
	}
	dsm.syncMutex.Unlock()

	if len(fetchDataTasks) > 0 {
		nextTask := fetchDataTasks[0]
		dsm.getData(nextTask)
	}
}

func (dsm *DataSyncManager) getData(t partitioner.DataSyncTask) {

	dsm.changeTaskStatus(t.ID, partitioner.Streaming)

	dsm.clusterManager.logger.Info("Start streaming data from node: " + t.ToNodeId + " from token: " + strconv.FormatUint(t.StartToken, 10) + " to token: " + strconv.FormatUint(t.EndToken, 10))

	e := dsm.clusterManager.commManager.SendStartDataStreamRequest(t.ID, t.ToNodeId, t.StartToken, t.EndToken)
	if e != nil {
		dsm.changeTaskStatus(t.ID, partitioner.SuspendedForError)
		dsm.clusterManager.logger.Error(e, "Could not start sync with node: "+t.ToNodeId)
	}
}

func (dsm *DataSyncManager) SaveNewDataChunk(taskId string, collectionName string, progress uint64, data []stream.StreamEntry) {
	dsm.syncMutex.Lock()
	for k, job := range dsm.jobsQueue {
		for i, task := range job.SynchTasks {
			if task.ID == taskId {
				dsm.jobsQueue[k].SynchTasks[i].Progress = progress
			}
		}
	}
	dsm.clusterManager.systemDb.Set(JOBS_KEY_NAME, dsm.jobsQueue)
	dsm.syncMutex.Unlock()

	//save to db:
	dsm.clusterManager.currentNode.upsertFromClusterStream(collectionName, data)
}

func (dsm *DataSyncManager) changeTaskStatus(taskId string, status partitioner.DataSyncTaskStatus) {
	dsm.syncMutex.Lock()
	defer dsm.syncMutex.Unlock()
	for k, job := range dsm.jobsQueue {
		for i, task := range job.SynchTasks {
			if task.ID == taskId {
				dsm.jobsQueue[k].SynchTasks[i].Status = status
			}
		}
	}
	dsm.clusterManager.systemDb.Set(JOBS_KEY_NAME, dsm.jobsQueue)
}

// remove tasks and jobs from queue
func (dsm *DataSyncManager) TaskCompleted(taskId string) {
	dsm.syncMutex.Lock()

	tmpJobs := []DataSyncJob{}
	completedSyncPtTimestamp := int64(0)
	pendingTasks := false
	for _, job := range dsm.jobsQueue {
		tmpTasks := []partitioner.DataSyncTask{}
		hasFetchDataTasks := false
		for _, task := range job.SynchTasks {
			if task.ID != taskId {
				tmpTasks = append(tmpTasks, task)
				if task.Action == partitioner.SendData || task.Action == partitioner.SendDataForReplication {
					hasFetchDataTasks = true
				}
			}
		}

		if len(tmpTasks) > 0 {
			if !hasFetchDataTasks {
				job.WaitingToStartDelete = true
			}
			pendingTasks = true
			job.SynchTasks = tmpTasks
			tmpJobs = append(tmpJobs, job)
		}

		if !hasFetchDataTasks {
			completedSyncPtTimestamp = job.PartitionTable.Timestamp
		}
	}
	dsm.jobsQueue = tmpJobs
	dsm.clusterManager.systemDb.Set(JOBS_KEY_NAME, dsm.jobsQueue)
	dsm.syncMutex.Unlock()

	if completedSyncPtTimestamp > 0 {
		//ok, all fetches completed => update heartbit to the new PT, wait for all nodes to update their PTs to this (or newer), then delete data for this PT
		dsm.clusterManager.currentNode.UpdateHeartbitPartitionTable(completedSyncPtTimestamp)
		//verify anyway, this node could be the last one to update its PT
		dsm.VerifyClusterSyncWihtPartionTable()
	}

	if pendingTasks {
		//move on to fetch any other pending task
		dsm.ProcessNextGetDataTask()
	}

}

func (dsm *DataSyncManager) VerifyClusterSyncWihtPartionTable() {
	servers := dsm.clusterManager.GetServers()
	dsm.syncMutex.Lock()
	tmpJobs := []DataSyncJob{}
	launchDeleteProcess := false
	for _, job := range dsm.jobsQueue {
		if job.WaitingToStartDelete {
			//verify that all servers were aligned
			goDelete := true
			for _, s := range servers {
				if s.NodeId != dsm.clusterManager.currentNode.GetId() {
					//fmt.Println(s.NodeId, s.PartitionTableTimestamp, job.PartitionTable.Timestamp)
					if s.PartitionTableTimestamp < job.PartitionTable.Timestamp {
						goDelete = false
					}
				}
			}

			if goDelete {
				launchDeleteProcess = true
			}
			job.GoDelete = goDelete

		}
		tmpJobs = append(tmpJobs, job)
	}
	dsm.jobsQueue = tmpJobs
	dsm.syncMutex.Unlock()

	if launchDeleteProcess {
		go dsm.ProcessDataDeleteTasks()
	}
}

func (dsm *DataSyncManager) ProcessDataDeleteTasks() {
	dsm.syncMutex.Lock()
	deleteDataTasks := []partitioner.DataSyncTask{}

	for _, job := range dsm.jobsQueue {
		if job.GoDelete {
			for _, task := range job.SynchTasks {
				if task.Action == partitioner.DeleteData {
					deleteDataTasks = append(deleteDataTasks, task)
				}
			}
		}
	}
	dsm.syncMutex.Unlock()

	if len(deleteDataTasks) > 0 {
		for _, t := range deleteDataTasks {
			dsm.deleteData(t)
		}
	}

}

func (dsm *DataSyncManager) deleteData(t partitioner.DataSyncTask) {
	dsm.clusterManager.logger.Info("Start deleting data from " + strconv.FormatUint(t.StartToken, 10) + " to " + strconv.FormatUint(t.EndToken, 10))

	node := dsm.clusterManager.currentNode

	var it *storage.Iterator

	defer func() {
		if it != nil {
			it.Close()
		}
	}()

	for _, c := range node.GetStats().Collections {
		it, _ = node.GetIterator(c)
		var batch [][]byte

		for it.HasMore() {
			hash, k, _, e := it.NextRaw()
			if e == nil {
				if hash >= t.StartToken && hash <= t.EndToken {
					//delete
					batch = append(batch, k)
				}
				if len(batch) >= 1000 {
					node.deleteForClusterSync(c, batch)
				}
			}
		}

		//process last batch
		if len(batch) >= 0 {
			node.deleteForClusterSync(c, batch)
		}

		it.Close()
	}

	dsm.TaskCompleted(t.ID)
}

// ********** STREAMING DATA TO NODE: ************

func (dsm *DataSyncManager) updateStreamingTask(t partitioner.DataSyncTask) {

	dsm.syncMutex.Lock()
	tmp := []partitioner.DataSyncTask{}
	for i, task := range dsm.streamingQueue {
		if task.ID == t.ID {
			if t.Status != partitioner.Completed {
				dsm.streamingQueue[i] = t
				tmp = append(tmp, task)
			}
		} else {
			tmp = append(tmp, task)
		}
	}
	dsm.streamingQueue = tmp
	dsm.clusterManager.systemDb.Set(STREAMING_KEY_NAME, dsm.streamingQueue)
	dsm.syncMutex.Unlock()
}

func (dsm *DataSyncManager) AddStreamingTask(taskId string, toNodeId string, startToken uint64, endToken uint64) {

	dsm.clusterManager.logger.Info("New streaming task (ID:" + taskId + ") from node: " + toNodeId + " tokens: " + strconv.FormatUint(startToken, 10) + " - " + strconv.FormatUint(endToken, 10))
	newTask := partitioner.DataSyncTask{
		ID:                 taskId,
		StartToken:         startToken,
		EndToken:           endToken,
		Status:             partitioner.NewTask,
		FromNodeId:         dsm.clusterManager.currentNode.GetId(),
		ToNodeId:           toNodeId,
		Progress:           0,
		ProgressCollection: "",
	}

	dsm.syncMutex.Lock()
	dsm.streamingQueue = append(dsm.streamingQueue, newTask)
	dsm.clusterManager.systemDb.Set(STREAMING_KEY_NAME, dsm.streamingQueue)
	dsm.syncMutex.Unlock()

	dsm.streamingTasksChan <- newTask
}

func (dsm *DataSyncManager) StartStreaming() {
	go dsm.startStreamingManager()

	for _, t := range dsm.streamingQueue {
		dsm.streamingTasksChan <- t
	}

}

func (dsm *DataSyncManager) startStreamingManager() {
	for t := range dsm.streamingTasksChan {
		if t.Status != partitioner.SuspendedForError && t.Status != partitioner.Completed {
			// to keep things simple before doing proper testing, lets just process one task at the time
			dsm.processStreamingTask(t)
		}
	}
}

func (dsm *DataSyncManager) ForceRetryStreamingTask(taskId string) {
	for i, t := range dsm.streamingQueue {
		if t.ID == taskId {
			dsm.streamingQueue[i].Status = partitioner.Streaming
			dsm.streamingTasksChan <- dsm.streamingQueue[i]
		}
	}
}

func (dsm *DataSyncManager) processStreamingTask(t partitioner.DataSyncTask) {

	var e error

	if t.Status != partitioner.CompletedMessageNotSent {
		const DATA_SIZE_PER_FRAME = 5000

		node := dsm.clusterManager.currentNode

		var it *storage.Iterator
		var hash uint64
		var k, v []byte

		defer func() {
			if it != nil {
				it.Close()
			}
		}()

		startFromCollection := t.ProgressCollection
		start := false

		for _, cn := range node.GetStats().Collections {

			if startFromCollection == "" {
				start = true
			} else if cn == startFromCollection {
				start = true
			}

			if start {
				it, e = node.GetIterator(cn)

				if e != nil {
					dsm.clusterManager.logger.Error(e, "Could not get iterator for collection: "+cn)
					t.Status = partitioner.SuspendedForError
					t.Error = e
					dsm.updateStreamingTask(t)
					return
				} else {
					data := make([]stream.StreamEntry, 0)

					counter := uint64(0)

					for it.HasMore() {
						hash, k, v, e = it.NextRaw()

						if hash >= t.StartToken && hash <= t.EndToken {
							if e != nil {
								dsm.clusterManager.logger.Error(e, "Could not get value for collection: "+cn)
								t.Status = partitioner.SuspendedForError
								t.Error = e
								dsm.updateStreamingTask(t)
								return
							} else {
								//need to make a copy of key because key is reused and slices are inherently reference-y things
								a := make([]byte, len(k))
								copy(a, k)
								b := make([]byte, len(v))
								copy(b, v)
								data = append(data, stream.StreamEntry{Key: a, Value: b})

								if len(data) >= DATA_SIZE_PER_FRAME {
									if counter >= t.Progress {
										//send to node
										t.Progress++
										t.ProgressCollection = cn
										t.Status = partitioner.Streaming

										msg := stream.StreamMessage{
											TaskId:         t.ID,
											CollectionName: t.ProgressCollection,
											Progress:       t.Progress,
											Data:           data,
										}

										e = dsm.clusterManager.commManager.SendDataStreamChunk(t.ToNodeId, msg)

										if e != nil {
											t.Status = partitioner.SuspendedForError
											t.Error = e
											dsm.clusterManager.logger.Error(e, "Could not send data to node: "+t.ToNodeId)
											dsm.updateStreamingTask(t)
											return
										} else {
											dsm.updateStreamingTask(t)
										}
									}

									counter++

									//reset data
									data = make([]stream.StreamEntry, 0)
								}
							}

						}
					}

					it.Close()

					if len(data) > 0 {
						//send remaining data to node
						t.Progress++
						t.ProgressCollection = cn
						t.Status = partitioner.Streaming

						msg := stream.StreamMessage{
							TaskId:         t.ID,
							CollectionName: t.ProgressCollection,
							Progress:       t.Progress,
							Data:           data,
						}

						e = dsm.clusterManager.commManager.SendDataStreamChunk(t.ToNodeId, msg)

						if e != nil {
							t.Status = partitioner.SuspendedForError
							t.Error = e
							dsm.clusterManager.logger.Error(e, "Could not send data to node: "+t.ToNodeId)
							dsm.updateStreamingTask(t)
							return
						}
					}

					t.Progress = 0
					t.ProgressCollection = ""
					t.Status = partitioner.Streaming

					dsm.updateStreamingTask(t)
				}

			}

		}

	}

	//send task completed message to node
	e = dsm.clusterManager.commManager.SendDataStreamTaskCompleted(t.ToNodeId, t.ID)

	if e != nil {
		t.Status = partitioner.CompletedMessageNotSent
		t.Error = e
		dsm.clusterManager.logger.Error(e, "Could not send data to node: "+t.ToNodeId)
		dsm.updateStreamingTask(t)
	} else {
		t.Status = partitioner.Completed
		dsm.updateStreamingTask(t)

	}

}
