package types

import "github.com/lrondanini/bit-box/bitbox/cluster/partitioner"

type DataSyncTaskRequest struct {
	TaskId string
	From   uint64
	To     uint64
}

type CollectionStats struct {
	CollectionName  string
	NumberOfEntries uint64
	NumberOfUpserts uint64
	NumberOfReads   uint64
}

type NodeStatsResponse struct {
	Collections        []string
	StatsPerCollection map[string]CollectionStats
}

type DataSyncJob struct {
	PartitionTableTimestamp int64
	SynchTasks              []partitioner.DataSyncTask
	WaitingToStartDelete    bool
	GoDelete                bool
}

type DataSyncStatusResponse struct {
	JobsQueue      []DataSyncJob
	StreamingQueue []partitioner.DataSyncTask
}

type RWRequest struct {
	Collection string
	Key        []byte
	Value      []byte
}

type ScanRequest struct {
	Collection      string
	StartFromKey    []byte
	NumberOfResults int
}

type CollectionItem struct {
	Key   []byte
	Value []byte
}
