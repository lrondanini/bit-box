package types

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
