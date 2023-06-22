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
