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

package actions

type Action uint8

const (
	Ping Action = iota
	NoAction
	JoinClusterRequest
	DecommissionNodeRequest
	RequestToBecomeMaster
	MasterRequestAccepted
	MasterRequestRejected
	GetPartitionTableRequest
	UpdatePartitionTableRequest
	ReleaseMasterRequest
	ForceReleaseMasterRequest
	CommitPartitionTableRequest //it also releases the master
	AbortPartitionTableChanges  //used to send messages to the requestor in case the op cannot be completed
	NodeBackOnlineNotification  //sent to notify a node that the sender is back online
	ClusterStatusRequest
	GetNodeStatsRequest
	StartDataStreamRequest //used during PT changes for sync
	SendDataStreamChunk
	SendDataStreamTaskCompleted
	SendGetSyncTasks //used to request list of sync tasks from a node
	RetrySyncTask
	Set
	Get
	Del
	Scan
	GetKeyLocation
	SendActionsLog //used to bring a resuscitated node up to date
)

// actions sent from the cluster to the current node
type NodeActions uint8

const (
	Shutdown NodeActions = iota
	NewPartitionTable
)
