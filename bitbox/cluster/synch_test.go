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

package cluster

import (
	"math"
	"os"
	"testing"

	"github.com/lrondanini/bit-box/bitbox/cluster/partitioner"
	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
)

func TestProcessTask(t *testing.T) {
	conf := utils.GetConfForTesting()
	err := os.RemoveAll(conf.DATA_FOLDER)
	if err != nil {
		t.Error(err)
	}
	node, err := InitNode(*conf)
	if err != nil {
		t.Error(err)
	}
	node.clusterManager.StartCommunications()
	defer node.Shutdown()
	node.clusterManager.JoinCluster(false)

	node.Upsert("tasks", "key1", "one")
	node.Upsert("tasks", "key2", "two")
	node.Upsert("tasks", "key3", "three")
	node.Upsert("tasks", "key4", "four")
	node.Upsert("tasks", "key5", "five")

	node.Upsert("rats", "key4", "four")
	node.Upsert("rats", "key5", "five")
	node.Upsert("rats", "key5", "five")

	task := partitioner.DataSyncTask{
		ID:                    "task-1",
		StartToken:            uint64(0),
		EndToken:              math.MaxUint64,
		Status:                partitioner.NewTask,
		FromNodeId:            "localhost:4444",
		ToNodeId:              "localhost:1111",
		VNodeId:               "vnode-1",
		Action:                partitioner.SendData,
		Progress:              uint64(1),
		ProgressCollection:    "rats",
		PartionTableTimestamp: uint64(0),
		Error:                 nil,
	}
	node.clusterManager.dataSyncManager.processStreamingTask(task)

	err = os.RemoveAll(conf.DATA_FOLDER)
	if err != nil {
		t.Error(err)
	}
}
