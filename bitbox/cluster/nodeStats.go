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

	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
	"github.com/lrondanini/bit-box/bitbox/storage"
	"golang.org/x/exp/slices"
)

type CollectionStats struct {
	CollectionName  string
	NumberOfEntries uint64
	NumberOfUpserts uint64
	NumberOfReads   uint64
}

type NodeStats struct {
	Collections        []string
	StatsPerCollection map[string]CollectionStats
	statsDb            *storage.Collection
	logger             *utils.InternalLogger
	statsEvents        chan Event
}

type Event struct {
	EventType      EventType
	CollectionName string
	IsNew          bool
}

type EventType uint8

const (
	UpsertEvent EventType = iota
	DeleteEvent
	ReadEvent
)

const collection_db_entry = "sys_all_collections"

func InitNodeStats() (*NodeStats, error) {
	logger := utils.GetLogger()
	statsDb, err := storage.OpenCollection(storage.NODE_STATS_DB_NAME)
	if err != nil {
		return nil, err
	}

	//load stats:
	allCollections := []string{}
	err = statsDb.Get(collection_db_entry, &allCollections)
	if err != nil {
		if err != storage.ErrKeyNotFound {
			return nil, err
		}
	}

	statsPerCollection := make(map[string]CollectionStats)
	for _, c := range allCollections {
		var cs CollectionStats
		err = statsDb.Get(c, &cs)
		if err != nil {
			logger.Error(err, "Could not load stats for collection: "+c)
		} else {
			statsPerCollection[c] = cs
		}
	}

	ns := NodeStats{
		Collections:        allCollections,
		StatsPerCollection: statsPerCollection,
		statsDb:            statsDb,
		logger:             logger,
		statsEvents:        make(chan Event, 1000),
	}

	go ns.EventsMonitor()

	return &ns, nil
}

func (ns *NodeStats) GetTotalNumberOfEntries() uint64 {
	res := uint64(0)

	for _, cs := range ns.StatsPerCollection {
		res += cs.NumberOfEntries
	}

	return res
}

func (ns *NodeStats) Shutdown() {
	ns.statsDb.Close()
	close(ns.statsEvents)
}

func (ns *NodeStats) Log(e Event) {
	ns.statsEvents <- e
}

func (ns *NodeStats) EventsMonitor() {
	for e := range ns.statsEvents {
		switch e.EventType {
		case UpsertEvent:
			ns.registerUpsert(e.CollectionName, e.IsNew)
		case DeleteEvent:
			ns.registerDelete(e.CollectionName)
		case ReadEvent:
			ns.registerRead(e.CollectionName)
		}
	}
}

func (ns *NodeStats) registerUpsert(collectionName string, isNew bool) {
	if !slices.Contains(ns.Collections, collectionName) {
		ns.Collections = append(ns.Collections, collectionName)
		ns.statsDb.Set(collection_db_entry, ns.Collections)
	}

	cs := ns.StatsPerCollection[collectionName]
	cs.NumberOfUpserts++
	if isNew {
		cs.NumberOfEntries++
	}
	ns.StatsPerCollection[collectionName] = cs
	ns.statsDb.Set(collectionName, cs)
}

func (ns *NodeStats) registerRead(collectionName string) {
	cs := ns.StatsPerCollection[collectionName]
	cs.NumberOfReads++
	ns.StatsPerCollection[collectionName] = cs
	ns.statsDb.Set(collectionName, cs)
}

func (ns *NodeStats) registerDelete(collectionName string) {
	cs := ns.StatsPerCollection[collectionName]
	cs.NumberOfEntries--
	if cs.NumberOfEntries == math.MaxUint64 {
		cs.NumberOfEntries = 0
	}
	ns.StatsPerCollection[collectionName] = cs
	ns.statsDb.Set(collectionName, cs)
}
