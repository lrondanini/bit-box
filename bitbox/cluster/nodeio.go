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
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/lrondanini/bit-box/bitbox/cluster/partitioner"
	"github.com/lrondanini/bit-box/bitbox/cluster/stream"
	"github.com/lrondanini/bit-box/bitbox/communication/types"
	"github.com/lrondanini/bit-box/bitbox/storage"
	"golang.org/x/exp/slices"
)

type Event struct {
	Type       EventType
	Collection string
	Key        []byte
}

func (e *Event) ToString() string {
	return fmt.Sprintf("Type: %s, Collection: %s, Key: %s", e.Type.String(), e.Collection, string(e.Key))
}

type EventType uint8

const (
	Insert EventType = iota
	Update
	Delete
)

func (s EventType) String() string {
	switch s {
	case Insert:
		return "Insert"
	case Update:
		return "Update"
	case Delete:
		return "Delete"
	default:
		panic(fmt.Sprintf("unknown EventType: %d", s))
	}
}

func (n *Node) GetKeyLocationInCluster(key []byte) partitioner.HashLocation {
	hash := partitioner.GetHash(key)
	return n.clusterManager.partitionTable.GetLocation(hash)
}

func (n *Node) convertKeyValueToBytes(key interface{}, value interface{}) ([]byte, []byte, error) {

	bKey, err := storage.ToBytes(key)

	if err != nil {
		return nil, nil, err
	}

	bValue, err := storage.ToBytes(value)

	if err != nil {
		return nil, nil, err
	}

	return bKey, bValue, nil
}

func (n *Node) Upsert(collectionName string, key interface{}, value interface{}) error {

	kBytes, vBytes, err := n.convertKeyValueToBytes(key, value)
	if err != nil {
		return err
	}

	return n.UpsertRaw(n.GetId(), collectionName, kBytes, vBytes)
}

func (n *Node) UpsertRaw(fromNodeId string, collectionName string, key []byte, value []byte) (err error) {

	timestamp := time.Now().UTC().UnixMicro()

	hash := partitioner.GetHash(key)

	//find WHERE to store this data
	isLocal := true
	skipReplicas := false

	loc := n.clusterManager.partitionTable.GetLocation(hash)

	if loc.Master != n.GetId() {
		isLocal = false
		if fromNodeId == loc.Master || slices.Contains(loc.Replicas, fromNodeId) {
			//sender knows that this node is a replica, we can write but we cannot replicate (to avoid an infine loop)
			isLocal = true
			skipReplicas = true
		}
	}

	if isLocal {
		err = n.performUpsert(collectionName, hash, key, value, timestamp)

		//upsert replicas
		if !skipReplicas && err == nil {
			for _, r := range loc.Replicas {
				if r != "" && r != n.GetId() {
					err = n.clusterManager.commManager.SendSet(r, collectionName, key, value)
					if err != nil {
						//write to log for r recovery
						storage.AppendToWal(r, storage.SET, collectionName, key, value, time.Now().UTC().UnixMicro())
					}
				}
			}
		}
	} else {
		err = n.clusterManager.commManager.SendSet(loc.Master, collectionName, key, value)
		if err != nil {
			//master is down
			if slices.Contains(loc.Replicas, n.GetId()) {
				//master is down, this node is temp master for this write

				//WRITE LOCALLY
				err = n.performUpsert(collectionName, hash, key, value, timestamp)

				//WRITE TO LOG FOR MASTER RECOVERY
				storage.AppendToWal(loc.Master, storage.SET, collectionName, key, value, time.Now().UTC().UnixMicro())

				if err == nil {
					//SEND TO OTHER REPLICA
					for _, r := range loc.Replicas {
						if r != "" && r != n.GetId() {
							err = n.clusterManager.commManager.SendSet(r, collectionName, key, value)
							if err != nil {
								//write to log for r recovery
								storage.AppendToWal(r, storage.SET, collectionName, key, value, time.Now().UTC().UnixMicro())
							}
						}
					}
				}
			} else {
				//contact the replicas
			SEND_TO_FIRST_REPLICA:
				for _, r := range loc.Replicas {
					if r != "" && r != n.GetId() {
						err = n.clusterManager.commManager.SendSet(r, collectionName, key, value)
						if err == nil {
							break SEND_TO_FIRST_REPLICA
						}
					}
				}
			}
		}
	}

	return err
}

func (n *Node) performUpsert(collectionName string, hash uint64, key []byte, value []byte, timestamp int64) error {
	c, err := n.storageManager.GetCollection(collectionName)

	if err == nil {
		exists, _ := c.Exists(key)

		skipUpsert := false
		if exists {
			var vBytes []byte

			vBytes, err = c.GetRaw(key)
			if err == nil {
				tmpV := storage.DbValue{}
				err = storage.DecodeValue(vBytes, &tmpV)
				if err == nil {
					if tmpV.Timestamp > timestamp {
						skipUpsert = true
					}
				}
			}
		}

		if !skipUpsert {
			v := storage.DbValue{
				Hash:      hash,
				Timestamp: timestamp,
				Value:     value,
			}

			vBytes, err := storage.EncodeValue(v)

			if err == nil {
				c.SetRaw(key, vBytes)

				ev := statEvent{
					EventType:      UpsertEvent,
					CollectionName: collectionName,
					IsNew:          !exists,
				}
				n.nodeStats.Log(ev)

				if exists {
					n.fireEvent(collectionName, Update, key)
				} else {
					n.fireEvent(collectionName, Insert, key)
				}

			}
		}

	}

	return err
}

func (n *Node) upsertFromClusterStream(collectionName string, data []stream.StreamEntry) error {
	c, e := n.storageManager.GetCollection(collectionName)
	if e != nil {
		return e
	}

	c.UpsertFromStreaming(data, func(collection string, key []byte) bool {
		//find out if this key was already marked to be deleted
		sKey, _ := storage.ToString(key)
		sBytes, _ := storage.ToBytes(collectionName + sKey)

		n.streamSync.Lock()
		res, _ := n.streamSyncDeletesCollections.Exists(sBytes)
		if res {
			n.streamSyncDeletesCollections.Delete(sBytes)
		}
		n.streamSync.Unlock()

		if !res {
			n.fireEvent(collectionName, Update, key)
		}
		return res
	}, func() {
		//update stats
		ev := statEvent{
			EventType:      UpsertEvent,
			CollectionName: collectionName,
			IsNew:          true,
		}
		n.nodeStats.Log(ev)

	})

	//verify that no new entry was added to streamSyncDeletesCollections
	it, err := n.streamSyncDeletesCollections.GetIteratorFrom(collectionName)

	if err == nil {
		for it.HasMoreWithPrefix(collectionName) {
			key := []byte{}
			keyToDelete := []byte{}

			it.Next(&key, &keyToDelete)
			n.streamSyncDeletesCollections.Delete(key)
			c.Delete(keyToDelete)
		}
	}
	return nil
}

func (n *Node) skipInsertFromStreamForEntry(collectionName string, key []byte) {
	n.streamSync.Lock()
	sKey, _ := storage.ToString(key)
	n.streamSyncDeletesCollections.Set(collectionName+sKey, key)
	n.streamSync.Unlock()
}

func (n *Node) deleteForClusterSync(collectionName string, keys [][]byte) error {
	c, e := n.storageManager.GetCollection(collectionName)
	if e != nil {
		return e
	}

	return c.DeleteKeys(keys, func(k []byte) {
		ev := statEvent{
			EventType:      DeleteEvent,
			CollectionName: collectionName,
		}
		n.nodeStats.Log(ev)
		n.fireEvent(collectionName, Delete, k)
	})
}

func (n *Node) Get(collectionName string, key interface{}, value interface{}) error {
	vv := reflect.ValueOf(value)
	if vv.Type().Kind() != reflect.Pointer {
		return errors.New("attempt to decode into a non-pointer")
	}

	bKey, err := storage.ToBytes(key)

	if err != nil {
		return err
	}
	res, err := n.GetRaw(n.GetId(), collectionName, bKey)
	if err != nil {
		return err
	}

	value = storage.FromBytes(res, value)

	return nil
}

func (n *Node) GetRaw(fromNodeId string, collectionName string, key []byte) ([]byte, error) {

	hash := partitioner.GetHash(key)

	//find WHERE to get this data
	isLocal := true

	loc := n.clusterManager.partitionTable.GetLocation(hash)
	if loc.Master != n.GetId() && !slices.Contains(loc.Replicas, n.GetId()) {
		isLocal = false
	} else if loc.Master == fromNodeId {
		//fromNodeId can ask this node to get a key if fromNodeId knows that there is streaming/synching going on
		isLocal = true
	}

	if isLocal {
		c, e := n.storageManager.GetCollection(collectionName)

		if e != nil {
			return nil, e
		}

		var vBytes []byte

		vBytes, e = c.GetRaw(key)
		if e != nil {
			if e == storage.ErrKeyNotFound {
				//verify that vnode associated with this key is not synching, if it is, we need to ask the remote node to send us the data
				waiting, askToNodeId := n.clusterManager.dataSyncManager.IsWaitingForData(hash)

				if waiting && askToNodeId != "" {
					v, e := n.clusterManager.commManager.SendGet(askToNodeId, collectionName, key)
					if e != nil {
						return nil, e
					}
					return v, nil
				} else {
					return nil, e
				}
			} else {
				return nil, e
			}

		}

		v := storage.DbValue{}

		e = storage.DecodeValue(vBytes, &v)
		if e != nil {
			return nil, e
		}

		vBytes = v.Value //.([]byte)

		ev := statEvent{
			EventType:      ReadEvent,
			CollectionName: collectionName,
		}
		n.nodeStats.Log(ev)

		return vBytes, nil
	} else {
		v, e := n.clusterManager.commManager.SendGet(loc.Master, collectionName, key)
		if e != nil {
			//try replicas:
			for _, r := range loc.Replicas {
				v, e := n.clusterManager.commManager.SendGet(r, collectionName, key)
				if e == nil {
					return v, nil
				}
			}
			return nil, e
		}
		return v, nil
	}
}

func (n *Node) Delete(collectionName string, key interface{}) error {
	bKey, err := storage.ToBytes(key)

	if err != nil {
		return err
	}

	return n.DeleteRaw(n.GetId(), collectionName, bKey)
}

func (n *Node) performDelete(fromNodeId string, hash uint64, collectionName string, key []byte) (err error) {
	c, err := n.storageManager.GetCollection(collectionName)

	if err == nil {
		exists, _ := c.Exists(key)

		if !exists {
			// verify that vnode associated with this key is not synching
			waiting, askToNodeId := n.clusterManager.dataSyncManager.IsWaitingForData(hash)

			if waiting && askToNodeId != "" && askToNodeId != fromNodeId {
				n.skipInsertFromStreamForEntry(collectionName, key)
			}
		} else {
			ev := statEvent{
				EventType:      DeleteEvent,
				CollectionName: collectionName,
			}
			n.nodeStats.Log(ev)

			err = c.DeleteRaw(key)

			if err == nil {
				n.fireEvent(collectionName, Delete, key)
			}
		}
	}

	return err
}

func (n *Node) DeleteRaw(fromNodeId string, collectionName string, key []byte) (err error) {

	hash := partitioner.GetHash(key)

	//find WHERE to store this data
	isLocal := true
	skipReplicas := false

	loc := n.clusterManager.partitionTable.GetLocation(hash)

	if loc.Master != n.GetId() || loc.Master == fromNodeId {
		isLocal = false

		if fromNodeId == loc.Master || slices.Contains(loc.Replicas, fromNodeId) {
			//sender knows that this node is a replica, we can write but we cannot replicate (to avoid an infine loop)
			isLocal = true
			skipReplicas = true
		}
	}

	if isLocal {

		err = n.performDelete(fromNodeId, hash, collectionName, key)

		//upsert replicas
		if !skipReplicas && err == nil {
			for _, r := range loc.Replicas {
				if r != "" && r != n.GetId() {
					err = n.clusterManager.commManager.SendDel(r, collectionName, key)
					if err != nil {
						//write to log for r recovery
						storage.AppendToWal(r, storage.DEL, collectionName, key, []byte{}, time.Now().UTC().UnixMicro())
					}
				}
			}
		}
	} else {
		err = n.clusterManager.commManager.SendDel(loc.Master, collectionName, key)
		if err != nil {
			//master is down
			if slices.Contains(loc.Replicas, n.GetId()) {
				//master is down, this node is temp master for this write

				//WRITE LOCALLY
				err = n.performDelete(n.GetId(), hash, collectionName, key)

				//WRITE TO LOG FOR MASTER RECOVERY
				storage.AppendToWal(loc.Master, storage.DEL, collectionName, key, []byte{}, time.Now().UTC().UnixMicro())

				if err == nil {
					//SEND TO OTHER REPLICAS
					for _, r := range loc.Replicas {
						if r != "" && r != n.GetId() {
							err = n.clusterManager.commManager.SendDel(r, collectionName, key)
							if err != nil {
								//write to log for r recovery
								storage.AppendToWal(r, storage.DEL, collectionName, key, []byte{}, time.Now().UTC().UnixMicro())
							}
						}
					}
				}
			} else {
				//contact the first available replica
			SEND_TO_FIRST_REPLICA:
				for _, r := range loc.Replicas {
					if r != "" && r != n.GetId() {
						err = n.clusterManager.commManager.SendDel(r, collectionName, key)
						if err == nil {
							break SEND_TO_FIRST_REPLICA
						}
					}
				}
			}
		}
	}

	return err
}

func (n *Node) GetIterator(collectionName string) (*storage.Iterator, error) {
	c, e := n.storageManager.GetCollection(collectionName)

	if e != nil {
		return nil, e
	}

	return c.GetIterator()
}

func (n *Node) GetIteratorFrom(collectionName string, from interface{}) (*storage.Iterator, error) {
	c, e := n.storageManager.GetCollection(collectionName)

	if e != nil {
		return nil, e
	}

	return c.GetIteratorFrom(from)
}

func (n *Node) GetFilteredIterator(collectionName string, from interface{}, to interface{}) (*storage.Iterator, error) {
	c, e := n.storageManager.GetCollection(collectionName)

	if e != nil {
		return nil, e
	}

	return c.GetFilteredIterator(from, to)
}

func (n *Node) ScanRaw(collectionName string, startFromKey []byte, numberOfResults int) ([]types.RWRequest, error) {
	c, e := n.storageManager.GetCollection(collectionName)

	if e != nil {
		return nil, e
	}
	it, err := c.GetIteratorFromRaw(startFromKey)
	if err != nil {
		return nil, err
	}

	if numberOfResults == 0 {
		numberOfResults = 100
	}

	data := []types.RWRequest{}
LOOP:
	for it.HasMore() {
		_, key, value, _ := it.NextRaw()

		vBytes := storage.DbValue{}

		e = storage.DecodeValue(value, &vBytes)
		if e == nil {
			value = vBytes.Value
			//need to make a copy of key because key is reused and slices are inherently reference-y things
			b := make([]byte, len(key))
			copy(b, key)
			data = append(data, types.RWRequest{Collection: collectionName, Key: b, Value: value})
			if len(data) >= numberOfResults {
				break LOOP
			}
		}
	}
	return data, nil

}

// this will delete the collection locally, it wont delete it from the other nodes in the cluster
func (n *Node) DeleteCollection(collectionName string) error {
	c, e := n.storageManager.GetCollection(collectionName)

	if e != nil {
		return e
	}

	return c.DeleteCollection()
}

func (n *Node) GetStats() *NodeStats {
	return n.nodeStats
}

// nodeId was down, see if we have data to send its way
func (n *Node) bringNodeUpToDate(nodeId string) {
	const DATA_SIZE_PER_FRAME = 500

	it, err := storage.StartScan(nodeId)
	if err != nil {
		n.logger.Error(err, "Cannot bring "+nodeId+" up to date")
		return
	}

	log := []storage.Entry{}
	for it.HasMore() {
		entry, _ := it.PullNext()

		log = append(log, entry)
		//fmt.Println(entry.LogPosition, entry.Action, entry.Collection, string(entry.Key), string(entry.Value), entry.Timestamp)
		if len(log) >= DATA_SIZE_PER_FRAME {
			n.clusterManager.commManager.SendActionsLog(nodeId, log)
			log = []storage.Entry{}
		}
	}

	if len(log) >= 0 {
		n.clusterManager.commManager.SendActionsLog(nodeId, log)
	}

	it.AllStreamed()

}

func (n *Node) processActionsLog(log []storage.Entry) {
	for _, entry := range log {
		if entry.Action == storage.SET {
			hash := partitioner.GetHash(entry.Key)
			//collectionName string,  uint64, key []byte, value []byte
			n.performUpsert(entry.Collection, hash, entry.Key, entry.Value, entry.Timestamp)
		} else if entry.Action == storage.DEL {
			c, err := n.storageManager.GetCollection(entry.Collection)

			if err == nil {
				ev := statEvent{
					EventType:      DeleteEvent,
					CollectionName: entry.Collection,
				}
				n.nodeStats.Log(ev)

				c.DeleteRaw(entry.Key)
			}
		}
	}
}

func (n *Node) SubscribeTo(collectionName string) chan Event {
	evChannel := make(chan Event)
	tmp := n.eventsSubscribers[collectionName]
	tmp = append(tmp, evChannel)
	n.eventsSubscribers[collectionName] = tmp
	return evChannel
}

func (n *Node) fireEvent(collectionName string, eventType EventType, key []byte) {
	ev := Event{
		Type:       eventType,
		Collection: collectionName,
		Key:        key,
	}

	tmp := n.eventsSubscribers[collectionName]
	for _, ch := range tmp {
		ch <- ev
	}
}
