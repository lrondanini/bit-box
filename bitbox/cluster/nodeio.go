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

	hash := partitioner.GetHash(key)

	//find WHERE to store this data
	isLocal := true
	skipReplicas := false

	loc := n.clusterManager.partitionTable.GetLocation(hash)
	fmt.Println(string(key), loc)
	if loc.Master != n.GetId() {
		isLocal = false
		if fromNodeId == loc.Master || slices.Contains(loc.Replicas, fromNodeId) {
			//sender knows that this node is a replica, we can write but we cannot replicate (to avoid an infine loop)
			isLocal = true
			skipReplicas = true
		}
	}

	if isLocal {
		err = n.performUpsert(collectionName, hash, key, value)

		//upsert replicas
		if !skipReplicas && err == nil {
			for _, r := range loc.Replicas {
				if r != "" && r != n.GetId() && r != fromNodeId {
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
				err = n.performUpsert(collectionName, hash, key, value)

				//WRITE TO LOG FOR MASTER RECOVERY
				storage.AppendToWal(loc.Master, storage.SET, collectionName, key, value, time.Now().UTC().UnixMicro())

				if err == nil {
					//SEND TO OTHER REPLICA
					for _, r := range loc.Replicas {
						if r != "" && r != n.GetId() && r != fromNodeId {
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
					if r != "" && r != n.GetId() && r != fromNodeId {
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

func (n *Node) performUpsert(collectionName string, hash uint64, key []byte, value []byte) error {
	c, err := n.storageManager.GetCollection(collectionName)

	if err == nil {
		exists, _ := c.Exists(key)

		v := storage.DbValue{
			Hash:      hash,
			Timestamp: time.Now().UTC().UnixMicro(),
			Value:     value,
		}

		vBytes, err := storage.EncodeValue(v)

		if err == nil {
			c.SetRaw(key, vBytes)

			ev := Event{
				EventType:      UpsertEvent,
				CollectionName: collectionName,
				IsNew:          !exists,
			}
			n.nodeStats.Log(ev)
		}
	}

	return err
}

func (n *Node) upsertFromClusterStream(collectionName string, data []stream.StreamEntry) error {
	c, e := n.storageManager.GetCollection(collectionName)
	if e != nil {
		return e
	}

	c.UpsertFromStreaming(data, func() {
		ev := Event{
			EventType:      UpsertEvent,
			CollectionName: collectionName,
			IsNew:          true,
		}
		n.nodeStats.Log(ev)
	})

	return nil
}

func (n *Node) deleteForClusterSync(collectionName string, keys [][]byte) error {
	c, e := n.storageManager.GetCollection(collectionName)
	if e != nil {
		return e
	}

	return c.DeleteKeys(keys, func() {
		ev := Event{
			EventType:      DeleteEvent,
			CollectionName: collectionName,
		}
		n.nodeStats.Log(ev)
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
	res, err := n.GetRaw(collectionName, bKey)
	if err != nil {
		return err
	}

	value = storage.FromBytes(res, value)

	return nil
}

func (n *Node) GetRaw(collectionName string, key []byte) ([]byte, error) {

	hash := partitioner.GetHash(key)

	//find WHERE to get this data
	isLocal := true

	loc := n.clusterManager.partitionTable.GetLocation(hash)
	if loc.Master != n.GetId() && !slices.Contains(loc.Replicas, n.GetId()) {
		isLocal = false
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
				waiting, askToNodeId := n.clusterManager.dataSyncManager.WaitingForData(hash)

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

		ev := Event{
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
			waiting, askToNodeId := n.clusterManager.dataSyncManager.WaitingForData(hash)

			if waiting && askToNodeId != "" && askToNodeId != fromNodeId {
				err = n.clusterManager.commManager.SendDel(askToNodeId, collectionName, key)
			}
		} else {
			ev := Event{
				EventType:      DeleteEvent,
				CollectionName: collectionName,
			}
			n.nodeStats.Log(ev)

			err = c.DeleteRaw(key)
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
		if loc.Master == fromNodeId {
			//fromNodeId can ask this node to delete a key if still streaming/synching
			isLocal = true
		} else {
			isLocal = false
		}

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
				if r != "" && r != n.GetId() && r != fromNodeId {
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
					//SEND TO OTHER REPLICA
					for _, r := range loc.Replicas {
						if r != "" && r != n.GetId() && r != fromNodeId {
							err = n.clusterManager.commManager.SendDel(r, collectionName, key)
							if err != nil {
								//write to log for r recovery
								storage.AppendToWal(r, storage.DEL, collectionName, key, []byte{}, time.Now().UTC().UnixMicro())
							}
						}
					}
				}
			} else {
				//contact the replicas
			SEND_TO_FIRST_REPLICA:
				for _, r := range loc.Replicas {
					if r != "" && r != n.GetId() && r != fromNodeId {
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
