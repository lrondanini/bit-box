package partitioner

import (
	"encoding/json"
	"errors"

	"github.com/lrondanini/bit-box/bitbox/partitioner/murmur3"
	"github.com/lrondanini/bit-box/bitbox/storage"

	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb"
)

const PT_COLLECTION_NAME = "partitionTable"

func GetHash(s string) uint64 {
	return murmur3.GetHash64(s)
}

func GenerateUUID() string {
	return uuid.New().String()
}

type PartitionTable struct {
	VNodes    []VNode `json:"vNodes"`
	Timestamp int64   `json:"timstamp"`
}

func InitEmptyPartitionTable() *PartitionTable {
	return &PartitionTable{}
}

func InitPartitionTable(nodes []VNode, timstamp int64) *PartitionTable {
	return &PartitionTable{
		VNodes:    nodes,
		Timestamp: timstamp,
	}
}

func LoadFromDb(systemDb *storage.Collection) (*PartitionTable, error) {
	str, err := systemDb.Get(PT_COLLECTION_NAME)

	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, err
		}
		return nil, errors.New("[partition-table-3]" + err.Error())
	}

	res := PartitionTable{}
	if str != "" {
		if err := json.Unmarshal([]byte(str), &res); err != nil {
			return nil, errors.New("[partition-table-4]" + err.Error())
		}
	}
	return &res, nil
}

func (pt *PartitionTable) SaveToDb(systemDb *storage.Collection) error {

	str, err := json.Marshal(pt)

	if err != nil {
		return errors.New("[partition-table-1]" + err.Error())
	}

	err = systemDb.Set(PT_COLLECTION_NAME, string(str))

	if err != nil {
		return errors.New("[partition-table-2]" + err.Error())
	}

	return nil
}
