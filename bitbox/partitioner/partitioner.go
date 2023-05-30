package partitioner

import (
	"github.com/lrondanini/bit-box/bitbox/partitioner/murmur3"
	"github.com/lrondanini/bit-box/bitbox/storage"

	"github.com/google/uuid"
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
	res := PartitionTable{}
	err := systemDb.Get(PT_COLLECTION_NAME, &res)

	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, err
		}
		return nil, err
	}
	return &res, nil
}

func (pt *PartitionTable) SaveToDb(systemDb *storage.Collection) error {
	return systemDb.Set(PT_COLLECTION_NAME, pt)
}
