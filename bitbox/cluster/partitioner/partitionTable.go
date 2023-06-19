package partitioner

import (
	"fmt"

	"github.com/lrondanini/bit-box/bitbox/cluster/partitioner/murmur3"
	"github.com/lrondanini/bit-box/bitbox/storage"

	"github.com/google/uuid"
)

const PT_KEY_NAME = "partitionTable"

func GetHashForString(s string) uint64 {
	return murmur3.GetHash64ForString(s)
}

func GetHash(s []byte) uint64 {
	return murmur3.GetHash64(s)
}

func GenerateUUID() string {
	return uuid.New().String()
}

type HashLocation struct {
	Master   string
	Replicas []string
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
	err := systemDb.Get(PT_KEY_NAME, &res)

	if err != nil {
		if err == storage.ErrKeyNotFound {
			return nil, err
		}
		return nil, err
	}
	return &res, nil
}

func (pt *PartitionTable) SaveToDb(systemDb *storage.Collection) error {
	return systemDb.Set(PT_KEY_NAME, pt)
}

func (pt *PartitionTable) PrintToStdOut() {
	fmt.Println("Timestamp: ", pt.Timestamp)
	PrintVnodes(pt.VNodes)
}

func (pt *PartitionTable) GetLocation(hash uint64) HashLocation {
	res := HashLocation{}
	for _, v := range pt.VNodes {
		if v.StartToken <= hash && v.EndToken >= hash {
			res.Master = v.NodeId
			res.Replicas = v.ReplicatedTo
		}
	}
	return res
}
