package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/lrondanini/bit-box/bitbox/cluster/utils"
	"github.com/lrondanini/bit-box/bitbox/storage/filesystem"
	"github.com/tidwall/wal"
)

type Wal struct {
	wal    *wal.Log
	dir    string
	status status
}

type status struct {
	NodeId            string
	FistSavedIndex    uint64
	LastSavedIndex    uint64
	LastStreamedIndex uint64
}

type Action uint8

const (
	SET Action = iota
	DEL
)

func (a Action) String() string {
	switch a {
	case SET:
		return "SET"
	case DEL:
		return "DEL"
	default:
		panic(fmt.Sprintf("unknown action: %d", a))
	}
}

type Entry struct {
	LogPosition uint64
	Collection  string
	Timestamp   int64
	Action      Action
	Key         []byte
	Value       []byte
}

const STATUS_FILE = "status"
const WAL_DIR = "wal"

func InitWal(nodeId string) (*Wal, error) {
	conf := utils.GetClusterConfiguration()
	var path = conf.DATA_FOLDER
	dir := filepath.Join(path, WAL_DIR, nodeId)

	wal, err := wal.Open(dir, nil)
	if err != nil {
		return nil, err
	}

	status := status{
		NodeId:            nodeId,
		FistSavedIndex:    1,
		LastSavedIndex:    0,
		LastStreamedIndex: 0,
	}

	filesystem.ReadBinary(filepath.Join(dir, STATUS_FILE), &status)

	return &Wal{
		wal:    wal,
		dir:    dir,
		status: status,
	}, nil
}

func StartScan(nodeId string) (*WalIterator, error) {
	w, err := InitWal(nodeId)
	if err == nil {
		return w.GetIterator(), nil
	}
	return nil, err
}

var logSync sync.Mutex

func AppendToWal(forNodeId string, a Action, collection string, key []byte, value []byte, timestamp int64) error {
	logSync.Lock()
	wal, err := InitWal(forNodeId)
	if err == nil {
		defer wal.Close()
		err = wal.Log(a, collection, key, value, timestamp)
	}
	logSync.Unlock()
	return err
}

func (w *Wal) Close() error {
	return w.wal.Close()
}

func (w *Wal) saveStatus() error {
	return filesystem.WriteBinary(filepath.Join(w.dir, STATUS_FILE), w.status)
}

func (w *Wal) Log(a Action, collection string, key []byte, value []byte, timestamp int64) error {
	entry := Entry{
		Collection: collection,
		Action:     a,
		Key:        key,
		Value:      value,
		Timestamp:  timestamp,
	}

	w.status.LastSavedIndex = w.status.LastSavedIndex + 1
	entry.LogPosition = w.status.LastSavedIndex
	e, err := filesystem.SimpleBinaryEncode(entry)
	if err != nil {
		return err
	}

	err = w.saveStatus()
	if err != nil {
		return err
	} else {
		return w.wal.Write(w.status.LastSavedIndex, e)
	}
}

func (w *Wal) Read(i uint64) (Entry, error) {
	entry := Entry{}

	data, err := w.wal.Read(i)
	if err == nil {
		err = filesystem.SimpleBinaryDecode(data, &entry)
	}

	return entry, err
}

func (w *Wal) GetIterator() *WalIterator {
	return initIterator(w)
}

func (w *Wal) TruncateFront(i uint64) error {
	w.status.FistSavedIndex = i
	err := w.saveStatus()
	if err != nil {
		return err
	}
	return w.wal.TruncateFront(i)
}

func (w *Wal) Destroy() error {
	w.Close()
	return os.RemoveAll(w.dir)
}

/********** ITERATOR **********/

type WalIterator struct {
	wal     *Wal
	current uint64
}

func initIterator(w *Wal) *WalIterator {
	return &WalIterator{
		wal:     w,
		current: w.status.FistSavedIndex,
	}
}

func (it *WalIterator) HasMore() bool {
	return it.current <= it.wal.status.LastSavedIndex
}

func (it *WalIterator) Next() (Entry, error) {
	entry, err := it.wal.Read(it.current)
	it.current = it.current + 1
	return entry, err
}

func (it *WalIterator) PullNext() (Entry, error) {
	entry, err := it.wal.Read(it.current)
	it.current = it.current + 1
	if err == nil {
		it.wal.TruncateFront(it.current)
	}
	return entry, err
}
